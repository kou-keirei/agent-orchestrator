package sessionmanager

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/workspace/gitworktree"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type birthDriftWorkspace struct {
	ports.Workspace
	attester    ports.WorkspaceExecutionBaseAttester
	beforeCheck func()
}

func (w birthDriftWorkspace) AttestExecutionBase(ctx context.Context, info ports.WorkspaceInfo) error {
	w.beforeCheck()
	return w.attester.AttestExecutionBase(ctx, info)
}

func TestNativeExactBaseSpawnBeforeProvider(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		attester, repo, _, a, b := newSwitchExecutionBaseFixture(t, "")
		git, err := exec.LookPath("git")
		if err != nil {
			t.Fatal(err)
		}
		runSwitchGit(t, git, repo, "branch", "wrong", a)
		for _, failure := range []string{"missing commit", "malformed commit", "missing ref", "stale ref", "branch mismatch", "ref drift", "launch ref drift"} {
			t.Run(string(mode)+"/"+failure, func(t *testing.T) {
				launcher := &recordingLauncher{}
				m, st, rt := newChatManager(launcher)
				m.dataDir = t.TempDir()
				st.projects["proj"] = domain.ProjectRecord{ID: "proj", Path: repo, Config: testRoleAgents()}
				m.workspace = birthDriftWorkspace{Workspace: attester.(ports.Workspace), attester: attester, beforeCheck: func() {}}
				cfg := ports.SpawnConfig{ProjectID: "proj", Kind: domain.KindWorker, Harness: domain.HarnessCodex, RequestedMode: mode, ExecutionBaseSHA: b, SourceRef: "main"}
				switch failure {
				case "missing commit":
					cfg.ExecutionBaseSHA = strings.Repeat("0", 40)
				case "malformed commit":
					cfg.ExecutionBaseSHA = "main"
				case "missing ref":
					cfg.SourceRef = "refs/heads/absent"
				case "stale ref":
					cfg.ExecutionBaseSHA = a
				case "branch mismatch":
					cfg.Branch = "wrong"
				case "ref drift":
					m.workspace = birthDriftWorkspace{Workspace: m.workspace, attester: attester, beforeCheck: func() { runSwitchGit(t, git, repo, "update-ref", "refs/heads/main", a) }}
				case "launch ref drift":
					// Restore the source after the preceding rejection, then move
					// it at the last admission before the fake provider can start.
					runSwitchGit(t, git, repo, "update-ref", "refs/heads/main", b)
					checks, at := 0, 4
					if mode == domain.SessionModeChat {
						at = 5 // PrepareControllerEnv, inside Chat's launch gate.
					}
					m.workspace = birthDriftWorkspace{Workspace: m.workspace, attester: attester, beforeCheck: func() {
						checks++
						if checks == at {
							runSwitchGit(t, git, repo, "update-ref", "refs/heads/main", a, b)
						}
					}}
				}
				if _, _, _, err := m.Spawn(t.Context(), cfg); !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
					t.Fatalf("got %v", err)
				}
				if len(launcher.started) != 0 || len(launcher.turns) != 0 || rt.created != 0 {
					t.Fatalf("source mismatch launched provider: chat=%d turns=%d runtime=%d", len(launcher.started), len(launcher.turns), rt.created)
				}
			})
		}
	}
}

func TestPromptlessExactBaseNativeReadback(t *testing.T) {
	attester, repo, _, a, b := newSwitchExecutionBaseFixture(t, "")
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	launcher := &recordingLauncher{}
	m, st, rt := newChatManager(launcher)
	m.dataDir = t.TempDir()
	m.workspace = attester.(ports.Workspace)
	st.projects["proj"] = domain.ProjectRecord{ID: "proj", Path: repo, Config: testRoleAgents()}
	launcher.beforeStart = func(in ChatStart) {
		seed, found, err := st.GetSession(t.Context(), in.SessionID)
		if err != nil || !found || seed.Metadata.ExecutionBaseSHA != b {
			t.Fatalf("base not persisted before provider: %+v %v", seed, err)
		}
		if runSwitchGit(t, git, in.WorkspacePath, "rev-parse", "HEAD") != b || runSwitchGit(t, git, in.WorkspacePath, "rev-parse", "HEAD^{tree}") != runSwitchGit(t, git, repo, "rev-parse", b+"^{tree}") {
			t.Fatal("provider saw wrong source")
		}
	}
	rec, _, _, err := m.Spawn(t.Context(), ports.SpawnConfig{ProjectID: "proj", Kind: domain.KindWorker, Harness: domain.HarnessCodex, RequestedMode: domain.SessionModeChat, ExecutionBaseSHA: b, SourceRef: "main"})
	if err != nil {
		t.Fatal(err)
	}
	read, found, err := st.GetSession(t.Context(), rec.ID)
	if err != nil || !found || read.Metadata.ExecutionBaseSHA != b || read.Metadata.SourceRef != "refs/heads/main" || read.Metadata.Branch == "" || read.Metadata.WorkspacePath == "" || len(launcher.turns) != 0 || rt.created != 0 {
		t.Fatalf("promptless binding: %+v err=%v", read, err)
	}
	runSwitchGit(t, git, repo, "update-ref", "refs/heads/main", a, b)
	if workspaceInfo(read).SourceRef != "" {
		t.Fatal("existing session follows moving ref")
	}
	if err := m.admitExecutionBase(t.Context(), workspaceInfo(read)); err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.started[0].PrepareControllerEnv(t.Context(), read.ControllerOwner()); err != nil {
		t.Fatalf("retained Chat callback followed the historical ref: %v", err)
	}
	current, _, err := st.GetSession(t.Context(), rec.ID)
	if err != nil || !reflect.DeepEqual(current.Metadata, read.Metadata) {
		t.Fatalf("existing session changed on drift: %v", err)
	}
}

func TestPromptlessExactBasePersistsThroughNativeLifecycle(t *testing.T) {
	f := newWorkSpawnFixture(t)
	_, repo, _, _, base := newSwitchExecutionBaseFixture(t, "")
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := gitworktree.New(gitworktree.Options{Binary: git,
		ManagedRoot:  filepath.Join(t.TempDir(), "managed"),
		RepoResolver: gitworktree.StaticRepoResolver{f.auth.project: repo}})
	if err != nil {
		t.Fatal(err)
	}
	project, _, err := f.store.GetProject(t.Context(), string(f.auth.project))
	if err != nil {
		t.Fatal(err)
	}
	project.Path = repo
	if err := f.store.UpsertProject(t.Context(), project); err != nil {
		t.Fatal(err)
	}
	f.manager.workspace = workspace
	cfg := f.config(domain.SessionModeChat)
	cfg.ExecutionBaseSHA = base
	cfg.SourceRef = "refs/heads/main"
	rec, _, _, err := f.manager.Spawn(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := f.store.GetSession(t.Context(), rec.ID)
	if err != nil || !found || got.Metadata.ExecutionBaseSHA != base || got.Metadata.SourceRef != cfg.SourceRef || got.Metadata.WorkspacePath == "" || got.Metadata.Branch == "" {
		t.Fatalf("persisted birth source: %+v found=%v err=%v", got, found, err)
	}
	if runSwitchGit(t, git, got.Metadata.WorkspacePath, "rev-parse", "HEAD") != base || runSwitchGit(t, git, got.Metadata.WorkspacePath, "rev-parse", "HEAD^{tree}") != runSwitchGit(t, git, repo, "rev-parse", base+"^{tree}") {
		t.Fatal("native lifecycle materialized the wrong source")
	}
	if f.births.Load() != 2 || f.runtime.created != 0 {
		t.Fatalf("expected parent and promptless Chat worker: chat=%d runtime=%d", f.births.Load(), f.runtime.created)
	}
	f.service.StopAll(t.Context())
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	read, found, err := reopened.GetSession(t.Context(), rec.ID)
	if err != nil || !found || !reflect.DeepEqual(read.Metadata, got.Metadata) {
		t.Fatalf("reopened birth source: %+v found=%v err=%v", read, found, err)
	}
}
