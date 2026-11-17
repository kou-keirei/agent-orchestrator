package sessionmanager

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/workspace/gitworktree"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestSpawnReusedProfileIDPersistsFreshBranchAtProviderBoundary(t *testing.T) {
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(path string, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, git, append([]string{"-C", path}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run(repo, "init", "-b", "main")
	run(repo, "config", "user.name", "AO test")
	run(repo, "config", "user.email", "ao@example.com")
	run(repo, "config", "commit.gpgsign", "false")
	run(repo, "commit", "--allow-empty", "-m", "historical base")
	old := run(repo, "rev-parse", "HEAD")
	run(repo, "branch", "ao/mer-1/root")
	run(repo, "commit", "--allow-empty", "-m", "current accepted base")
	target := run(repo, "rev-parse", "HEAD")
	ws, err := gitworktree.New(gitworktree.Options{
		Binary: git, ManagedRoot: filepath.Join(root, "new-profile-worktrees"), RepoResolver: gitworktree.StaticRepoResolver{"mer": repo},
	})
	if err != nil {
		t.Fatal(err)
	}
	launcher := &recordingLauncher{}
	m, st, rt := newChatManager(launcher)
	m.dataDir = filepath.Join(root, "new-profile-data")
	m.workspace = ws
	project := st.projects["mer"]
	project.Path = repo
	project.Config.DefaultBranch = "main"
	st.projects["mer"] = project
	launcher.beforeStart = func(start ChatStart) {
		if got := run(start.WorkspacePath, "rev-parse", "HEAD"); got != target {
			t.Fatalf("provider birth HEAD = %s, want %s", got, target)
		}
		if got := run(start.WorkspacePath, "symbolic-ref", "--short", "HEAD"); got != "ao/mer-1-2/root" {
			t.Fatalf("provider birth branch = %s", got)
		}
	}
	rec, _, _, err := m.Spawn(ctx, ports.SpawnConfig{
		ProjectID: "mer", Kind: domain.KindWorker, Harness: domain.HarnessCodex, RequestedMode: domain.SessionModeChat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.ID != "mer-1" || st.sessions[rec.ID].Metadata.Branch != "ao/mer-1-2/root" || len(launcher.started) != 1 || rt.created != 0 {
		t.Fatalf("fresh session metadata/controller mismatch: %+v, starts=%d runtime=%d", rec, len(launcher.started), rt.created)
	}
	if got := run(repo, "rev-parse", "refs/heads/ao/mer-1/root"); got != old {
		t.Fatalf("historical session ref changed: %s", got)
	}
}

func TestSpawnGeneratedBranchRequestsFreshAllocation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   domain.SessionKind
		branch string
		fresh  bool
	}{
		{"generated worker", domain.KindWorker, "", true},
		{"explicit worker", domain.KindWorker, "custom/work", false},
		{"generated orchestrator", domain.KindOrchestrator, "", false},
		{"explicit orchestrator", domain.KindOrchestrator, "custom/coordinator", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, _, ws := newManager()
			_, _, _, err := m.Spawn(ctx, ports.SpawnConfig{ProjectID: "mer", Kind: tc.kind, Branch: tc.branch})
			if err != nil {
				t.Fatal(err)
			}
			if ws.lastCfg.FreshBranch != tc.fresh {
				t.Fatalf("FreshBranch = %v, want %v", ws.lastCfg.FreshBranch, tc.fresh)
			}
			wantBranch := tc.branch
			if wantBranch == "" {
				wantBranch = DefaultSpawnBranch("mer-1", tc.kind, "mer", domain.ProjectKindSingleRepo, m.dataDir)
			}
			if ws.lastCfg.Branch != wantBranch {
				t.Fatalf("branch = %q, want %q", ws.lastCfg.Branch, wantBranch)
			}
		})
	}
}

func TestFreshAllocationConflictStopsBeforeProviderBirth(t *testing.T) {
	launcher := &recordingLauncher{}
	m, st, rt := newChatManager(launcher)
	m.workspace.(*fakeWorkspace).createErr = ports.ErrWorkspaceFreshConflict
	_, _, _, err := m.Spawn(context.Background(), ports.SpawnConfig{
		ProjectID: chatTestProject, Kind: domain.KindWorker, Harness: domain.HarnessCodex, RequestedMode: domain.SessionModeChat,
	})
	if !errors.Is(err, ports.ErrWorkspaceFreshConflict) {
		t.Fatalf("Spawn error = %v, want fresh allocation conflict", err)
	}
	if len(launcher.started) != 0 || rt.created != 0 {
		t.Fatalf("provider/runtime born on conflict: starts=%d runtime=%d", len(launcher.started), rt.created)
	}
	if _, exists := st.sessions["mer-1"]; exists {
		t.Fatal("failed fresh allocation left seed row")
	}
}
