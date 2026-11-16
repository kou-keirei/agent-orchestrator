package sessionmanager

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/workspace/gitworktree"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// Inject drift at deterministic boundaries while using real Git worktrees and
// the production SQLite/lifecycle/Chat service. No provider process is launched.
type sourceCleanupWorkspace struct {
	*gitworktree.Workspace
	afterCreate bool
	check, at   int
	move        func()
	created     ports.WorkspaceInfo
}

type deadlineCleanupWorkspace struct {
	*sourceCleanupWorkspace
	destroyCalls int
	destroyErr   error
}

func (w *deadlineCleanupWorkspace) Destroy(ctx context.Context, _ ports.WorkspaceInfo) error {
	w.destroyCalls++
	<-ctx.Done()
	w.destroyErr = ctx.Err()
	return w.destroyErr
}

func TestExactSourceFailedAdmissionCleanupDeadlinePreservesDurableOwner(t *testing.T) {
	f := newWorkSpawnFixture(t)
	_, repo, _, old, base := newSwitchExecutionBaseFixture(t, "")
	git, err := exec.LookPath("git")
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
	adapter, err := gitworktree.New(gitworktree.Options{Binary: git, ManagedRoot: filepath.Join(t.TempDir(), "managed"), RepoResolver: gitworktree.StaticRepoResolver{f.auth.project: repo}})
	if err != nil {
		t.Fatal(err)
	}
	w := &deadlineCleanupWorkspace{sourceCleanupWorkspace: &sourceCleanupWorkspace{Workspace: adapter, at: 1}}
	const unfinished = "unfinished worker content\n"
	w.move = func() {
		if err := os.WriteFile(filepath.Join(w.created.Path, "unfinished.txt"), []byte(unfinished), 0o600); err != nil {
			t.Fatal(err)
		}
		runSwitchGit(t, git, repo, "update-ref", "refs/heads/main", old, base)
	}
	f.manager.workspace = w
	cfg := f.config(domain.SessionModeChat)
	cfg.Branch = "ao/exact-cleanup-deadline"
	cfg.ExecutionBaseSHA, cfg.SourceRef = base, "main"
	if _, _, _, err := f.manager.Spawn(t.Context(), cfg); !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
		t.Fatalf("source drift error = %v", err)
	}
	if w.destroyCalls != 1 || !errors.Is(w.destroyErr, context.DeadlineExceeded) {
		t.Fatalf("teardown did not exhaust its deadline exactly once: calls=%d err=%v", w.destroyCalls, w.destroyErr)
	}
	if f.births.Load() != 1 || f.runtime.created != 0 {
		t.Fatalf("rejected birth reached provider: chat=%d runtime=%d", f.births.Load()-1, f.runtime.created)
	}
	if content, err := os.ReadFile(filepath.Join(w.created.Path, "unfinished.txt")); err != nil || string(content) != unfinished {
		t.Fatalf("unfinished work changed: content=%q err=%v", content, err)
	}
	if registered := runSwitchGit(t, git, repo, "worktree", "list", "--porcelain"); strings.Count(registered, "worktree ") != 3 || !strings.Contains(registered+"\n", "branch refs/heads/"+cfg.Branch+"\n") {
		t.Fatalf("retained workspace registration or branch missing: %s", registered)
	}
	// Reopen SQLite and read with a fresh bounded context after teardown's
	// deadline, independently of the spawn and its rollback context.
	reopened, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	readCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	owner, found, err := reopened.GetSession(readCtx, w.created.SessionID)
	if err != nil || !found {
		t.Fatalf("retained owner missing after reopen: found=%v err=%v", found, err)
	}
	if !owner.IsTerminated || owner.Activity.State != domain.ActivityExited || owner.Metadata.WorkspacePath != w.created.Path || owner.Metadata.Branch != cfg.Branch || owner.Metadata.WorkspaceRepoPath != w.created.RepoPath || owner.Metadata.ExecutionBaseSHA != base || owner.Metadata.SourceRef != "refs/heads/main" || owner.Metadata.RuntimeHandleID != "" || owner.Metadata.ProviderConversationID != "" {
		t.Fatalf("cleanup deadline lost durable ownership: %+v", owner)
	}
}

func (w *sourceCleanupWorkspace) Create(ctx context.Context, cfg ports.WorkspaceConfig) (ports.WorkspaceInfo, error) {
	info, err := w.Workspace.Create(ctx, cfg)
	w.created = info
	if err == nil && w.afterCreate {
		w.move()
		err = w.Workspace.AttestExecutionBase(ctx, info)
	}
	return info, err
}

func (w *sourceCleanupWorkspace) AttestExecutionBase(ctx context.Context, info ports.WorkspaceInfo) error {
	w.check++
	if w.check == w.at {
		w.move()
	}
	return w.Workspace.AttestExecutionBase(ctx, info)
}

func TestExactSourceFailedAdmissionCleanupAndDurableOwner(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		for _, stage := range []string{"adapter", "manager-first", "manager-second", "before-provider", "cancelled"} {
			for _, retain := range []bool{false, true} {
				t.Run(string(mode)+"/"+stage+"/retain="+map[bool]string{false: "false", true: "true"}[retain], func(t *testing.T) {
					f := newWorkSpawnFixture(t)
					spawnCtx, cancel := context.WithCancel(t.Context())
					defer cancel()
					_, repo, _, old, base := newSwitchExecutionBaseFixture(t, "")
					git, err := exec.LookPath("git")
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
					adapter, err := gitworktree.New(gitworktree.Options{Binary: git, ManagedRoot: filepath.Join(t.TempDir(), "managed"), RepoResolver: gitworktree.StaticRepoResolver{f.auth.project: repo}})
					if err != nil {
						t.Fatal(err)
					}
					w := &sourceCleanupWorkspace{Workspace: adapter}
					w.move = func() {
						runSwitchGit(t, git, repo, "update-ref", "refs/heads/main", old, base)
						if retain {
							runSwitchGit(t, git, repo, "worktree", "lock", w.created.Path)
						}
						if stage == "cancelled" {
							cancel()
						}
					}
					switch stage {
					case "adapter":
						w.afterCreate = true
					case "manager-first", "cancelled":
						w.at = 1
					case "manager-second":
						w.at = 2
					case "before-provider":
						w.at = 4
					}
					f.manager.workspace = w
					cfg := f.config(mode)
					cfg.Branch = "ao/exact-cleanup"
					cfg.ExecutionBaseSHA, cfg.SourceRef = base, "main"
					if _, _, _, err := f.manager.Spawn(spawnCtx, cfg); !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
						t.Fatalf("source drift error = %v", err)
					}
					if f.births.Load() != 1 || f.runtime.created != 0 {
						t.Fatalf("rejected birth reached provider: chat=%d runtime=%d", f.births.Load()-1, f.runtime.created)
					}
					rows, err := f.store.ListSessions(t.Context(), f.auth.project)
					if err != nil {
						t.Fatal(err)
					}
					registered := runSwitchGit(t, git, repo, "worktree", "list", "--porcelain")
					if !retain {
						if len(rows) != 1 || strings.Count(registered, "worktree ") != 2 {
							t.Fatalf("orphaned row or worktree: rows=%+v registrations=%s", rows, registered)
						}
						if _, err := os.Stat(w.created.Path); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("rejected workspace still on disk: %v", err)
						}
						return
					}
					if len(rows) != 2 || strings.Count(registered, "worktree ") != 3 {
						t.Fatalf("cleanup failure lost ownership: rows=%+v registrations=%s", rows, registered)
					}
					owner := rows[1]
					if !owner.IsTerminated || owner.Metadata.WorkspacePath != w.created.Path || owner.Metadata.Branch != cfg.Branch || owner.Metadata.ExecutionBaseSHA != base || owner.Metadata.SourceRef != "refs/heads/main" || owner.Metadata.RuntimeHandleID != "" || owner.Metadata.ProviderConversationID != "" {
						t.Fatalf("invalid retained owner: %+v", owner)
					}
					// Reopen independently to verify the retained owner is durable,
					// then exercise the existing cleanup path against that identity.
					reopened, err := sqlite.Open(f.dir)
					if err != nil {
						t.Fatal(err)
					}
					defer reopened.Close()
					read, found, err := reopened.GetSession(t.Context(), owner.ID)
					if err != nil || !found || !read.IsTerminated || !reflect.DeepEqual(read.Metadata, owner.Metadata) {
						t.Fatalf("retained owner did not survive reopen: %+v %v", read, err)
					}
					runSwitchGit(t, git, repo, "worktree", "unlock", w.created.Path)
					if _, err := f.manager.Cleanup(t.Context(), f.auth.project); err != nil {
						t.Fatal(err)
					}
					if got := runSwitchGit(t, git, repo, "worktree", "list", "--porcelain"); strings.Count(got, "worktree ") != 2 {
						t.Fatalf("retained owner could not reclaim workspace: %s", got)
					}
				})
			}
		}
	}
}
