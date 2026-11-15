package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/workspace/gitworktree"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	sessionmanager "github.com/aoagents/agent-orchestrator/backend/internal/session_manager"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

// Keep service, manager, lifecycle, SQLite and Git real; no agent process is run.
func TestOrchestratorReplacementCanonicalWorkspaceAndRestore(t *testing.T) {
	for _, failReplacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("launch_failure=%v", failReplacement), func(t *testing.T) {
			git, err := exec.LookPath("git")
			if err != nil {
				t.Skip("git not found")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
			run(repo, "commit", "--allow-empty", "-m", "base")
			base := run(repo, "rev-parse", "HEAD")
			store, err := sqlitetest.Open(filepath.Join(root, "db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.UpsertProject(ctx, domain.ProjectRecord{
				ID: "mer", Path: repo, RegisteredAt: time.Now(),
				Config: domain.ProjectConfig{DefaultBranch: "main", Orchestrator: domain.RoleOverride{Harness: domain.HarnessClaudeCode}},
			}); err != nil {
				t.Fatal(err)
			}
			ws, err := gitworktree.New(gitworktree.Options{
				Binary: git, ManagedRoot: filepath.Join(root, "worktrees"), RepoResolver: gitworktree.StaticRepoResolver{"mer": repo},
			})
			if err != nil {
				t.Fatal(err)
			}
			dataDir := filepath.Join(root, "data")
			rt := &replacementRuntime{alive: map[string]bool{}}
			msg := replacementMessenger{}
			lcm := lifecycle.New(store, msg)
			manager := sessionmanager.New(sessionmanager.Deps{
				Runtime: rt, Agents: replacementAgents{}, Workspace: ws, Store: store, Messenger: msg, Lifecycle: lcm,
				DataDir: dataDir, LookPath: func(string) (string, error) { return git, nil },
			})
			svc := NewWithDeps(Deps{Manager: manager, Store: store, DataDir: dataDir})
			old, err := svc.SpawnOrchestrator(ctx, "mer", false, domain.SessionModeTUI)
			if err != nil {
				t.Fatal(err)
			}
			canonical := sessionmanager.DefaultOrchestratorBranch("mer", dataDir)
			if old.Metadata.Branch != canonical {
				t.Fatalf("initial branch = %q, want %q", old.Metadata.Branch, canonical)
			}
			// Give the predecessor both committed history and untracked work.
			write := func(path, value string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(old.Metadata.WorkspacePath, "committed.txt"), "coordinator history\n")
			run(old.Metadata.WorkspacePath, "add", "committed.txt")
			run(old.Metadata.WorkspacePath, "commit", "-m", "coordinator history")
			head := run(old.Metadata.WorkspacePath, "rev-parse", "HEAD")
			write(filepath.Join(old.Metadata.WorkspacePath, "pending.txt"), "pending coordination\n")
			boom := errors.New("injected replacement launch failure")
			if failReplacement {
				rt.createErr = boom
			}
			next, err := svc.SpawnOrchestrator(ctx, "mer", true, domain.SessionModeTUI)
			if failReplacement {
				var apiError *apierr.Error
				if !errors.As(err, &apiError) || apiError.Code != "RUNTIME_CREATE_FAILED" || !strings.Contains(apiError.Message, boom.Error()) {
					t.Fatalf("replacement error = %v, want injected RUNTIME_CREATE_FAILED", err)
				}
				if records := run(repo, "worktree", "list", "--porcelain"); strings.Count(records, "worktree ") != 1 {
					t.Fatalf("failed replacement left an unowned worktree: %s", records)
				}
				if _, err := os.Stat(rt.lastPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed replacement workspace still exists: %v", err)
				}
				rows, rowsErr := store.ListSessions(ctx, "mer")
				if rowsErr != nil || len(rows) != 1 || !rows[0].IsTerminated || len(rt.alive) != 0 {
					t.Fatalf("failed replacement left a seed or live controller: %+v, %v", rows, rowsErr)
				}
				rt.createErr = nil
				// Retry through the same replacement service, after the failed seed
				// row has been rolled back and the predecessor has retired.
				next, err = svc.SpawnOrchestrator(ctx, "mer", true, domain.SessionModeTUI)
			}
			if err != nil {
				t.Fatal(err)
			}
			prior, ok, err := store.GetSession(ctx, old.ID)
			if err != nil || !ok || !prior.IsTerminated || rt.alive[old.Metadata.RuntimeHandleID] {
				t.Fatalf("predecessor not retired: %+v, %v", prior, err)
			}
			if next.ID == old.ID || next.IsTerminated || next.Metadata.Branch != canonical || next.Metadata.WorkspacePath != old.Metadata.WorkspacePath {
				t.Fatalf("replacement identity/workspace mismatch: %+v", next)
			}
			if got := run(next.Metadata.WorkspacePath, "symbolic-ref", "--short", "HEAD"); got != canonical {
				t.Fatalf("replacement Git branch = %q, want %q", got, canonical)
			}
			if got := run(repo, "rev-parse", "refs/heads/"+canonical); got != head {
				t.Fatalf("canonical history changed: %s, want %s", got, head)
			}
			preserved := "refs/ao/preserved/" + string(old.ID)
			if got := run(repo, "show", preserved+":pending.txt"); got != "pending coordination" {
				t.Fatalf("predecessor uncommitted work lost: %q", got)
			}
			if got := run(repo, "show", preserved+":committed.txt"); got != "coordinator history" {
				t.Fatalf("predecessor committed work lost: %q", got)
			}
			if got := run(repo, "rev-parse", "main"); got != base {
				t.Fatalf("main changed: %s", got)
			}
			if records := run(repo, "worktree", "list", "--porcelain"); strings.Count(records, "worktree ") != 2 {
				t.Fatalf("unexpected worktree ownership after replacement: %s", records)
			}
			rows, err := store.ListSessions(ctx, "mer")
			if err != nil || len(rows) != 2 {
				t.Fatalf("session rows after replacement = %d, %v; want predecessor and successor only", len(rows), err)
			}
			// Normal kill/Restore must reattach this exact canonical history.
			if freed, err := svc.Kill(ctx, next.ID); err != nil || !freed {
				t.Fatalf("kill replacement: freed=%v, err=%v", freed, err)
			}
			restored, err := svc.Restore(ctx, next.ID)
			if err != nil {
				t.Fatal(err)
			}
			if restored.Session.ID != next.ID || restored.Session.IsTerminated || restored.Session.Metadata.Branch != canonical || restored.Session.Metadata.WorkspacePath != next.Metadata.WorkspacePath {
				t.Fatalf("restore changed canonical session: %+v", restored)
			}
			if got := run(restored.Session.Metadata.WorkspacePath, "rev-parse", "HEAD"); got != head {
				t.Fatalf("restored HEAD = %s, want %s", got, head)
			}
		})
	}
}

type replacementRuntime struct {
	alive     map[string]bool
	created   int
	createErr error
	lastPath  string
}

func (r *replacementRuntime) Create(_ context.Context, cfg ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	r.lastPath = cfg.WorkspacePath
	if r.createErr != nil {
		return ports.RuntimeHandle{}, r.createErr
	}
	r.created++
	id := fmt.Sprintf("replacement-runtime-%d", r.created)
	r.alive[id] = true
	return ports.RuntimeHandle{ID: id}, nil
}
func (r *replacementRuntime) Destroy(_ context.Context, h ports.RuntimeHandle) error {
	delete(r.alive, h.ID)
	return nil
}
func (r *replacementRuntime) IsAlive(_ context.Context, h ports.RuntimeHandle) (bool, error) {
	return r.alive[h.ID], nil
}
func (r *replacementRuntime) ProbeFencedRuntime(_ context.Context, ref ports.FencedRuntimeRef) ports.FencedProbeResult {
	if r.alive[ref.Handle.ID] {
		return ports.FencedProbeResult{Liveness: ports.FencedAlive, Reason: ports.FencedReasonExactMatch}
	}
	return ports.FencedProbeResult{Liveness: ports.FencedDead, Reason: ports.FencedReasonExactAbsent}
}
func (*replacementRuntime) GetOutput(context.Context, ports.RuntimeHandle, int) (string, error) {
	return "", nil
}

type replacementAgents struct{}

func (replacementAgents) Agent(domain.AgentHarness) (ports.Agent, bool) {
	return replacementAgent{}, true
}

type replacementAgent struct{}

func (replacementAgent) GetConfigSpec(context.Context) (ports.ConfigSpec, error) {
	return ports.ConfigSpec{}, nil
}
func (replacementAgent) GetLaunchCommand(context.Context, ports.LaunchConfig) ([]string, error) {
	return []string{"test-agent"}, nil
}
func (replacementAgent) GetPromptDeliveryStrategy(context.Context, ports.LaunchConfig) (ports.PromptDeliveryStrategy, error) {
	return ports.PromptDeliveryInCommand, nil
}
func (replacementAgent) GetAgentHooks(context.Context, ports.WorkspaceHookConfig) error { return nil }
func (replacementAgent) GetRestoreCommand(context.Context, ports.RestoreConfig) ([]string, bool, error) {
	return nil, false, nil
}
func (replacementAgent) SessionInfo(context.Context, ports.SessionRef) (ports.SessionInfo, bool, error) {
	return ports.SessionInfo{}, false, nil
}

type replacementMessenger struct{}

func (replacementMessenger) Send(context.Context, domain.SessionID, string) error { return nil }
