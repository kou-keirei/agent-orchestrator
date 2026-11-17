package gitworktree

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const reusedBranch = "ao/proj-139/root"

type freshFixture struct {
	ws                     *Workspace
	repo, git, old, target string
}

func freshGit(t *testing.T, git, repo string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, git, append([]string{"-C", repo}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func newFreshFixture(t *testing.T) freshFixture {
	t.Helper()
	git := requireGit(t)
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string { return freshGit(t, git, repo, args...) }
	run("init", "-b", "main")
	run("config", "user.email", "ao@example.com")
	run("config", "user.name", "AO test")
	run("config", "commit.gpgsign", "false")
	run("config", "ao.defaultBranch", "main")
	run("commit", "--allow-empty", "-m", "seed")
	run("switch", "-c", reusedBranch)
	if err := os.WriteFile(filepath.Join(repo, "historical.txt"), []byte("old session\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "historical.txt")
	run("commit", "-m", "historical session")
	old := run("rev-parse", "HEAD")
	run("switch", "main")
	if err := os.WriteFile(filepath.Join(repo, "current.txt"), []byte("current main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "current.txt")
	run("commit", "-m", "current authorized main")
	target := run("rev-parse", "HEAD")
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(root, "new-profile"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatal(err)
	}
	ws.run = func(ctx context.Context, binary string, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return runCommand(ctx, binary, args...)
	}
	return freshFixture{ws: ws, repo: repo, git: git, old: old, target: target}
}

func freshConfig() ports.WorkspaceConfig {
	return ports.WorkspaceConfig{ProjectID: "proj", SessionID: "proj-139", Kind: domain.KindWorker, Branch: reusedBranch, FreshBranch: true}
}

func TestFreshCreateDisambiguatesHistoricalNamespace(t *testing.T) {
	for _, mode := range []string{"no collision", "local ref", "dirty old worktree", "remote ref", "sibling ref", "second collision", "exact authority"} {
		t.Run(mode, func(t *testing.T) {
			f := newFreshFixture(t)
			run := func(args ...string) string { return freshGit(t, f.git, f.repo, args...) }
			cfg := freshConfig()
			wantBranch := "ao/proj-139-2/root"
			wantSHA := f.target
			oldPath := filepath.Join(filepath.Dir(f.ws.managedRoot), "old-profile", "proj-139")
			switch mode {
			case "no collision":
				run("branch", "-m", reusedBranch, "historical/review")
				wantBranch = reusedBranch
			case "dirty old worktree":
				run("worktree", "add", oldPath, reusedBranch)
				if err := os.WriteFile(filepath.Join(oldPath, "pending-review.txt"), []byte("user work\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			case "remote ref":
				run("update-ref", "refs/remotes/origin/"+reusedBranch, f.old)
				run("branch", "-D", reusedBranch)
			case "sibling ref":
				run("branch", "-m", reusedBranch, "ao/proj-139/pending-review")
			case "second collision":
				run("branch", "ao/proj-139-2/root", f.old)
				wantBranch = "ao/proj-139-3/root"
			case "exact authority":
				cfg.ExecutionBaseSHA = run("rev-parse", "main^")
				wantSHA = cfg.ExecutionBaseSHA
			}
			before := run("for-each-ref", "--format=%(refname) %(objectname)")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			info, err := f.ws.Create(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if info.Branch != wantBranch || info.SessionID != cfg.SessionID || info.Path != filepath.Join(f.ws.managedRoot, "proj", "proj-139") {
				t.Fatalf("fresh metadata = %#v, want branch %s with original session ID", info, wantBranch)
			}
			if got := freshGit(t, f.git, info.Path, "rev-parse", "HEAD"); got != wantSHA {
				t.Fatalf("fresh HEAD = %s, want %s", got, wantSHA)
			}
			if got := freshGit(t, f.git, info.Path, "rev-parse", "HEAD^{tree}"); got != run("rev-parse", wantSHA+"^{tree}") {
				t.Fatalf("fresh tree differs from authorized commit: %s", got)
			}
			if got := freshGit(t, f.git, info.Path, "symbolic-ref", "--short", "HEAD"); got != wantBranch {
				t.Fatalf("fresh branch = %s, want %s", got, wantBranch)
			}
			if got := freshGit(t, f.git, info.Path, "status", "--porcelain"); got != "" {
				t.Fatalf("fresh status = %q", got)
			}
			after := run("for-each-ref", "--format=%(refname) %(objectname)")
			for _, ref := range strings.Split(before, "\n") {
				if !strings.Contains(after+"\n", ref+"\n") {
					t.Fatalf("historical ref changed: %s", ref)
				}
			}
			if mode == "dirty old worktree" {
				if got := freshGit(t, f.git, oldPath, "rev-parse", "HEAD"); got != f.old {
					t.Fatalf("old HEAD changed: %s", got)
				}
				if got := freshGit(t, f.git, oldPath, "status", "--porcelain"); got != "?? pending-review.txt" {
					t.Fatalf("old work changed: %s", got)
				}
			}
		})
	}
}

func TestFreshCreateRequiresDefaultAndPreservesExplicitOccupiedBranch(t *testing.T) {
	for _, mode := range []string{"unresolved default", "explicit occupied branch"} {
		t.Run(mode, func(t *testing.T) {
			f := newFreshFixture(t)
			run := func(args ...string) string { return freshGit(t, f.git, f.repo, args...) }
			cfg := freshConfig()
			wantErr := ports.ErrWorkspaceDefaultBranchUnresolved
			if mode == "unresolved default" {
				run("config", "--unset", "ao.defaultBranch")
			} else {
				cfg.FreshBranch = false
				run("worktree", "add", filepath.Join(filepath.Dir(f.repo), "old-profile"), reusedBranch)
				wantErr = ports.ErrWorkspaceBranchCheckedOutElsewhere
			}
			refs := run("for-each-ref", "--format=%(refname) %(objectname)")
			records := run("worktree", "list", "--porcelain")
			if _, err := f.ws.Create(context.Background(), cfg); !errors.Is(err, wantErr) {
				t.Fatalf("Create error = %v, want %v", err, wantErr)
			}
			if refs != run("for-each-ref", "--format=%(refname) %(objectname)") || records != run("worktree", "list", "--porcelain") {
				t.Fatal("failed Create changed refs or registrations")
			}
		})
	}
}

func TestFreshCreateLosingBranchRaceDoesNotAttachWinner(t *testing.T) {
	f := newFreshFixture(t)
	runner := f.ws.run
	attempts := 0
	f.ws.run = func(ctx context.Context, binary string, args ...string) ([]byte, error) {
		if len(args) > 5 && args[2] == "worktree" && args[3] == "add" {
			attempts++
			if args[4] != "-b" {
				t.Fatalf("fresh allocation retried with attach or force: %v", args)
			}
			freshGit(t, f.git, f.repo, "update-ref", "refs/heads/"+args[5], f.old)
		}
		return runner(ctx, binary, args...)
	}
	_, err := f.ws.Create(context.Background(), freshConfig())
	if !errors.Is(err, ports.ErrWorkspaceFreshConflict) || attempts != 1 {
		t.Fatalf("losing allocation: err=%v, attempts=%d", err, attempts)
	}
	if got := freshGit(t, f.git, f.repo, "rev-parse", "refs/heads/ao/proj-139-2/root"); got != f.old {
		t.Fatalf("winner ref changed: %s", got)
	}
	path, err := f.ws.managedPath(freshConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lost allocation materialized a path: %v", err)
	}
}

func TestFreshBranchExhaustionIsTypedAndReadOnly(t *testing.T) {
	f := newFreshFixture(t)
	// A historical parent ref blocks every generated child namespace. Do not
	// delete it or silently fall back to attaching a historical branch.
	f.ws.run = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) < 3 || args[2] != "for-each-ref" {
			t.Fatalf("unexpected mutation: %v", args)
		}
		return []byte("refs/heads/ao\n"), nil
	}
	if _, err := f.ws.freshBranch(context.Background(), f.repo, reusedBranch); !errors.Is(err, ports.ErrWorkspaceFreshConflict) {
		t.Fatalf("exhaustion = %v, want typed conflict", err)
	}
}

func TestFreshCreateRejectsOccupiedPathWithoutMutation(t *testing.T) {
	for _, mode := range []string{"dirty registered", "clean registered", "missing registered", "locked registered", "unregistered directory"} {
		t.Run(mode, func(t *testing.T) {
			f := newFreshFixture(t)
			cfg := freshConfig()
			path, err := f.ws.managedPath(cfg)
			if err != nil {
				t.Fatal(err)
			}
			run := func(args ...string) string { return freshGit(t, f.git, f.repo, args...) }
			if mode == "unregistered directory" {
				if err := os.MkdirAll(path, 0o755); err != nil {
					t.Fatal(err)
				}
			} else {
				run("worktree", "add", path, reusedBranch)
				if mode == "missing registered" {
					if err := os.RemoveAll(path); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "locked registered" {
					run("worktree", "lock", path)
				}
			}
			if mode == "dirty registered" || mode == "unregistered directory" {
				if err := os.WriteFile(filepath.Join(path, "user.txt"), []byte("preserve me"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			refs := run("for-each-ref", "--format=%(refname) %(objectname)")
			records := run("worktree", "list", "--porcelain")
			_, err = f.ws.Create(context.Background(), cfg)
			if !errors.Is(err, ports.ErrWorkspaceFreshConflict) {
				t.Fatalf("Create error = %v, want typed fresh conflict", err)
			}
			if got := run("for-each-ref", "--format=%(refname) %(objectname)"); got != refs {
				t.Fatal("ref set changed on conflict")
			}
			if got := run("worktree", "list", "--porcelain"); got != records {
				t.Fatal("registrations changed on conflict")
			}
			if mode == "dirty registered" || mode == "unregistered directory" {
				if got, err := os.ReadFile(filepath.Join(path, "user.txt")); err != nil || string(got) != "preserve me" {
					t.Fatalf("user file changed: %q, %v", got, err)
				}
			}
		})
	}
}

func TestHistoricalRestoreAndExplicitBranchKeepHistory(t *testing.T) {
	for _, mode := range []string{"explicit Create", "registered Restore", "unregistered Restore"} {
		t.Run(mode, func(t *testing.T) {
			f := newFreshFixture(t)
			cfg := freshConfig()
			cfg.FreshBranch = false
			var info ports.WorkspaceInfo
			var err error
			if mode == "explicit Create" {
				info, err = f.ws.Create(context.Background(), cfg)
			} else {
				path, pathErr := f.ws.managedPath(cfg)
				if pathErr != nil {
					t.Fatal(pathErr)
				}
				if mode == "registered Restore" {
					freshGit(t, f.git, f.repo, "worktree", "add", path, reusedBranch)
					if err := os.WriteFile(filepath.Join(path, "pending.txt"), []byte("review"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				info, err = f.ws.Restore(context.Background(), cfg)
			}
			if err != nil {
				t.Fatal(err)
			}
			if info.Branch != reusedBranch || freshGit(t, f.git, info.Path, "rev-parse", "HEAD") != f.old {
				t.Fatalf("historical branch changed: %#v", info)
			}
			if mode == "registered Restore" {
				if got := freshGit(t, f.git, info.Path, "status", "--porcelain"); got != "?? pending.txt" {
					t.Fatalf("Restore changed user work: %s", got)
				}
			}
		})
	}
}
