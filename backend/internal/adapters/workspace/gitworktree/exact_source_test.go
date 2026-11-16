package gitworktree

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestExactSourceBirth(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	target := gitOutput(t, git, repo, "rev-parse", "HEAD")
	runGit(t, git, repo, "commit", "--allow-empty", "-m", "next")
	next := gitOutput(t, git, repo, "rev-parse", "HEAD")
	runGit(t, git, repo, "branch", "source", target)
	runGit(t, git, repo, "branch", "wrong-destination", next)
	w, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, sha, ref, branch string }{
		{"missing commit", strings.Repeat("0", 40), "", "ao/missing"},
		{"abbreviated commit", target[:12], "", "ao/short"},
		{"symbolic commit", "main", "", "ao/symbolic"},
		{"uppercase commit", strings.ToUpper(target), "", "ao/upper"},
		{"missing ref", target, "refs/heads/absent", "ao/absent"},
		{"stale ref", target, "main", "ao/stale"},
		{"revision expression", target, "source^{commit}", "ao/expression"},
		{"source without sha", "", "source", "ao/unbound"},
		{"branch mismatch", target, "source", "wrong-destination"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := w.Create(t.Context(), ports.WorkspaceConfig{ProjectID: "proj", SessionID: domain.SessionID(strings.ReplaceAll(tc.branch, "/", "-")), Branch: tc.branch, ExecutionBaseSHA: tc.sha, SourceRef: tc.ref})
			if !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
				t.Fatalf("got %v", err)
			}
		})
	}
	create := func(id string) (ports.WorkspaceInfo, error) {
		return w.Create(t.Context(), ports.WorkspaceConfig{ProjectID: "proj", SessionID: domain.SessionID(id), Branch: "ao/" + id, ExecutionBaseSHA: target, SourceRef: "source"})
	}
	info, err := create("exact")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Destroy(context.Background(), info) })
	if info.ExecutionBaseSHA != target || info.SourceRef != "refs/heads/source" || gitOutput(t, git, info.Path, "rev-parse", "HEAD") != target || gitOutput(t, git, info.Path, "rev-parse", "HEAD^{tree}") != gitOutput(t, git, repo, "rev-parse", target+"^{tree}") {
		t.Fatalf("wrong birth: %+v", info)
	}
	runGit(t, git, repo, "update-ref", "refs/heads/source", next, target)
	if err := w.AttestExecutionBase(t.Context(), info); !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
		t.Fatalf("moving ref admitted: %v", err)
	}
	if _, err := create("after-drift"); !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
		t.Fatalf("new stale birth admitted: %v", err)
	}
	// A completed birth retains its execution authority; restoring never follows
	// or revalidates the historical source ref.
	info.SourceRef = ""
	if err := w.AttestExecutionBase(t.Context(), info); err != nil {
		t.Fatalf("existing authority changed: %v", err)
	}
	if gitOutput(t, git, info.Path, "rev-parse", "HEAD") != target {
		t.Fatal("ref drift changed worker")
	}
}

func TestExactSourceMovesWhileCreating(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "generated"}[fresh], func(t *testing.T) {
			git := requireGit(t)
			tmp := t.TempDir()
			repo := setupOriginClone(t, git, tmp)
			target := gitOutput(t, git, repo, "rev-parse", "HEAD")
			runGit(t, git, repo, "commit", "--allow-empty", "-m", "next")
			next := gitOutput(t, git, repo, "rev-parse", "HEAD")
			runGit(t, git, repo, "branch", "source", target)
			w, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
			if err != nil {
				t.Fatal(err)
			}
			baseRun := w.run
			w.run = func(ctx context.Context, binary string, args ...string) ([]byte, error) {
				out, err := baseRun(ctx, binary, args...)
				if err == nil && len(args) > 3 && args[2] == "worktree" && args[3] == "add" {
					runGit(t, git, repo, "update-ref", "refs/heads/source", next, target)
				}
				return out, err
			}
			info, err := w.Create(t.Context(), ports.WorkspaceConfig{ProjectID: "proj", SessionID: "drift", Branch: "ao/drift", FreshBranch: fresh, ExecutionBaseSHA: target, SourceRef: "source"})
			if !errors.Is(err, ports.ErrWorkspaceExecutionBaseMismatch) {
				t.Fatalf("drift during birth admitted: %v", err)
			}
			if info.Path == "" || info.Branch != "ao/drift" || info.SourceRef != "refs/heads/source" || info.ExecutionBaseSHA != target {
				t.Fatalf("post-create rejection lost cleanup identity: %+v", info)
			}
			if err := w.Destroy(t.Context(), info); err != nil {
				t.Fatal(err)
			}
			if records, err := w.listRecords(t.Context(), repo); err != nil || len(records) != 1 {
				t.Fatalf("orphaned registration after rejected birth: %+v %v", records, err)
			}
		})
	}
}
