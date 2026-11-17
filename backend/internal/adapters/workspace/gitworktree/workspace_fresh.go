package gitworktree

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// createFreshWorktree is only for generated spawn branches. Explicit branch
// requests and Restore may attach existing history; a new allocation must not.
func (w *Workspace) createFreshWorktree(ctx context.Context, repo, path string, cfg ports.WorkspaceConfig) (ports.WorkspaceInfo, error) {
	records, err := w.listRecords(ctx, repo)
	if err != nil {
		return ports.WorkspaceInfo{}, err
	}
	if _, exists := findWorktree(records, path); exists {
		return ports.WorkspaceInfo{}, fmt.Errorf("%w: path %q is already registered; use Restore for its existing session", ports.ErrWorkspaceFreshConflict, path)
	}
	if _, err := os.Lstat(path); err == nil {
		return ports.WorkspaceInfo{}, fmt.Errorf("%w: path %q already exists", ports.ErrWorkspaceFreshConflict, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ports.WorkspaceInfo{}, fmt.Errorf("gitworktree: inspect fresh path %q: %w", path, err)
	}

	branch, err := w.freshBranch(ctx, repo, cfg.Branch)
	if err != nil {
		return ports.WorkspaceInfo{}, err
	}
	baseRef := cfg.BaseRef
	seed := cfg.ExecutionBaseSHA
	if seed == "" {
		if baseRef == "" {
			// An empty requested branch resolves only the repository default,
			// never a historical remote session branch with the same name.
			refs, err := w.resolveWorktreeRefsWithBudget(ctx, repo, "", cfg.BaseBranch)
			if err != nil {
				return ports.WorkspaceInfo{}, err
			}
			baseRef = refs.baseRef
		}
		seed, err = w.revParse(ctx, repo, baseRef+"^{commit}")
		if err != nil {
			return ports.WorkspaceInfo{}, err
		}
	}
	// Git owns the atomic create-only ref check. Never retry by attaching an
	// existing branch, or force a path registration, if another allocation wins.
	if _, err := w.run(ctx, w.binary, worktreeAddNewBranchArgs(repo, branch, path, seed, false)...); err != nil {
		return ports.WorkspaceInfo{}, fmt.Errorf("%w: create branch %q at %s: %w", ports.ErrWorkspaceFreshConflict, branch, seed, err)
	}
	info := ports.WorkspaceInfo{Path: path, Branch: branch, BaseRef: baseRef, ExecutionBaseSHA: cfg.ExecutionBaseSHA, SourceRef: cfg.SourceRef, SessionID: cfg.SessionID, ProjectID: cfg.ProjectID}
	if err := w.AttestExecutionBase(ctx, info); err != nil {
		return info, err
	}
	return info, nil
}

func (w *Workspace) freshBranch(ctx context.Context, repo, requested string) (string, error) {
	out, err := w.run(ctx, w.binary, "-C", repo, "for-each-ref", "--format=%(refname)", "refs/heads/", "refs/remotes/")
	if err != nil {
		return "", fmt.Errorf("gitworktree: list allocated branches: %w", err)
	}
	var branches []string
	for _, ref := range strings.Fields(string(out)) {
		if branch, ok := strings.CutPrefix(ref, "refs/heads/"); ok {
			branches = append(branches, branch)
		} else if remoteBranch, ok := strings.CutPrefix(ref, "refs/remotes/"); ok {
			if _, branch, ok := strings.Cut(remoteBranch, "/"); ok {
				branches = append(branches, branch)
			}
		}
	}
	// Keep /root and allocate a fresh namespace so sibling PR branches from
	// an older session cannot be mistaken for children of the new session.
	namespace := strings.TrimSuffix(requested, "/root")
	suffix := strings.TrimPrefix(requested, namespace)
	for i := 0; i < 100; i++ {
		candidate := namespace
		if i > 0 {
			candidate = fmt.Sprintf("%s-%d", namespace, i+1)
		}
		occupied := false
		for _, branch := range branches {
			if branch == candidate || strings.HasPrefix(branch, candidate+"/") || strings.HasPrefix(candidate, branch+"/") {
				occupied = true
				break
			}
		}
		if !occupied {
			return candidate + suffix, nil
		}
	}
	return "", fmt.Errorf("%w: no free namespace for %q within 100 candidates", ports.ErrWorkspaceFreshConflict, requested)
}
