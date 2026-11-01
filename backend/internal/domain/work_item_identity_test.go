package domain

import (
	"errors"
	"strings"
	"testing"
)

func workIdentityFixture() WorkItemIdentity {
	return WorkItemIdentity{
		ID: "work-1", AttemptID: "attempt-1", ProjectID: "project-1",
		Repository: RootWorkspaceRepoName, ExecutionBaseSHA: strings.Repeat("a", 40),
		OriginalParentSessionID: "parent-1", OriginalWorkerSessionID: "worker-1",
		OwnerSessionID: "worker-1", OwnerGeneration: 2,
		WorktreePath: "C:/ao/worktrees/worker-1", Branch: "ao/worker-1/root",
	}
}

func TestWorkItemIdentityValidation(t *testing.T) {
	valid := workIdentityFixture()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	sha256Base := valid
	sha256Base.ExecutionBaseSHA = strings.Repeat("a", 64)
	if err := sha256Base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*WorkItemIdentity)
	}{
		{"empty ID", func(i *WorkItemIdentity) { i.ID = "" }},
		{"empty attempt", func(i *WorkItemIdentity) { i.AttemptID = "" }},
		{"empty project", func(i *WorkItemIdentity) { i.ProjectID = "" }},
		{"empty repository", func(i *WorkItemIdentity) { i.Repository = "" }},
		{"empty base", func(i *WorkItemIdentity) { i.ExecutionBaseSHA = "" }},
		{"short base", func(i *WorkItemIdentity) { i.ExecutionBaseSHA = "abcdef" }},
		{"nonhex base", func(i *WorkItemIdentity) { i.ExecutionBaseSHA = strings.Repeat("z", 40) }},
		{"empty parent", func(i *WorkItemIdentity) { i.OriginalParentSessionID = "" }},
		{"empty worker", func(i *WorkItemIdentity) { i.OriginalWorkerSessionID = "" }},
		{"unknown owner", func(i *WorkItemIdentity) { i.OwnerSessionID = "" }},
		{"unknown generation", func(i *WorkItemIdentity) { i.OwnerGeneration = 0 }},
		{"negative generation", func(i *WorkItemIdentity) { i.OwnerGeneration = -1 }},
		{"empty worktree", func(i *WorkItemIdentity) { i.WorktreePath = "" }},
		{"empty branch", func(i *WorkItemIdentity) { i.Branch = "" }},
		{"whitespace", func(i *WorkItemIdentity) { i.ID = " work-1" }},
		{"NUL", func(i *WorkItemIdentity) { i.WorktreePath += "\x00" }},
		{"newline", func(i *WorkItemIdentity) { i.Repository += "\nforeign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := valid
			tc.mutate(&got)
			if err := got.Validate(); !errors.Is(err, ErrInvalidWorkIdentity) {
				t.Fatalf("Validate() = %v, want invalid identity", err)
			}
		})
	}
}

func TestWorkItemIdentityExactBinding(t *testing.T) {
	expected := workIdentityFixture()
	if err := expected.ValidateAgainst(expected); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*WorkItemIdentity)
		want   error
	}{
		{"foreign work", func(i *WorkItemIdentity) { i.ID = "work-2" }, ErrWorkIdentityMismatch},
		{"foreign attempt", func(i *WorkItemIdentity) { i.AttemptID = "attempt-2" }, ErrWorkIdentityMismatch},
		{"foreign project", func(i *WorkItemIdentity) { i.ProjectID = "project-2" }, ErrWorkIdentityMismatch},
		{"foreign repository", func(i *WorkItemIdentity) { i.Repository = "child" }, ErrWorkIdentityMismatch},
		{"changed base", func(i *WorkItemIdentity) { i.ExecutionBaseSHA = strings.Repeat("b", 40) }, ErrWorkIdentityMismatch},
		{"foreign parent", func(i *WorkItemIdentity) { i.OriginalParentSessionID = "parent-2" }, ErrWorkIdentityMismatch},
		{"changed original worker", func(i *WorkItemIdentity) { i.OriginalWorkerSessionID = "worker-2" }, ErrWorkIdentityMismatch},
		{"foreign owner", func(i *WorkItemIdentity) { i.OwnerSessionID = "worker-2" }, ErrWorkIdentityMismatch},
		{"stale owner", func(i *WorkItemIdentity) { i.OwnerGeneration-- }, ErrStaleWorkOwner},
		{"unaccepted generation", func(i *WorkItemIdentity) { i.OwnerGeneration++ }, ErrWorkIdentityMismatch},
		{"foreign worktree", func(i *WorkItemIdentity) { i.WorktreePath += "-other" }, ErrWorkIdentityMismatch},
		{"path spelling", func(i *WorkItemIdentity) { i.WorktreePath = strings.ToLower(i.WorktreePath) }, ErrWorkIdentityMismatch},
		{"foreign branch", func(i *WorkItemIdentity) { i.Branch += "-other" }, ErrWorkIdentityMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := expected
			tc.mutate(&got)
			if err := got.ValidateAgainst(expected); !errors.Is(err, tc.want) {
				t.Fatalf("ValidateAgainst() = %v, want %v", err, tc.want)
			}
		})
	}
	if err := expected.ValidateAgainst(WorkItemIdentity{}); !errors.Is(err, ErrInvalidWorkIdentity) {
		t.Fatalf("unknown expected identity accepted: %v", err)
	}
	if err := (WorkItemIdentity{}).ValidateAgainst(expected); !errors.Is(err, ErrInvalidWorkIdentity) {
		t.Fatalf("unknown submitted identity accepted: %v", err)
	}
}

func TestWorkItemCandidatePreservesExecutionBase(t *testing.T) {
	expected := workIdentityFixture()
	original := expected
	for _, head := range []string{expected.ExecutionBaseSHA, strings.Repeat("b", 40), strings.Repeat("c", 64)} {
		candidate := WorkItemCandidate{Identity: expected, HeadSHA: head}
		if err := candidate.ValidateAgainst(expected); err != nil {
			t.Fatal(err)
		}
	}
	if expected != original {
		t.Fatal("candidate validation changed original identity")
	}
	for _, head := range []string{"", "main", strings.Repeat("z", 40)} {
		if err := (WorkItemCandidate{Identity: expected, HeadSHA: head}).ValidateAgainst(expected); !errors.Is(err, ErrInvalidWorkIdentity) {
			t.Fatalf("invalid candidate HEAD accepted: %v", err)
		}
	}
	changed := expected
	changed.ExecutionBaseSHA = strings.Repeat("b", 40)
	if err := (WorkItemCandidate{Identity: changed, HeadSHA: changed.ExecutionBaseSHA}).ValidateAgainst(expected); !errors.Is(err, ErrWorkIdentityMismatch) {
		t.Fatalf("candidate HEAD substituted for original base: %v", err)
	}
}

func TestWorkItemIdentitySnapshotDuplicates(t *testing.T) {
	first := workIdentityFixture()
	second := first
	second.ID, second.AttemptID = "work-2", "attempt-2"
	second.WorktreePath += "-2"
	if err := ValidateWorkItemIdentities([]WorkItemIdentity{first, second}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*WorkItemIdentity)
	}{
		{"logical ID", func(i *WorkItemIdentity) { i.ID = first.ID }},
		{"attempt ID", func(i *WorkItemIdentity) { i.AttemptID = first.AttemptID }},
		{"worktree", func(i *WorkItemIdentity) { i.WorktreePath = first.WorktreePath }},
		{"worktree across repositories", func(i *WorkItemIdentity) {
			i.WorktreePath = first.WorktreePath
			i.Repository = "child"
		}},
		{"worktree across projects", func(i *WorkItemIdentity) {
			i.WorktreePath = first.WorktreePath
			i.ProjectID = "project-2"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			duplicate := second
			tc.mutate(&duplicate)
			if err := ValidateWorkItemIdentities([]WorkItemIdentity{first, duplicate}); !errors.Is(err, ErrDuplicateWorkIdentity) {
				t.Fatalf("duplicate accepted: %v", err)
			}
		})
	}
	if err := ValidateWorkItemIdentities([]WorkItemIdentity{first, {}}); !errors.Is(err, ErrInvalidWorkIdentity) {
		t.Fatalf("invalid snapshot member accepted: %v", err)
	}
}
