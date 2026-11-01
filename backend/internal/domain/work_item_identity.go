package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// WorkItemID identifies logical work independently of a session or attempt.
type WorkItemID string

// WorkAttemptID identifies one execution attempt of logical work.
type WorkAttemptID string

var (
	ErrInvalidWorkIdentity   = errors.New("invalid work identity")
	ErrWorkIdentityMismatch  = errors.New("work identity mismatch")
	ErrStaleWorkOwner        = errors.New("stale work owner generation")
	ErrDuplicateWorkIdentity = errors.New("duplicate work identity")
)

// WorkItemIdentity binds one attempt to its original authorization and owner.
// These are values to validate against a caller's authoritative record, not a
// durable store or evidence that the named sessions, repository or worktree exist.
// Replacing an owner or starting another attempt requires admission outside this
// kernel. Existing SessionID and ProjectID types retain their meaning.
type WorkItemIdentity struct {
	ID        WorkItemID
	AttemptID WorkAttemptID
	ProjectID ProjectID
	// Repository is the exact AO repository name within ProjectID, including
	// RootWorkspaceRepoName for a workspace root; it is not a transport URL.
	Repository              string
	ExecutionBaseSHA        string
	OriginalParentSessionID SessionID
	OriginalWorkerSessionID SessionID
	OwnerSessionID          SessionID
	OwnerGeneration         int64
	// WorktreePath and Branch carry the exact identity supplied by AO. No path
	// normalization, filesystem probing or Git attestation happens here.
	WorktreePath string
	Branch       string
}

// Validate rejects incomplete identity, including unknown ownership. It cannot
// establish freshness or authority without an independently supplied expectation.
func (i WorkItemIdentity) Validate() error {
	for _, field := range []struct{ name, value string }{
		{"id", string(i.ID)}, {"attempt", string(i.AttemptID)},
		{"project", string(i.ProjectID)}, {"repository", i.Repository},
		{"parent", string(i.OriginalParentSessionID)},
		{"original worker", string(i.OriginalWorkerSessionID)},
		{"owner", string(i.OwnerSessionID)},
		{"worktree", i.WorktreePath}, {"branch", i.Branch},
	} {
		if field.value == "" || strings.TrimSpace(field.value) != field.value || strings.ContainsAny(field.value, "\x00\r\n") {
			return fmt.Errorf("%w: %s", ErrInvalidWorkIdentity, field.name)
		}
	}
	if i.OwnerGeneration <= 0 {
		return fmt.Errorf("%w: owner generation must be positive", ErrInvalidWorkIdentity)
	}
	if !validWorkCommitID(i.ExecutionBaseSHA) {
		return fmt.Errorf("%w: execution base must be a full commit ID", ErrInvalidWorkIdentity)
	}
	return nil
}

// ValidateAgainst checks exact identity, including the immutable execution base.
// expected must come from the caller's accepted record, not from the submission.
// This is a comparison only: it neither claims ownership nor advances generation.
func (i WorkItemIdentity) ValidateAgainst(expected WorkItemIdentity) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected: %w", err)
	}
	if err := i.Validate(); err != nil {
		return err
	}
	if i.ID == expected.ID && i.OwnerGeneration < expected.OwnerGeneration {
		return ErrStaleWorkOwner
	}
	if i != expected {
		return ErrWorkIdentityMismatch
	}
	return nil
}

// WorkItemCandidate keeps the mutable candidate HEAD separate from the original
// execution base. Validating it never updates the caller's accepted identity.
type WorkItemCandidate struct {
	Identity WorkItemIdentity
	HeadSHA  string
}

// ValidateAgainst accepts a candidate only for the caller's accepted identity.
func (c WorkItemCandidate) ValidateAgainst(expected WorkItemIdentity) error {
	if err := c.Identity.ValidateAgainst(expected); err != nil {
		return err
	}
	if !validWorkCommitID(c.HeadSHA) {
		return fmt.Errorf("%w: candidate HEAD must be a full commit ID", ErrInvalidWorkIdentity)
	}
	return nil
}

// ValidateWorkItemIdentities checks a snapshot with one current attempt per work
// item. It rejects duplicate logical IDs, attempt IDs and repository/worktree
// bindings. It is not an attempt history validator or a transactional uniqueness
// guarantee; persistence must enforce its own constraints.
func ValidateWorkItemIdentities(items []WorkItemIdentity) error {
	ids := make(map[WorkItemID]bool)
	attempts := make(map[WorkAttemptID]bool)
	type worktreeKey struct {
		project ProjectID
		repo    string
		path    string
	}
	worktrees := make(map[worktreeKey]bool)
	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
		key := worktreeKey{item.ProjectID, item.Repository, item.WorktreePath}
		if ids[item.ID] || attempts[item.AttemptID] || worktrees[key] {
			return ErrDuplicateWorkIdentity
		}
		ids[item.ID], attempts[item.AttemptID], worktrees[key] = true, true, true
	}
	return nil
}

func validWorkCommitID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
