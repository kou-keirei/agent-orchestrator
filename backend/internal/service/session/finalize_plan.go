package session

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// FinalizeFact is supplied evidence. Zero and unrecognized values fail closed.
type FinalizeFact uint8

const (
	// FinalizeUnknown means no qualified observation is available.
	FinalizeUnknown FinalizeFact = iota
	// FinalizeNo is an explicit negative observation.
	FinalizeNo
	// FinalizeYes is an explicit positive observation.
	FinalizeYes
)

// FinalizeWorktree binds Git observations to an exact native ownership record.
// LinkOrReparse covers the path AND its ancestors; unknown path inspection is
// not a negative result. UniqueCommits must cover all commits needing preservation,
// not merely commits ahead of an arbitrary local tracking ref. Git owns semantics.
type FinalizeWorktree struct {
	Record                                                                domain.SessionWorktreeRecord
	Branch, Head, BranchReadback, HeadReadback                            string
	OwnerKnown, Protected, Dirty, Untracked, UniqueCommits, LinkOrReparse FinalizeFact
}

// FinalizeSnapshot is a caller-supplied, read-only evidence bundle, not a native
// collector or authorization token. All observations must cover this exact session,
// generation and revision. PRsComplete and WorktreesComplete explicitly distinguish
// a complete empty set from an unavailable/partial enumeration.
//
// INTEGRATION UNQUALIFIED: this base has no qualified #20 positive owner-release
// source, backup/restore observation seam, or cleanup-row revision in its domain
// record. CleanupRevision tokens below must come from an authoritative caller;
// never synthesize them from timestamps/attempt counts or use session Revision as
// a substitute. Matching readbacks do not establish an atomic snapshot. Missing
// evidence stays unknown. No daemon/CLI/API wiring or cleanup writer is changed.
type FinalizeSnapshot struct {
	SessionID                                                                   domain.SessionID
	Session                                                                     domain.SessionRecord
	SessionRevisionReadback                                                     int64
	Cleanup                                                                     domain.SessionCleanupRecord
	CleanupPresent                                                              bool
	CleanupRevision, CleanupRevisionReadback                                    string
	TaskCompleted, PendingTurns, PendingApprovals, PendingReview, PendingRepair FinalizeFact
	Protected, Resumable, ProviderResume, Backup, Restore, OwnerReleased        FinalizeFact
	PRsComplete, WorktreesComplete                                              FinalizeFact
	PRs                                                                         []domain.PRFacts
	Worktrees                                                                   []FinalizeWorktree
}

// FinalizeHoldCode identifies why a diagnostic plan must be held.
type FinalizeHoldCode string

const (
	// FinalizeMissing means a required observation is unavailable.
	FinalizeMissing FinalizeHoldCode = "MISSING_EVIDENCE"
	// FinalizeInvalid means an observation is unrecognized or malformed.
	FinalizeInvalid FinalizeHoldCode = "INVALID_EVIDENCE"
	// FinalizeProtected means an explicit fact prevents finalization.
	FinalizeProtected FinalizeHoldCode = "PROTECTED"
	// FinalizeIdentityMismatch means evidence names a different owner or branch.
	FinalizeIdentityMismatch FinalizeHoldCode = "IDENTITY_MISMATCH"
	// FinalizeGenerationMismatch rejects cleanup from another terminal episode.
	FinalizeGenerationMismatch FinalizeHoldCode = "GENERATION_MISMATCH"
	// FinalizeRevisionMismatch rejects evidence changed during observation.
	FinalizeRevisionMismatch FinalizeHoldCode = "REVISION_MISMATCH"
	// FinalizeReleaseUnqualified is unconditional until #20 is qualified separately.
	FinalizeReleaseUnqualified FinalizeHoldCode = "NATIVE_RELEASE_UNQUALIFIED"
)

// FinalizeHold preserves a stable field path and typed reason, in check order.
type FinalizeHold struct {
	Field string
	Code  FinalizeHoldCode
}

// FinalizePlanState is a diagnostic state, never an executable instruction.
type FinalizePlanState string

// FinalizeHeld is currently the only supported disposition.
const FinalizeHeld FinalizePlanState = "HELD"

// FinalizePlan preserves the identity fences and Git evidence used for a plan.
// It grants no authority to finalize, delete, kill, prune, restore or write SQL.
type FinalizePlan struct {
	SessionID                                domain.SessionID
	Generation, Revision                     int64
	Cleanup                                  domain.SessionCleanupRecord
	CleanupPresent                           bool
	CleanupRevision, CleanupRevisionReadback string
	SessionRevisionReadback                  int64
	Worktrees                                []FinalizeWorktree
	State                                    FinalizePlanState
	PhysicalRemovalEligible                  bool
	Holds                                    []FinalizeHold
}

// PlanFinalize is a pure callable planner: it performs no I/O, owns no storage or
// lifecycle, uses no age heuristic, and cannot qualify physical removal. Even
// RELEASED/ALREADY_ABSENT claims or DispositionRemoved cannot bypass the permanent
// native-release qualification hold. A future integration requires exact-source
// review and authoritative revalidation at the existing cleanup boundary.
func PlanFinalize(s FinalizeSnapshot) FinalizePlan {
	p := FinalizePlan{
		SessionID: s.SessionID, Generation: s.Session.CleanupGeneration, Revision: s.Session.Revision,
		Cleanup: s.Cleanup, CleanupPresent: s.CleanupPresent,
		CleanupRevision: s.CleanupRevision, CleanupRevisionReadback: s.CleanupRevisionReadback,
		SessionRevisionReadback: s.SessionRevisionReadback,
		Worktrees:               slices.Clone(s.Worktrees), State: FinalizeHeld,
	}
	add := func(field string, code FinalizeHoldCode) { p.Holds = append(p.Holds, FinalizeHold{field, code}) }
	fact := func(field string, value, safe FinalizeFact) {
		switch value {
		case FinalizeUnknown:
			add(field, FinalizeMissing)
		case FinalizeYes, FinalizeNo:
			if value != safe {
				add(field, FinalizeProtected)
			}
		default:
			add(field, FinalizeInvalid)
		}
	}
	if !finalizeKey(string(s.SessionID)) || s.Session.ID != s.SessionID {
		add("session.id", FinalizeIdentityMismatch)
	}
	if s.Session.Revision <= 0 || s.SessionRevisionReadback <= 0 {
		add("session.revision", FinalizeMissing)
	} else if s.Session.Revision != s.SessionRevisionReadback {
		add("session.revision", FinalizeRevisionMismatch)
	}
	if s.Session.CleanupGeneration < 0 {
		add("session.generation", FinalizeInvalid)
	}
	if !s.CleanupPresent {
		add("cleanup", FinalizeMissing)
	} else {
		if s.Cleanup.SessionID != s.SessionID {
			add("cleanup.session_id", FinalizeIdentityMismatch)
		}
		if s.Cleanup.SessionGeneration < 0 || s.Cleanup.SessionGeneration != s.Session.CleanupGeneration {
			add("cleanup.generation", FinalizeGenerationMismatch)
		}
		if s.Cleanup.RuntimeReleasedAt.IsZero() {
			add("cleanup.runtime_release", FinalizeMissing)
		}
		switch s.Cleanup.WorkspaceDisposition {
		case domain.DispositionPending, domain.DispositionRemoved, domain.DispositionNotApplicable:
		case domain.DispositionPreservedDirty, domain.DispositionFailed:
			add("cleanup.disposition", FinalizeProtected)
		case "":
			add("cleanup.disposition", FinalizeMissing)
		default:
			add("cleanup.disposition", FinalizeInvalid)
		}
	}
	if !finalizeKey(s.CleanupRevision) || !finalizeKey(s.CleanupRevisionReadback) {
		add("cleanup.revision", FinalizeMissing)
	} else if s.CleanupRevision != s.CleanupRevisionReadback {
		add("cleanup.revision", FinalizeRevisionMismatch)
	}
	if !s.Session.IsTerminated {
		add("session.terminated", FinalizeProtected)
	}
	if s.Session.Kind != domain.KindWorker {
		add("session.kind", FinalizeProtected)
	}
	if s.Session.IsPinned {
		add("session.pinned", FinalizeProtected)
	}
	if s.Session.Activity.State != domain.ActivityExited {
		add("session.activity", FinalizeProtected)
	}
	for _, f := range []struct {
		field       string
		value, safe FinalizeFact
	}{
		{"task.completed", s.TaskCompleted, FinalizeYes},
		{"pending.turns", s.PendingTurns, FinalizeNo},
		{"pending.approvals", s.PendingApprovals, FinalizeNo},
		{"pending.review", s.PendingReview, FinalizeNo},
		{"pending.repair", s.PendingRepair, FinalizeNo},
		{"session.protected", s.Protected, FinalizeNo},
		{"session.resumable", s.Resumable, FinalizeNo},
		{"provider.resume", s.ProviderResume, FinalizeYes},
		{"backup", s.Backup, FinalizeYes}, {"restore", s.Restore, FinalizeYes},
		{"owner.release", s.OwnerReleased, FinalizeYes},
		{"prs.complete", s.PRsComplete, FinalizeYes},
		{"worktrees.complete", s.WorktreesComplete, FinalizeYes},
	} {
		fact(f.field, f.value, f.safe)
	}
	for i, pr := range s.PRs {
		prefix := fmt.Sprintf("prs[%d]", i)
		if !finalizeKey(pr.SourceBranch) || !finalizeKey(pr.HeadSHA) {
			add(prefix+".git_identity", FinalizeMissing)
		} else if !slices.ContainsFunc(s.Worktrees, func(w FinalizeWorktree) bool {
			return w.Record.SessionID == s.SessionID && w.Record.Branch == pr.SourceBranch && w.Branch == pr.SourceBranch && w.Head == pr.HeadSHA
		}) {
			add(prefix+".git_identity", FinalizeIdentityMismatch)
		}
		if !pr.Merged && !pr.Closed {
			add(prefix+".pending", FinalizeProtected)
		}
		if pr.Draft || pr.ReviewComments || pr.Review == domain.ReviewChangesRequest || pr.Review == domain.ReviewRequired {
			add(prefix+".review", FinalizeProtected)
		} else if pr.Review != domain.ReviewNone && pr.Review != domain.ReviewApproved {
			add(prefix+".review", FinalizeMissing)
		}
		if pr.CI == "" || pr.CI == domain.CIUnknown {
			add(prefix+".ci", FinalizeMissing)
		} else if pr.CI != domain.CIPassing {
			add(prefix+".ci", FinalizeProtected)
		}
	}
	seen := make(map[string]bool, len(s.Worktrees))
	for i, w := range s.Worktrees {
		prefix := fmt.Sprintf("worktrees[%d]", i)
		if w.Record.SessionID != s.SessionID {
			add(prefix+".owner", FinalizeIdentityMismatch)
		}
		if !finalizeKey(w.Record.WorktreePath) || seen[w.Record.WorktreePath] {
			add(prefix+".path", FinalizeInvalid)
		}
		seen[w.Record.WorktreePath] = true
		if !finalizeKey(w.Record.Branch) || w.Branch != w.Record.Branch {
			add(prefix+".branch", FinalizeIdentityMismatch)
		}
		if !finalizeKey(w.Head) || !finalizeKey(w.HeadReadback) {
			add(prefix+".head", FinalizeMissing)
		}
		if w.Branch != w.BranchReadback || w.Head != w.HeadReadback {
			add(prefix+".git_readback", FinalizeRevisionMismatch)
		}
		if w.Record.PreservedRef != "" {
			add(prefix+".preserved_ref", FinalizeProtected)
		}
		for _, f := range []struct {
			field       string
			value, safe FinalizeFact
		}{
			{"owner", w.OwnerKnown, FinalizeYes}, {"protected", w.Protected, FinalizeNo},
			{"dirty", w.Dirty, FinalizeNo}, {"untracked", w.Untracked, FinalizeNo},
			{"unique_commits", w.UniqueCommits, FinalizeNo}, {"link_or_reparse", w.LinkOrReparse, FinalizeNo},
		} {
			fact(prefix+"."+f.field, f.value, f.safe)
		}
	}
	add("owner.release_qualification", FinalizeReleaseUnqualified)
	return p
}

func finalizeKey(s string) bool {
	return s != "" && strings.TrimSpace(s) == s && !strings.ContainsRune(s, '\x00')
}

// FinalizePlanPageSize bounds diagnostic output per call.
const FinalizePlanPageSize = 200

// FinalizePlanPage covers supplied snapshots only; Total does not claim native
// inventory completeness. NextOffset is -1 at end. Reuse offsets only against the
// same frozen input; a changed enumeration must restart at zero.
type FinalizePlanPage struct {
	Plans             []FinalizePlan
	Total, NextOffset int
}

// PlanFinalizePage sorts by exact session ID without changing inputs. Blank or
// duplicate IDs anywhere (including beyond the page) reject the entire request.
// This is in-memory pagination, not a replacement store or candidate scanner.
func PlanFinalizePage(snapshots []FinalizeSnapshot, offset int) (FinalizePlanPage, error) {
	p := FinalizePlanPage{Total: len(snapshots), NextOffset: -1}
	if offset < 0 || offset > len(snapshots) {
		return p, fmt.Errorf("finalize plan: offset out of range")
	}
	ordered := slices.Clone(snapshots)
	slices.SortFunc(ordered, func(a, b FinalizeSnapshot) int { return strings.Compare(string(a.SessionID), string(b.SessionID)) })
	for i, s := range ordered {
		if !finalizeKey(string(s.SessionID)) || (i > 0 && ordered[i-1].SessionID == s.SessionID) {
			return p, fmt.Errorf("finalize plan: blank, invalid or duplicate session ID %q", s.SessionID)
		}
	}
	end := offset + min(FinalizePlanPageSize, len(ordered)-offset)
	for _, s := range ordered[offset:end] {
		p.Plans = append(p.Plans, PlanFinalize(s))
	}
	if end < len(ordered) {
		p.NextOffset = end
	}
	return p, nil
}
