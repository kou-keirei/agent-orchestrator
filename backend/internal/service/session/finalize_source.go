package session

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// FinalizeSource is the native store's scalar read-only surface. No lifecycle,
// runtime, SQL writer, or filesystem capability is granted to this adapter.
type FinalizeSource interface {
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
	GetSessionCleanupFacts(context.Context, domain.SessionID) (domain.SessionCleanupRecord, bool, error)
}

// ErrFinalizeSourceUnavailable means bounded native collection is unsupported.
// Callers can use errors.Is; there is no fallback to unbounded store methods.
var ErrFinalizeSourceUnavailable = errors.New("SOURCE_UNAVAILABLE")

// FinalizeBoundedSource must enforce max during native collection, before
// materializing results, for initial reads AND readbacks. Each method returns
// the complete set or an error on overflow, never a silently truncated set.
// Wrapping an unbounded ListAll/List method does not satisfy this contract.
// The SQLite store supplies this seam with bounded native reads. Legacy sources
// without it are rejected before any reads; unavailable native reads fail closed.
type FinalizeBoundedSource interface {
	FinalizeSource
	ListFinalizeSessions(context.Context, int) ([]domain.SessionRecord, error)
	ListFinalizePRs(context.Context, domain.SessionID, int) ([]domain.PRFacts, error)
	ListFinalizeWorktrees(context.Context, domain.SessionID, int) ([]domain.SessionWorktreeRecord, error)
}

// FinalizeGitBinding scopes a trusted observation to an exact native read.
type FinalizeGitBinding struct {
	SessionID            domain.SessionID
	Revision, Generation int64
	Record               domain.SessionWorktreeRecord
}

// FinalizeGitObservation must echo the binding and exact ownership record.
type FinalizeGitObservation struct {
	Binding  FinalizeGitBinding
	Evidence FinalizeWorktree
}

// FinalizeGitSource is explicitly trusted read-only evidence, not a Git executor.
// It must inspect link/reparse aliases including ancestors, preserve unknowns,
// and supply branch/HEAD readbacks with the semantics of FinalizeWorktree.
// A nil source or a failed observation supplies no positive evidence.
type FinalizeGitSource interface {
	ObserveFinalizeWorktree(context.Context, FinalizeGitBinding) (FinalizeGitObservation, error)
}

const (
	// FinalizePlanOnly is diagnostic output, never an apply authorization.
	FinalizePlanOnly = "PLAN_ONLY"
	// FinalizeSourceMaxSessions bounds the frozen inventory accepted per preview.
	FinalizeSourceMaxSessions = 10000
	// FinalizeSourceMaxFacts bounds PR/worktree evidence accepted per session.
	FinalizeSourceMaxFacts = 200
)

// FinalizeSourceRow exposes both native evidence and its composed held plan.
type FinalizeSourceRow struct {
	Evidence FinalizeSnapshot
	Plan     FinalizePlan
}

// FinalizeSourcePage contains at most FinalizePlanPageSize rows.
type FinalizeSourcePage struct {
	Mode              string
	Rows              []FinalizeSourceRow
	Total, NextOffset int
}

// FinalizePreview retains an exact inventory for stable offsets, not an atomic
// database snapshot. New/deleted sessions require a new preview; changed rows
// are held. Collection requires a source that enforces native hard limits.
// Matching readbacks cannot detect ABA and do not qualify native release.
type FinalizePreview struct {
	source    FinalizeBoundedSource
	git       FinalizeGitSource
	inventory []finalizeSourceIdentity
}

type finalizeSourceIdentity struct {
	id                   domain.SessionID
	revision, generation int64
	owner                domain.SessionControllerOwner
}

// NewFinalizePreview freezes IDs and identity fences using native enumeration,
// including sessions with missing cleanup rows. It does not scan live AO data
// itself. The caller supplies the store; no daemon or database is opened here.
func NewFinalizePreview(ctx context.Context, source FinalizeSource, git FinalizeGitSource) (*FinalizePreview, error) {
	if source == nil {
		return nil, fmt.Errorf("finalize source: missing source")
	}
	bounded, ok := source.(FinalizeBoundedSource)
	if !ok {
		return nil, ErrFinalizeSourceUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records, err := bounded.ListFinalizeSessions(ctx, FinalizeSourceMaxSessions)
	if err != nil {
		return nil, fmt.Errorf("finalize source inventory: %w", err)
	}
	if len(records) > FinalizeSourceMaxSessions {
		return nil, fmt.Errorf("finalize source: inventory exceeds %d", FinalizeSourceMaxSessions)
	}
	p := &FinalizePreview{source: bounded, git: git}
	for _, r := range records {
		if !finalizeKey(string(r.ID)) || len(r.ID) > 256 {
			return nil, fmt.Errorf("finalize source: invalid exact session ID")
		}
		p.inventory = append(p.inventory, finalizeSourceIdentity{r.ID, r.Revision, r.CleanupGeneration, r.ControllerOwner()})
	}
	slices.SortFunc(p.inventory, func(a, b finalizeSourceIdentity) int { return strings.Compare(string(a.id), string(b.id)) })
	for i := 1; i < len(p.inventory); i++ {
		if p.inventory[i-1].id == p.inventory[i].id {
			return nil, fmt.Errorf("finalize source: duplicate exact session ID")
		}
	}
	return p, ctx.Err()
}

// Page composes native evidence with PlanFinalize, without changing the planner.
// Per-row read failures become typed missing-evidence holds; cancellation stops
// the page. Readbacks detect conflicts but confer no transactional guarantee.
func (p *FinalizePreview) Page(ctx context.Context, offset int) (FinalizeSourcePage, error) {
	page := FinalizeSourcePage{Mode: FinalizePlanOnly, Total: len(p.inventory), NextOffset: -1}
	if offset < 0 || offset > len(p.inventory) {
		return page, fmt.Errorf("finalize source: offset out of range")
	}
	end := offset + min(FinalizePlanPageSize, len(p.inventory)-offset)
	for _, identity := range p.inventory[offset:end] {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		page.Rows = append(page.Rows, p.collect(ctx, identity))
	}
	if end < len(p.inventory) {
		page.NextOffset = end
	}
	return page, ctx.Err()
}

func (p *FinalizePreview) collect(ctx context.Context, identity finalizeSourceIdentity) FinalizeSourceRow {
	s := FinalizeSnapshot{SessionID: identity.id}
	var holds []FinalizeHold
	hold := func(field string, code FinalizeHoldCode) { holds = append(holds, FinalizeHold{field, code}) }
	r, ok, err := p.source.GetSession(ctx, identity.id)
	if err != nil || !ok {
		hold("source.session", FinalizeMissing)
		return FinalizeSourceRow{s, finalizeSourcePlan(s, holds)}
	}
	s.Session = r
	check := func(next domain.SessionRecord) {
		if next.ID != identity.id || next.ControllerOwner() != identity.owner {
			hold("source.session.owner", FinalizeIdentityMismatch)
		}
		if next.CleanupGeneration != identity.generation {
			hold("source.session.generation", FinalizeGenerationMismatch)
		}
		if next.Revision != identity.revision {
			hold("source.session.revision", FinalizeRevisionMismatch)
		}
	}
	check(r)
	s.Cleanup, s.CleanupPresent, err = p.source.GetSessionCleanupFacts(ctx, identity.id)
	if err != nil {
		s.Cleanup, s.CleanupPresent = domain.SessionCleanupRecord{}, false
		hold("source.cleanup", FinalizeMissing)
	}
	// No cleanup revision exists in this store. Never synthesize one from the
	// session revision, timestamps, retry counts, or a content hash.
	prs, prErr := p.source.ListFinalizePRs(ctx, identity.id, FinalizeSourceMaxFacts)
	if prErr == nil && len(prs) <= FinalizeSourceMaxFacts {
		prs = slices.Clone(prs)
		s.PRs, s.PRsComplete = slices.Clone(prs), FinalizeYes
	} else {
		hold("source.prs", FinalizeMissing)
	}
	worktrees, wtErr := p.source.ListFinalizeWorktrees(ctx, identity.id, FinalizeSourceMaxFacts)
	if wtErr == nil && len(worktrees) <= FinalizeSourceMaxFacts {
		worktrees = slices.Clone(worktrees)
		s.WorktreesComplete = FinalizeYes
		// Single-repo legacy workspaces may live only in session metadata; an
		// empty registration set cannot prove that workspace is absent.
		if r.Metadata.WorkspacePath != "" && !slices.ContainsFunc(worktrees, func(w domain.SessionWorktreeRecord) bool {
			return w.WorktreePath == r.Metadata.WorkspacePath && w.SessionID == identity.id
		}) {
			s.WorktreesComplete = FinalizeUnknown
		}
		for _, record := range worktrees {
			w := FinalizeWorktree{Record: record}
			if p.git != nil && record.SessionID == identity.id {
				binding := FinalizeGitBinding{identity.id, r.Revision, r.CleanupGeneration, record}
				observation, gitErr := p.git.ObserveFinalizeWorktree(ctx, binding)
				if gitErr != nil {
					hold("source.git", FinalizeMissing)
				} else if observation.Binding != binding || observation.Evidence.Record != record {
					hold("source.git.binding", FinalizeIdentityMismatch)
				} else {
					w = observation.Evidence
				}
			}
			s.Worktrees = append(s.Worktrees, w)
		}
	} else {
		hold("source.worktrees", FinalizeMissing)
	}
	// Compare independently persisted facts too: their writes need not advance
	// the session revision. Equal values still do not prove an atomic snapshot.
	cleanup, present, cleanupErr := p.source.GetSessionCleanupFacts(ctx, identity.id)
	if cleanupErr != nil {
		hold("source.cleanup.readback", FinalizeMissing)
	} else if present != s.CleanupPresent || cleanup != s.Cleanup {
		hold("source.cleanup.readback", FinalizeRevisionMismatch)
	}
	prBack, err := p.source.ListFinalizePRs(ctx, identity.id, FinalizeSourceMaxFacts)
	if err != nil || len(prBack) > FinalizeSourceMaxFacts {
		hold("source.prs.readback", FinalizeMissing)
	} else if prErr == nil && !slices.Equal(prs, prBack) {
		hold("source.prs.readback", FinalizeRevisionMismatch)
	}
	wtBack, err := p.source.ListFinalizeWorktrees(ctx, identity.id, FinalizeSourceMaxFacts)
	if err != nil || len(wtBack) > FinalizeSourceMaxFacts {
		hold("source.worktrees.readback", FinalizeMissing)
	} else if wtErr == nil && !slices.Equal(worktrees, wtBack) {
		hold("source.worktrees.readback", FinalizeRevisionMismatch)
	}
	back, ok, err := p.source.GetSession(ctx, identity.id)
	if err != nil || !ok {
		hold("source.session.readback", FinalizeMissing)
	} else {
		s.SessionRevisionReadback = back.Revision
		check(back)
	}
	// RuntimeReleasedAt is persisted bookkeeping, not #20 release proof.
	// Pending work, backup/restore, resume and task completion have no qualified
	// source on this seam and intentionally retain their zero UNKNOWN values.
	return FinalizeSourceRow{s, finalizeSourcePlan(s, holds)}
}

func finalizeSourcePlan(s FinalizeSnapshot, holds []FinalizeHold) FinalizePlan {
	p := PlanFinalize(s)
	p.Holds = append(p.Holds, holds...)
	return p
}
