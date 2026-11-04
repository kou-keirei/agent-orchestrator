package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ ports.WorkItemLeaseStore = (*Store)(nil)

// GetWorkItemLease reads retained lease facts, including terminal/expired rows.
func (s *Store) GetWorkItemLease(ctx context.Context, id domain.WorkItemID) (domain.WorkItemLease, bool, error) {
	lease, err := readWorkLease(ctx, s.qr, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkItemLease{}, false, nil
	}
	return lease, err == nil, err
}

func readWorkLease(ctx context.Context, q *gen.Queries, id domain.WorkItemID) (domain.WorkItemLease, error) {
	r, err := q.GetWorkItemLease(ctx, string(id))
	if err != nil {
		return domain.WorkItemLease{}, err
	}
	l := domain.WorkItemLease{WorkItemID: id, OwnerGeneration: r.OwnerGeneration, Revision: r.Revision,
		Grant:     domain.WorkLeaseGrant{ID: r.LeaseID, Duration: time.Duration(r.DurationMs) * time.Millisecond},
		ExpiresAt: time.UnixMilli(r.ExpiresAtMs).UTC(), State: r.State}
	if err := json.Unmarshal([]byte(r.AllowedEffects), &l.Grant.AllowedEffects); err != nil {
		return domain.WorkItemLease{}, err
	}
	return l, l.Grant.Validate()
}

// withWorkLease reserves the writer before any reads, including across separate
// Store opens. Every operation validates caller identity against durable facts.
func (s *Store) withWorkLease(ctx context.Context, identity domain.WorkItemIdentity, fn func(*gen.Queries, WorkItemRecord) error) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.writeMu.Unlock()
	return s.inTx(ctx, "work item lease", func(q *gen.Queries) error {
		if err := q.LockWorkItemForLease(ctx, string(identity.ID)); err != nil {
			return err
		}
		row, err := q.GetWorkItem(ctx, string(identity.ID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkItemNotFound
		}
		if err != nil {
			return err
		}
		current := workItemFromGen(row)
		if err := identity.ValidateAgainst(current.Identity); err != nil {
			return err
		}
		return fn(q, current)
	})
}

// AcquireWorkItemLease creates the first lease or reads back an exact live retry.
// Existing leases require explicit supersession, regardless of expiry/release.
func (s *Store) AcquireWorkItemLease(ctx context.Context, identity domain.WorkItemIdentity, grant domain.WorkLeaseGrant) (domain.WorkItemLease, error) {
	if err := grant.Validate(); err != nil {
		return domain.WorkItemLease{}, err
	}
	var current domain.WorkItemLease
	err := s.withWorkLease(ctx, identity, func(q *gen.Queries, _ WorkItemRecord) error {
		var err error
		current, err = readWorkLease(ctx, q, identity.ID)
		if err == nil {
			if current.OwnerGeneration == identity.OwnerGeneration && current.State == domain.WorkLeaseActive && sameGrant(current.Grant, grant) {
				return liveWorkLease(current)
			}
			return domain.ErrWorkLeaseConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		effects, err := json.Marshal(grant.AllowedEffects)
		if err != nil {
			return err
		}
		wanted := domain.WorkItemLease{WorkItemID: identity.ID, OwnerGeneration: identity.OwnerGeneration,
			Revision: 1, Grant: grant, State: domain.WorkLeaseActive,
			ExpiresAt: time.UnixMilli(time.Now().Add(grant.Duration).UnixMilli()).UTC()}
		if err := q.CreateWorkItemLease(ctx, gen.CreateWorkItemLeaseParams{
			WorkItemID: string(identity.ID), OwnerGeneration: identity.OwnerGeneration, Revision: 1,
			LeaseID: grant.ID, DurationMs: grant.Duration.Milliseconds(),
			ExpiresAtMs: wanted.ExpiresAt.UnixMilli(), AllowedEffects: string(effects), State: domain.WorkLeaseActive,
		}); err != nil {
			return err
		}
		current, err = readWorkLease(ctx, q, identity.ID)
		if err == nil && !sameWorkLease(current, wanted) {
			return domain.ErrWorkLeaseConflict
		}
		return err
	})
	return current, err
}

func sameGrant(a, b domain.WorkLeaseGrant) bool {
	return a.ID == b.ID && a.Duration == b.Duration && slices.Equal(a.AllowedEffects, b.AllowedEffects)
}

func sameWorkLease(a, b domain.WorkItemLease) bool {
	return a.WorkItemID == b.WorkItemID && a.OwnerGeneration == b.OwnerGeneration && a.Revision == b.Revision &&
		a.State == b.State && a.ExpiresAt.Equal(b.ExpiresAt) && sameGrant(a.Grant, b.Grant)
}

func liveWorkLease(l domain.WorkItemLease) error {
	if l.State != domain.WorkLeaseActive {
		return domain.ErrWorkLeaseConflict
	}
	if !time.Now().Before(l.ExpiresAt) {
		return domain.ErrWorkLeaseExpired
	}
	return nil
}

func saveWorkLease(ctx context.Context, q *gen.Queries, expected, next domain.WorkItemLease) (domain.WorkItemLease, error) {
	if expected.Revision == math.MaxInt64 {
		return expected, domain.ErrInvalidWorkLease
	}
	effects, err := json.Marshal(next.Grant.AllowedEffects)
	if err != nil {
		return expected, err
	}
	rows, err := q.CASWorkItemLease(ctx, gen.CASWorkItemLeaseParams{
		WorkItemID: string(expected.WorkItemID), ExpectedRevision: expected.Revision,
		OwnerGeneration: next.OwnerGeneration, LeaseID: next.Grant.ID, DurationMs: next.Grant.Duration.Milliseconds(),
		ExpiresAtMs: next.ExpiresAt.UnixMilli(), AllowedEffects: string(effects), State: next.State,
	})
	if err != nil {
		return expected, err
	}
	current, err := readWorkLease(ctx, q, expected.WorkItemID)
	if err != nil {
		return current, err
	}
	next.Revision = expected.Revision + 1
	if rows != 1 || !sameWorkLease(current, next) {
		return current, domain.ErrWorkLeaseConflict
	}
	return current, nil
}

// SupersedeWorkItemLease atomically advances the admitted WorkItem owner and
// replaces the exact expected lease. It never changes the original identity.
func (s *Store) SupersedeWorkItemLease(ctx context.Context, identity domain.WorkItemIdentity, expected domain.WorkItemLease, owner domain.SessionID, grant domain.WorkLeaseGrant) (domain.WorkItemLease, error) {
	if err := grant.Validate(); err != nil {
		return domain.WorkItemLease{}, err
	}
	nextIdentity := identity
	if identity.OwnerGeneration == math.MaxInt64 {
		return domain.WorkItemLease{}, domain.ErrInvalidWorkIdentity
	}
	nextIdentity.OwnerSessionID, nextIdentity.OwnerGeneration = owner, identity.OwnerGeneration+1
	if err := nextIdentity.Validate(); err != nil {
		return domain.WorkItemLease{}, err
	}
	var current domain.WorkItemLease
	err := s.withWorkLease(ctx, identity, func(q *gen.Queries, record WorkItemRecord) error {
		var err error
		current, err = readWorkLease(ctx, q, identity.ID)
		if err != nil {
			return err
		}
		if !sameWorkLease(expected, current) || current.State == domain.WorkLeaseCompleted || grant.ID == current.Grant.ID {
			return domain.ErrWorkLeaseConflict
		}
		rows, err := q.ClaimWorkItemOwner(ctx, gen.ClaimWorkItemOwnerParams{
			NextOwner: string(owner), ID: string(identity.ID), AttemptID: string(identity.AttemptID),
			ProjectID: string(identity.ProjectID), Repository: identity.Repository, ExecutionBaseSha: identity.ExecutionBaseSHA,
			OriginalParentSessionID: string(identity.OriginalParentSessionID), OriginalWorkerSessionID: string(identity.OriginalWorkerSessionID),
			OriginalNativeSessionID: record.OriginalNativeSessionID, OwnerSessionID: string(identity.OwnerSessionID),
			OwnerGeneration: identity.OwnerGeneration, WorktreePath: identity.WorktreePath, Branch: identity.Branch,
		})
		if err != nil {
			return err
		}
		row, err := q.GetWorkItem(ctx, string(identity.ID))
		if err != nil {
			return err
		}
		if rows != 1 || nextIdentity.ValidateAgainst(workItemFromGen(row).Identity) != nil {
			return domain.ErrWorkIdentityMismatch
		}
		next := current
		next.OwnerGeneration, next.Grant, next.State = nextIdentity.OwnerGeneration, grant, domain.WorkLeaseActive
		next.ExpiresAt = time.UnixMilli(time.Now().Add(grant.Duration).UnixMilli()).UTC()
		current, err = saveWorkLease(ctx, q, current, next)
		return err
	})
	return current, err
}

// RenewWorkItemLease extends a live lease to an exact millisecond deadline.
// Exact retry readback does not grant authority after the deadline has elapsed.
func (s *Store) RenewWorkItemLease(ctx context.Context, identity domain.WorkItemIdentity, expected domain.WorkItemLease, until time.Time) (domain.WorkItemLease, error) {
	if until.IsZero() || !until.Equal(time.UnixMilli(until.UnixMilli())) || !until.After(expected.ExpiresAt) {
		return domain.WorkItemLease{}, domain.ErrInvalidWorkLease
	}
	next := expected
	next.ExpiresAt = until.UTC()
	return s.transitionWorkLease(ctx, identity, expected, next, true)
}

// ReleaseWorkItemLease ends authorization, including for an expired lease.
func (s *Store) ReleaseWorkItemLease(ctx context.Context, identity domain.WorkItemIdentity, expected domain.WorkItemLease) (domain.WorkItemLease, error) {
	next := expected
	next.State = domain.WorkLeaseReleased
	return s.transitionWorkLease(ctx, identity, expected, next, false)
}

// CompleteWorkItemLease terminally completes a live lease; retries read it back.
func (s *Store) CompleteWorkItemLease(ctx context.Context, identity domain.WorkItemIdentity, expected domain.WorkItemLease) (domain.WorkItemLease, error) {
	next := expected
	next.State = domain.WorkLeaseCompleted
	return s.transitionWorkLease(ctx, identity, expected, next, true)
}

func (s *Store) transitionWorkLease(ctx context.Context, identity domain.WorkItemIdentity, expected, next domain.WorkItemLease, requireLive bool) (domain.WorkItemLease, error) {
	var current domain.WorkItemLease
	err := s.withWorkLease(ctx, identity, func(q *gen.Queries, _ WorkItemRecord) error {
		var err error
		current, err = readWorkLease(ctx, q, identity.ID)
		if err != nil {
			return err
		}
		if current.OwnerGeneration != identity.OwnerGeneration || expected.Revision <= 0 || expected.Revision == math.MaxInt64 {
			return domain.ErrWorkLeaseConflict
		}
		retry := next
		retry.Revision++
		if sameWorkLease(retry, current) {
			return nil
		}
		if !sameWorkLease(expected, current) || current.State != domain.WorkLeaseActive {
			return domain.ErrWorkLeaseConflict
		}
		if requireLive {
			if err := liveWorkLease(current); err != nil {
				return err
			}
		}
		current, err = saveWorkLease(ctx, q, current, next)
		return err
	})
	return current, err
}

// CheckWorkItemEffect checks only a hypothetical effect. No provider permission
// is inferred, no external operation runs, and no reusable permit is returned.
func (s *Store) CheckWorkItemEffect(ctx context.Context, candidate domain.WorkItemCandidate, expected domain.WorkItemLease, effect string) error {
	return s.withWorkLease(ctx, candidate.Identity, func(q *gen.Queries, record WorkItemRecord) error {
		if err := candidate.ValidateAgainst(record.Identity); err != nil {
			return err
		}
		current, err := readWorkLease(ctx, q, candidate.Identity.ID)
		if err != nil {
			return err
		}
		if current.OwnerGeneration != record.Identity.OwnerGeneration || !sameWorkLease(expected, current) {
			return domain.ErrWorkLeaseConflict
		}
		if err := liveWorkLease(current); err != nil {
			return err
		}
		if !slices.Contains(current.Grant.AllowedEffects, effect) {
			return domain.ErrWorkEffectDenied
		}
		return nil
	})
}
