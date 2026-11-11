package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ ports.WorkItemAdmissionStore = (*Store)(nil)

func readWorkAdmission(ctx context.Context, db gen.DBTX, id domain.WorkItemID) (domain.WorkItemAdmission, error) {
	var a domain.WorkItemAdmission
	err := db.QueryRowContext(ctx, `SELECT work_item_id, attempt_id, owner_session_id, owner_generation, lease_id, lease_revision, state
		FROM work_item_admissions WHERE work_item_id=?`, id).Scan(
		&a.WorkItemID, &a.AttemptID, &a.OwnerSessionID, &a.OwnerGeneration, &a.LeaseID, &a.LeaseRevision, &a.State)
	return a, err
}

// withWorkAdmission takes SQLite's writer reservation before reading identity,
// lease or global occupancy, also across independent Store opens.
func (s *Store) withWorkAdmission(ctx context.Context, identity domain.WorkItemIdentity, fn func(*gen.Queries, gen.DBTX) error) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	// Provider projections already own writeMu and do not expose a raw handle.
	if q, ok := ctx.Value(conversationProjectionTxKey{}).(*gen.Queries); ok && q != nil {
		return domain.ErrWorkAdmissionHeld
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.writeMu.Unlock()
	return s.inTxDB(ctx, "work item admission", func(q *gen.Queries, db gen.DBTX) error {
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
		if err := identity.ValidateAgainst(workItemFromGen(row).Identity); err != nil {
			return err
		}
		return fn(q, db)
	})
}

// ReserveWorkItemAdmission persists one slot without performing any external
// effect. Exact readback is idempotency, never a second dispatch authorization.
func (s *Store) ReserveWorkItemAdmission(ctx context.Context, identity domain.WorkItemIdentity, expected domain.WorkItemLease) (domain.WorkItemAdmission, error) {
	var current domain.WorkItemAdmission
	err := s.withWorkAdmission(ctx, identity, func(q *gen.Queries, db gen.DBTX) error {
		var err error
		current, err = reserveWorkItemAdmission(ctx, q, db, identity, expected)
		return err
	})
	return current, err
}

func workAdmissionFor(identity domain.WorkItemIdentity, expected domain.WorkItemLease) domain.WorkItemAdmission {
	return domain.WorkItemAdmission{WorkItemID: identity.ID, AttemptID: identity.AttemptID,
		OwnerSessionID: identity.OwnerSessionID, OwnerGeneration: identity.OwnerGeneration,
		LeaseID: expected.Grant.ID, LeaseRevision: expected.Revision, State: domain.WorkAdmissionReserved}
}

// Caller owns the Store writer lock and SQLite writer-fenced transaction.
func reserveWorkItemAdmission(ctx context.Context, q *gen.Queries, db gen.DBTX, identity domain.WorkItemIdentity, expected domain.WorkItemLease) (domain.WorkItemAdmission, error) {
	wanted := workAdmissionFor(identity, expected)

	lease, err := readWorkLease(ctx, q, identity.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkItemAdmission{}, domain.ErrWorkLeaseConflict
	}
	if err != nil {
		return domain.WorkItemAdmission{}, err
	}
	if lease.OwnerGeneration != identity.OwnerGeneration || !sameWorkLease(lease, expected) {
		return domain.WorkItemAdmission{}, domain.ErrWorkLeaseConflict
	}
	if err := liveWorkLease(lease); err != nil {
		return domain.WorkItemAdmission{}, err
	}
	current, err := readWorkAdmission(ctx, db, identity.ID)
	if err == nil {
		if current != wanted {
			return domain.WorkItemAdmission{}, domain.ErrWorkAdmissionHeld
		}
		return current, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.WorkItemAdmission{}, err
	}
	var occupied int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM work_item_admissions WHERE state='reserved'`).Scan(&occupied); err != nil {
		return domain.WorkItemAdmission{}, err
	}
	if occupied != 0 {
		return domain.WorkItemAdmission{}, domain.ErrWorkAdmissionHeld
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO work_item_admissions
			(work_item_id, attempt_id, owner_session_id, owner_generation, lease_id, lease_revision, state) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		wanted.WorkItemID, wanted.AttemptID, wanted.OwnerSessionID, wanted.OwnerGeneration, wanted.LeaseID, wanted.LeaseRevision, wanted.State); err != nil {
		return domain.WorkItemAdmission{}, err
	}
	current, err = readWorkAdmission(ctx, db, identity.ID)
	if err == nil && current != wanted {
		return domain.WorkItemAdmission{}, domain.ErrWorkAdmissionHeld
	}
	return current, err
}

// ReleaseWorkItemAdmission explicitly frees reconciled occupancy, even after
// lease expiry. It preserves the receipt so retries cannot reserve this attempt.
func (s *Store) ReleaseWorkItemAdmission(ctx context.Context, identity domain.WorkItemIdentity, expected domain.WorkItemAdmission) (domain.WorkItemAdmission, error) {
	var current domain.WorkItemAdmission
	err := s.withWorkAdmission(ctx, identity, func(_ *gen.Queries, db gen.DBTX) error {
		if expected.WorkItemID != identity.ID || expected.AttemptID != identity.AttemptID ||
			expected.OwnerSessionID != identity.OwnerSessionID || expected.OwnerGeneration != identity.OwnerGeneration ||
			expected.State != domain.WorkAdmissionReserved {
			return domain.ErrWorkAdmissionHeld
		}
		var err error
		current, err = readWorkAdmission(ctx, db, identity.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrWorkAdmissionHeld
		}
		if err != nil {
			return err
		}
		next := expected
		next.State = domain.WorkAdmissionReleased
		if current == next {
			return nil
		}
		if current != expected {
			return domain.ErrWorkAdmissionHeld
		}
		if _, err := db.ExecContext(ctx, `UPDATE work_item_admissions SET state='released' WHERE work_item_id=?`, identity.ID); err != nil {
			return err
		}
		current, err = readWorkAdmission(ctx, db, identity.ID)
		if err == nil && current != next {
			return domain.ErrWorkAdmissionHeld
		}
		return err
	})
	return current, err
}
