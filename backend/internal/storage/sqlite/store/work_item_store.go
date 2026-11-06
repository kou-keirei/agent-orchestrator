package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// ErrWorkItemNotFound reports an owner claim for a work item that does not exist.
var ErrWorkItemNotFound = errors.New("work item not found")

// WorkItemRecord persists the frozen domain identity plus the original worker's
// opaque native session reference. Empty means unknown; it grants no native
// continuation authority. References are retained even after session cleanup.
type WorkItemRecord struct {
	Identity                domain.WorkItemIdentity
	OriginalNativeSessionID string
}

func (r WorkItemRecord) validate() error {
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	native := r.OriginalNativeSessionID
	if strings.TrimSpace(native) != native || strings.ContainsAny(native, "\x00\r\n") {
		return fmt.Errorf("%w: original native session", domain.ErrInvalidWorkIdentity)
	}
	return nil
}

func (r WorkItemRecord) validateAgainst(current WorkItemRecord) error {
	if err := r.Identity.ValidateAgainst(current.Identity); err != nil {
		return err
	}
	if r.OriginalNativeSessionID != current.OriginalNativeSessionID {
		return domain.ErrWorkIdentityMismatch
	}
	return nil
}

// CreateWorkItem atomically creates an admitted identity at generation 1 and
// reads it back. An exact duplicate is idempotent. Conflicting IDs, attempts or
// exact worktree paths never overwrite an existing record. Admission is the
// caller's responsibility; this store does not probe sessions, paths or Git.
func (s *Store) CreateWorkItem(ctx context.Context, record WorkItemRecord) (WorkItemRecord, error) {
	if err := record.validate(); err != nil {
		return WorkItemRecord{}, err
	}
	i := record.Identity
	if i.OwnerGeneration != 1 {
		return WorkItemRecord{}, fmt.Errorf("%w: new work starts at generation 1", domain.ErrInvalidWorkIdentity)
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return WorkItemRecord{}, err
	}
	defer s.writeMu.Unlock()
	var current WorkItemRecord
	err := s.inTx(ctx, "create work item", func(q *gen.Queries) error {
		var err error
		current, err = createWorkItem(ctx, q, record)
		return err
	})
	return current, err
}

// createWorkItem is shared by legacy creation and atomic original-parent issuance.
// The caller owns the writer reservation and transaction.
func createWorkItem(ctx context.Context, q *gen.Queries, record WorkItemRecord) (WorkItemRecord, error) {
	i := record.Identity
	if err := q.CreateWorkItem(ctx, gen.CreateWorkItemParams{
		ID: string(i.ID), AttemptID: string(i.AttemptID), ProjectID: string(i.ProjectID),
		Repository: i.Repository, ExecutionBaseSha: i.ExecutionBaseSHA,
		OriginalParentSessionID: string(i.OriginalParentSessionID),
		OriginalWorkerSessionID: string(i.OriginalWorkerSessionID),
		OriginalNativeSessionID: record.OriginalNativeSessionID,
		OwnerSessionID:          string(i.OwnerSessionID), OwnerGeneration: i.OwnerGeneration,
		WorktreePath: i.WorktreePath, Branch: i.Branch,
	}); err != nil {
		return WorkItemRecord{}, err
	}
	row, err := q.GetWorkItem(ctx, string(i.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return WorkItemRecord{}, domain.ErrDuplicateWorkIdentity
	}
	if err != nil {
		return WorkItemRecord{}, err
	}
	current := workItemFromGen(row)
	return current, record.validateAgainst(current)
}

// GetWorkItem reads the current authoritative record. Missing work is explicit;
// an idle/missing runtime or absent native reference never implies reclamation.
func (s *Store) GetWorkItem(ctx context.Context, id domain.WorkItemID) (WorkItemRecord, bool, error) {
	row, err := s.qr.GetWorkItem(ctx, string(id))
	if errors.Is(err, sql.ErrNoRows) {
		return WorkItemRecord{}, false, nil
	}
	if err != nil {
		return WorkItemRecord{}, false, fmt.Errorf("get work item: %w", err)
	}
	record := workItemFromGen(row)
	if err := record.validate(); err != nil {
		return WorkItemRecord{}, false, err
	}
	return record, true, nil
}

// ClaimWorkItemOwner CASes the complete accepted record to an admitted owner at
// generation+1, then reads back in the same transaction. Only ownership changes;
// a new attempt or changed authorization requires separate admission, not an
// upsert. The caller supplies AO's accepted expectation, never worker assertions.
// Rejected stale/foreign requests return the durable current record plus a typed
// error. A retry cannot advance again. This is ownership bookkeeping, not a
// lease, process-liveness decision, completion or allowed-effects fence.
func (s *Store) ClaimWorkItemOwner(ctx context.Context, expected WorkItemRecord, owner domain.SessionID) (WorkItemRecord, error) {
	if err := expected.validate(); err != nil {
		return WorkItemRecord{}, err
	}
	next := expected
	next.Identity.OwnerSessionID = owner
	if next.Identity.OwnerGeneration == math.MaxInt64 {
		return WorkItemRecord{}, fmt.Errorf("%w: owner generation exhausted", domain.ErrInvalidWorkIdentity)
	}
	next.Identity.OwnerGeneration++
	if err := next.validate(); err != nil {
		return WorkItemRecord{}, err
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return WorkItemRecord{}, err
	}
	defer s.writeMu.Unlock()
	i := expected.Identity
	var current WorkItemRecord
	err := s.inTx(ctx, "claim work item owner", func(q *gen.Queries) error {
		// Write first: competing SQLite connections serialize before readback,
		// avoiding a deferred read transaction's stale snapshot upgrade.
		rows, err := q.ClaimWorkItemOwner(ctx, gen.ClaimWorkItemOwnerParams{
			NextOwner: string(owner), ID: string(i.ID), AttemptID: string(i.AttemptID),
			ProjectID: string(i.ProjectID), Repository: i.Repository,
			ExecutionBaseSha:        i.ExecutionBaseSHA,
			OriginalParentSessionID: string(i.OriginalParentSessionID),
			OriginalWorkerSessionID: string(i.OriginalWorkerSessionID),
			OriginalNativeSessionID: expected.OriginalNativeSessionID,
			OwnerSessionID:          string(i.OwnerSessionID), OwnerGeneration: i.OwnerGeneration,
			WorktreePath: i.WorktreePath, Branch: i.Branch,
		})
		if err != nil {
			return err
		}
		row, err := q.GetWorkItem(ctx, string(i.ID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkItemNotFound
		}
		if err != nil {
			return err
		}
		current = workItemFromGen(row)
		if rows == 0 {
			if err := expected.validateAgainst(current); err != nil {
				return err
			}
			return domain.ErrWorkIdentityMismatch
		}
		return next.validateAgainst(current)
	})
	return current, err
}

func workItemFromGen(row gen.WorkItem) WorkItemRecord {
	return WorkItemRecord{
		Identity: domain.WorkItemIdentity{
			ID: domain.WorkItemID(row.ID), AttemptID: domain.WorkAttemptID(row.AttemptID),
			ProjectID: domain.ProjectID(row.ProjectID), Repository: row.Repository,
			ExecutionBaseSHA:        row.ExecutionBaseSha,
			OriginalParentSessionID: domain.SessionID(row.OriginalParentSessionID),
			OriginalWorkerSessionID: domain.SessionID(row.OriginalWorkerSessionID),
			OwnerSessionID:          domain.SessionID(row.OwnerSessionID), OwnerGeneration: row.OwnerGeneration,
			WorktreePath: row.WorktreePath, Branch: row.Branch,
		},
		OriginalNativeSessionID: row.OriginalNativeSessionID,
	}
}
