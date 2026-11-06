package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var (
	_ ports.WorkItemParentIssuer      = (*Store)(nil)
	_ ports.WorkItemParentReader      = (*Store)(nil)
	_ ports.WorkItemParentPersistence = (*Store)(nil)
)

// IssueWorkItemFromOriginalParent refuses to use caller assertions as origin.
// Use the Chat service's controller-bound issuer instead. Metadata, worker IDs
// and report/HTTP claims cannot establish live controller ownership.
func (s *Store) IssueWorkItemFromOriginalParent(ctx context.Context, identity domain.WorkItemIdentity, workerNativeID string) (domain.WorkItemParentTarget, error) {
	if err := ctx.Err(); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	if err := (WorkItemRecord{Identity: identity, OriginalNativeSessionID: workerNativeID}).validate(); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	return domain.WorkItemParentTarget{}, &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
}

// GetWorkItemOriginalParent cannot promote a legacy AO parent ID or the worker's
// OriginalNativeSessionID to a verified destination. Reads never backfill.
func (s *Store) GetWorkItemOriginalParent(ctx context.Context, id domain.WorkItemID, attempt domain.WorkAttemptID) (domain.WorkItemParentTarget, error) {
	work, found, err := s.GetWorkItem(ctx, id)
	if err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	if !found {
		return domain.WorkItemParentTarget{}, ErrWorkItemNotFound
	}
	if work.Identity.AttemptID != attempt {
		return domain.WorkItemParentTarget{}, domain.ErrWorkIdentityMismatch
	}
	target, err := readWorkItemParent(ctx, s.readDB, id)
	if err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	if err := target.ValidateFor(work.Identity); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	return target, nil
}

func readWorkItemParent(ctx context.Context, db gen.DBTX, id domain.WorkItemID) (domain.WorkItemParentTarget, error) {
	var t domain.WorkItemParentTarget
	err := db.QueryRowContext(ctx, `SELECT work_item_id, attempt_id, original_parent_session_id,
		native_session_id, controller_generation, provider_conversation_id, provider_scope_id,
		issued_by_session_id, issued_by_controller_generation, provenance FROM work_item_parents WHERE work_item_id=?`, id).Scan(
		&t.WorkItemID, &t.AttemptID, &t.OriginalParentSessionID, &t.NativeSessionID, &t.ControllerGeneration,
		&t.ProviderConversationID, &t.ProviderScopeID, &t.IssuedBySessionID, &t.IssuedByControllerGeneration, &t.Provenance)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkItemParentTarget{}, &domain.WorkItemParentHold{Reason: domain.WorkItemParentLegacyMissing}
	}
	return t, err
}

// PersistWorkItemOriginalParent consumes only the internal live-controller
// capability's observation. The caller holds Chat's controller/dispatch gates;
// the writer reservation fences durable ownership across independent Store opens.
// This is not a public ingress and must never be wired to a DTO/report handler.
func (s *Store) PersistWorkItemOriginalParent(ctx context.Context, i domain.WorkItemIdentity, workerNativeID string, target domain.WorkItemParentTarget) (domain.WorkItemParentTarget, error) {
	r := WorkItemRecord{Identity: i, OriginalNativeSessionID: workerNativeID}
	if err := ctx.Err(); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	if err := r.validate(); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	if err := target.ValidateFor(i); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	if i.OriginalParentSessionID == i.OriginalWorkerSessionID {
		return domain.WorkItemParentTarget{}, domain.ErrInvalidWorkItemParent
	}
	if q, ok := ctx.Value(conversationProjectionTxKey{}).(*gen.Queries); ok && q != nil {
		return domain.WorkItemParentTarget{}, &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	defer s.writeMu.Unlock()
	var current domain.WorkItemParentTarget
	err := s.inTxDB(ctx, "issue work item original parent", func(q *gen.Queries, db gen.DBTX) error {
		// Reserve SQLite's writer before reads, even when this ID is not present.
		if err := q.LockWorkItemForLease(ctx, string(i.ID)); err != nil {
			return err
		}
		if row, err := q.GetWorkItem(ctx, string(i.ID)); err == nil {
			if err := r.validateAgainst(workItemFromGen(row)); err != nil {
				return err
			}
			var readErr error
			current, readErr = readWorkItemParent(ctx, db, i.ID)
			if readErr != nil {
				return readErr
			}
			if current != target {
				return domain.ErrInvalidWorkItemParent
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := checkWorkItemParentFence(ctx, q, i, target); err != nil {
			return err
		}
		if current != (domain.WorkItemParentTarget{}) {
			return nil
		}
		if i.OwnerGeneration != 1 || i.OwnerSessionID != i.OriginalWorkerSessionID {
			return domain.ErrInvalidWorkItemParent
		}
		if _, err := createWorkItem(ctx, q, r); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO work_item_parents VALUES(?,?,?,?,?,?,?,?,?,?)`,
			target.WorkItemID, target.AttemptID, target.OriginalParentSessionID, target.NativeSessionID,
			target.ControllerGeneration, target.ProviderConversationID, target.ProviderScopeID,
			target.IssuedBySessionID, target.IssuedByControllerGeneration, target.Provenance); err != nil {
			return err
		}
		var err error
		current, err = readWorkItemParent(ctx, db, i.ID)
		if err == nil && current != target {
			return domain.ErrInvalidWorkItemParent
		}
		return err
	})
	if err != nil {
		return domain.WorkItemParentTarget{}, err
	}
	return current, nil
}

func checkWorkItemParentFence(ctx context.Context, q *gen.Queries, i domain.WorkItemIdentity, t domain.WorkItemParentTarget) error {
	held := &domain.WorkItemParentHold{Reason: domain.WorkItemParentControllerMismatch}
	parent, err := q.GetSession(ctx, i.OriginalParentSessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return held
	}
	if err != nil {
		return err
	}
	if parent.ProjectID == nil || *parent.ProjectID != i.ProjectID || parent.IsTerminated ||
		parent.SessionMode != domain.SessionModeChat || parent.ControllerGeneration != t.ControllerGeneration ||
		parent.ProviderConversationID != t.ProviderConversationID || t.NativeSessionID != t.ProviderConversationID {
		return held
	}
	worker, err := q.GetSession(ctx, i.OriginalWorkerSessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return held
	}
	if err != nil {
		return err
	}
	if worker.ProjectID == nil || *worker.ProjectID != i.ProjectID || worker.IsTerminated || worker.Kind != domain.KindWorker {
		return held
	}
	// AO owns these exact repository/workspace facts; do not re-attest Git.
	for _, id := range []domain.SessionID{i.OriginalParentSessionID, i.OriginalWorkerSessionID} {
		wt, err := q.GetSessionWorktree(ctx, gen.GetSessionWorktreeParams{SessionID: id, RepoName: i.Repository})
		if errors.Is(err, sql.ErrNoRows) {
			return held
		}
		if err != nil {
			return err
		}
		if wt.State != "active" || wt.WorktreePath == "" || wt.Branch == "" || (id == i.OriginalWorkerSessionID &&
			(wt.WorktreePath != i.WorktreePath || wt.Branch != i.Branch || wt.BaseSha != i.ExecutionBaseSHA)) {
			return held
		}
	}
	conv, err := q.SelectConversationBySession(ctx, &i.OriginalParentSessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return held
	}
	if err != nil {
		return err
	}
	branch, err := q.SelectConversationBranch(ctx, gen.SelectConversationBranchParams{BranchID: conv.ActiveBranchID, ConversationID: conv.ID})
	if errors.Is(err, sql.ErrNoRows) {
		return held
	}
	if err != nil {
		return err
	}
	if !branch.SessionID.Valid || branch.SessionID.String != string(i.OriginalParentSessionID) || branch.ProviderConversationID != t.ProviderConversationID || branch.EffectiveProviderScopeID != t.ProviderScopeID {
		return held
	}
	return nil
}
