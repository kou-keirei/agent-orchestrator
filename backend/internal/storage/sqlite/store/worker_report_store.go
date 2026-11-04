package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ ports.WorkerReportStore = (*Store)(nil)

// GetWorkerReport reads the immutable acceptance receipt, independently of the
// mutable delivery row. Cleanup of a worker does not erase accepted reports.
func (s *Store) GetWorkerReport(ctx context.Context, id string) (domain.WorkerReportReceipt, bool, error) {
	row, err := s.qr.GetWorkerReport(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkerReportReceipt{}, false, nil
	}
	if err != nil {
		return domain.WorkerReportReceipt{}, false, err
	}
	receipt, err := workerReportReceipt(row)
	return receipt, err == nil, err
}

func workerReportReceipt(row gen.WorkerReport) (domain.WorkerReportReceipt, error) {
	receipt := domain.WorkerReportReceipt{EventID: row.EventID, AcceptedAtMS: row.AcceptedAtMs}
	err := json.Unmarshal([]byte(row.Envelope), &receipt.Report)
	return receipt, err
}

// AcceptWorkerReport reserves the durable writer before any source read. The
// report and its single existing-outbox event commit or roll back together.
// No lease transition, runtime observation, delivery or parent ACK occurs here.
func (s *Store) AcceptWorkerReport(ctx context.Context, report domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	if err := report.Validate(); err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	envelope, err := json.Marshal(report)
	if err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	// Bound a single authored artifact; references do not cause artifact reads.
	if len(envelope) > 1<<20 {
		return domain.WorkerReportReceipt{}, domain.ErrInvalidWorkerReport
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	defer s.writeMu.Unlock()
	var receipt domain.WorkerReportReceipt
	err = s.inTx(ctx, "accept worker report", func(q *gen.Queries) error {
		if err := q.LockWorkItemForLease(ctx, string(report.Identity.ID)); err != nil {
			return err
		}
		prior, err := q.GetWorkerReport(ctx, report.ID)
		if err == nil {
			if prior.Envelope != string(envelope) {
				return domain.ErrWorkerReportConflict
			}
			// Historical readback only: never re-enqueue, renew or authorize.
			receipt, err = workerReportReceipt(prior)
			return err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		row, err := q.GetWorkItem(ctx, string(report.Identity.ID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkItemNotFound
		}
		if err != nil {
			return err
		}
		current := workItemFromGen(row)
		if err := report.Identity.ValidateAgainst(current.Identity); err != nil {
			return err
		}
		if report.OriginalNativeSessionID != current.OriginalNativeSessionID {
			return domain.ErrWorkIdentityMismatch
		}
		lease, err := readWorkLease(ctx, q, report.Identity.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ErrWorkLeaseConflict
		}
		if err != nil {
			return err
		}
		if lease.OwnerGeneration != report.Identity.OwnerGeneration || lease.Grant.ID != report.LeaseID || lease.Revision != report.LeaseRevision {
			return domain.ErrWorkLeaseConflict
		}
		if err := liveWorkLease(lease); err != nil {
			return err
		}
		worker, err := q.GetSession(ctx, report.WorkerSessionID)
		if err != nil {
			return err
		}
		if worker.ProjectID == nil || *worker.ProjectID != report.Identity.ProjectID || worker.Kind != domain.KindWorker {
			return domain.ErrWorkIdentityMismatch
		}
		if (worker.AgentSessionID != "" && worker.AgentSessionID != report.NativeSessionID) ||
			(report.WorkerSessionID == current.Identity.OriginalWorkerSessionID && current.OriginalNativeSessionID != "" && report.NativeSessionID != current.OriginalNativeSessionID) {
			return domain.ErrWorkIdentityMismatch
		}
		if report.Type == "done" {
			count, err := q.HasWorkerDoneReport(ctx, gen.HasWorkerDoneReportParams{WorkItemID: string(report.Identity.ID), AttemptID: string(report.Identity.AttemptID)})
			if err != nil {
				return err
			}
			if count != 0 {
				return domain.ErrWorkerReportConflict
			}
		}
		sum := sha256.Sum256(envelope)
		source := "worker-report:v1:" + hex.EncodeToString(sum[:])
		eventID := "oe:" + source
		now := time.Now().UTC()
		if !now.Before(lease.ExpiresAt) {
			return domain.ErrWorkLeaseExpired
		}
		if err := q.InsertWorkerReport(ctx, gen.InsertWorkerReportParams{
			ID: report.ID, WorkItemID: string(report.Identity.ID), AttemptID: string(report.Identity.AttemptID),
			OwnerGeneration: report.Identity.OwnerGeneration, ReportType: report.Type, Envelope: string(envelope),
			EventID: eventID, SourceRevision: source, AcceptedAtMs: now.UnixMilli(),
		}); err != nil {
			return err
		}
		if err := q.InsertWorkerReportEvent(ctx, gen.InsertWorkerReportEventParams{
			ID: eventID, ProjectID: string(report.Identity.ProjectID), WorkerID: string(report.WorkerSessionID),
			SourceRevision: source, EnqueuedAt: now, NextAttemptAt: now,
			WorkerReportID: sql.NullString{String: report.ID, Valid: true},
		}); err != nil {
			return err
		}
		stored, err := q.GetWorkerReport(ctx, report.ID)
		if err != nil {
			return err
		}
		receipt, err = workerReportReceipt(stored)
		return err
	})
	if err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	return receipt, nil
}
