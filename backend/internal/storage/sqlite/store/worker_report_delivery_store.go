package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ ports.WorkerReportDeliveryStore = (*Store)(nil)

// HoldWorkerReportDeliveries preserves every source and delivery binding.
// Only an exact singleton resolved by the internal semantic writer is exempt.
// A preexisting mixed batch remains indivisible, including its legacy members.
func (s *Store) HoldWorkerReportDeliveries(ctx context.Context, project domain.ProjectID, at time.Time) (held bool, err error) {
	err = s.WithOrchestrationDispatch(ctx, project, func() error {
		if err := s.writeMu.LockContext(ctx); err != nil {
			return err
		}
		defer s.writeMu.Unlock()
		return s.inTxDB(ctx, "hold worker reports", func(_ *gen.Queries, db gen.DBTX) error {
			rows, err := db.QueryContext(ctx, `SELECT `+orchestrationColumns+`
			 FROM orchestration_events WHERE project_id=? AND kind='worker_report'`, project)
			if err != nil {
				return err
			}
			reports, scanErr := scanOrchestrationEvents(rows)
			if err := errors.Join(scanErr, rows.Close()); err != nil {
				return err
			}
			for _, event := range reports {
				resolved, err := reportSemanticallyResolved(ctx, db, event)
				if err != nil {
					return err
				}
				if resolved {
					// A stale marker is retry evidence for an existing notification.
					// Only successful notification resolution may clear it.
					continue
				}
				held = true
				_, err = db.ExecContext(ctx, `UPDATE orchestration_events
				 SET attention_required_at=COALESCE(attention_required_at,?),last_error=? WHERE id=?`,
					at, ports.WorkerReportDeliveryHoldReason, event.ID)
				if err != nil {
					return err
				}
			}
			result, err := db.ExecContext(ctx, `UPDATE orchestration_events
			 SET attention_required_at=COALESCE(attention_required_at,?),last_error=?
			 WHERE project_id=? AND kind<>'worker_report' AND state IN ('pending','leased','submitted','dead_letter')
			 AND NULLIF(TRIM(lease_token),'') IS NOT NULL AND EXISTS (
			   SELECT 1 FROM orchestration_events r WHERE r.project_id=orchestration_events.project_id
			   AND r.kind='worker_report' AND r.lease_token=orchestration_events.lease_token)`, at, ports.WorkerReportDeliveryHoldReason, project)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			held = held || n > 0
			return err
		})
	})
	return held, err
}

// WorkerReportSemanticallyResolved consumes durable readback, never transport
// admission. The trusted internal semantic writer already owns ACK authority;
// no second ACK ledger or independently asserted authentication is introduced.
func (s *Store) WorkerReportSemanticallyResolved(ctx context.Context, event domain.OrchestrationEvent) (bool, error) {
	return reportSemanticallyResolved(ctx, s.readDB, event)
}

// ClearResolvedWorkerReportAttention is called after notification resolution.
// Recheck the exact singleton and immutable receipt in the clearing transaction;
// generic ACKs, stale snapshots and unresolved reports cannot clear attention.
func (s *Store) ClearResolvedWorkerReportAttention(ctx context.Context, events []domain.OrchestrationEvent) error {
	if err := s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.writeMu.Unlock()
	return s.inTxDB(ctx, "clear resolved worker report attention", func(_ *gen.Queries, db gen.DBTX) error {
		for _, event := range events {
			resolved, err := reportSemanticallyResolved(ctx, db, event)
			if err != nil {
				return err
			}
			if !resolved {
				continue
			}
			if _, err := db.ExecContext(ctx, `UPDATE orchestration_events
			 SET attention_required_at=NULL,last_error='' WHERE id=?`, event.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

func reportSemanticallyResolved(ctx context.Context, db gen.DBTX, event domain.OrchestrationEvent) (bool, error) {
	if event.Kind != domain.OrchestrationWorkerReport || event.State != domain.OrchestrationAcknowledged ||
		event.AcknowledgedAt.IsZero() || event.AttemptCount == 0 {
		return false, nil
	}
	m, err := ports.ParseExactReportMessage(event.LeaseToken)
	if err != nil {
		return false, nil
	}
	w := &exactReportWriter{project: event.ProjectID, reportID: m.ReportID}
	r, current, err := w.read(ctx, db)
	if errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return current.ID == event.ID && current.ProjectID == event.ProjectID && current.WorkerID == event.WorkerID &&
		current.State == domain.OrchestrationAcknowledged && current.AcknowledgedAt.Equal(event.AcknowledgedAt) &&
		current.AttemptCount == event.AttemptCount && current.LeaseToken == event.LeaseToken &&
		current.SourceRevision == event.SourceRevision && current.DestinationSessionID == event.DestinationSessionID &&
		event.DestinationSessionID == m.Parent.SessionID && m.ValidateSource(r, current) == nil, nil
}

var _ ports.ExactReportStore = (*Store)(nil)

// WithExactParentReport is the opt-in singleton lane. It uses the SAME dispatch
// admission gate as legacy delivery and operator holds; it creates no scheduler,
// outbox or report mutation. The callback alone owns native semantic authority.
func (s *Store) WithExactParentReport(ctx context.Context, project domain.ProjectID, reportID string, fn func(ports.ExactReportWriter) error) error {
	return s.WithOrchestrationDispatch(ctx, project, func() error {
		return fn(&exactReportWriter{s: s, project: project, reportID: reportID})
	})
}

type exactReportWriter struct {
	s        *Store
	project  domain.ProjectID
	reportID string
	// Invocation-local receipt of a committed Prepare, never a new authority
	// source. Admission must match this exact outbox lease, even after reclaim.
	preparedAttempt int
	preparedUntil   time.Time
}

func (w *exactReportWriter) read(ctx context.Context, db gen.DBTX) (domain.WorkerReportReceipt, domain.OrchestrationEvent, error) {
	var r domain.WorkerReportReceipt
	var envelope string
	if err := db.QueryRowContext(ctx, `SELECT envelope,event_id,accepted_at_ms FROM worker_reports WHERE id=?`, w.reportID).
		Scan(&envelope, &r.EventID, &r.AcceptedAtMS); err != nil {
		return r, domain.OrchestrationEvent{}, err
	}
	if err := json.Unmarshal([]byte(envelope), &r.Report); err != nil {
		return r, domain.OrchestrationEvent{}, err
	}
	e, err := scanOrchestrationEvent(db.QueryRowContext(ctx, `SELECT `+orchestrationColumns+`
	 FROM orchestration_events WHERE id=? AND worker_report_id=? AND project_id=? AND kind='worker_report'`, r.EventID, w.reportID, w.project))
	if err != nil {
		return r, e, err
	}
	if e.LeaseToken != "" {
		var others int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM orchestration_events WHERE lease_token=? AND id<>?`, e.LeaseToken, e.ID).Scan(&others); err != nil {
			return r, e, err
		}
		if others != 0 {
			return r, e, &ports.ReportDeliveryHold{Reason: "report_mixed_batch"}
		}
	}
	return r, e, nil
}

func (w *exactReportWriter) Read(ctx context.Context) (domain.WorkerReportReceipt, domain.OrchestrationEvent, error) {
	return w.read(ctx, w.s.readDB)
}

func (w *exactReportWriter) write(ctx context.Context, fn func(gen.DBTX) error) error {
	if err := w.s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer w.s.writeMu.Unlock()
	return w.s.inTxDB(ctx, "exact parent report", func(_ *gen.Queries, db gen.DBTX) error { return fn(db) })
}

func (w *exactReportWriter) Prepare(ctx context.Context, m ports.ExactReportMessage, expected domain.OrchestrationEvent, now func() time.Time) error {
	var at time.Time
	err := w.write(ctx, func(db gen.DBTX) error {
		r, e, err := w.read(ctx, db)
		if err != nil {
			return err
		}
		if err := m.ValidateSource(r, e); err != nil {
			return err
		}
		// Historical acceptance is not fresh delivery authority. A superseded
		// work generation or changed/released/expired lease cannot admit a send.
		// This is a transaction-local check, not a native permission grant.
		q := gen.New(db)
		work, err := q.GetWorkItem(ctx, string(r.Report.Identity.ID))
		if err != nil {
			return err
		}
		if err := r.Report.Identity.ValidateAgainst(workItemFromGen(work).Identity); err != nil {
			return err
		}
		lease, err := readWorkLease(ctx, q, r.Report.Identity.ID)
		if err != nil {
			return err
		}
		if lease.OwnerGeneration != r.Report.Identity.OwnerGeneration || lease.Grant.ID != r.Report.LeaseID ||
			lease.Revision != r.Report.LeaseRevision || lease.State != domain.WorkLeaseActive {
			return domain.ErrWorkLeaseConflict
		}
		at = now()
		if !at.Before(lease.ExpiresAt) {
			return domain.ErrWorkLeaseExpired
		}
		if e.ID != expected.ID || e.State != expected.State || e.AttemptCount != expected.AttemptCount ||
			e.LeaseToken != expected.LeaseToken || e.DestinationSessionID != expected.DestinationSessionID ||
			!e.LeaseExpiresAt.Equal(expected.LeaseExpiresAt) || !e.NextAttemptAt.Equal(expected.NextAttemptAt) || at.Before(e.NextAttemptAt) ||
			!e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() || e.AttemptCount >= 8 ||
			(e.State != domain.OrchestrationPending && e.State != domain.OrchestrationLeased) ||
			(e.State == domain.OrchestrationLeased && at.Before(e.LeaseExpiresAt)) {
			return &ports.ReportDeliveryHold{Reason: "report_singleton_cas"}
		}
		if e.LeaseToken == "" {
			if e.DestinationSessionID != "" || e.AttemptCount != 0 || e.State != domain.OrchestrationPending {
				return &ports.ReportDeliveryHold{Reason: "report_singleton_cas"}
			}
		} else if e.LeaseToken != m.Token() || e.DestinationSessionID != m.Parent.SessionID {
			return &ports.ReportDeliveryHold{Reason: "report_singleton_cas"}
		}
		result, err := db.ExecContext(ctx, `UPDATE orchestration_events SET state='leased',
		 lease_token=?,destination_session_id=?,lease_expires_at=?,attempt_count=attempt_count+1,
		 attention_required_at=COALESCE(attention_required_at,?),last_error=?
		 WHERE id=? AND worker_report_id=? AND project_id=? AND kind='worker_report'
		 AND state=? AND attempt_count=? AND COALESCE(lease_token,'')=? AND COALESCE(destination_session_id,'')=?
		 AND submitted_at IS NULL AND acknowledged_at IS NULL
		 AND NOT EXISTS (SELECT 1 FROM orchestration_events other WHERE other.lease_token=? AND other.id<>orchestration_events.id)
		 AND EXISTS (SELECT 1 FROM sessions s WHERE s.id=? AND s.project_id=orchestration_events.project_id
		   AND s.kind='orchestrator' AND s.is_terminated=0 AND s.session_mode='chat')`,
			m.Token(), m.Parent.SessionID, at.Add(orchestrationevents.LeaseDuration), at, ports.WorkerReportDeliveryHoldReason,
			e.ID, w.reportID, w.project, e.State, e.AttemptCount, e.LeaseToken, e.DestinationSessionID, m.Token(), m.Parent.SessionID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err == nil && n != 1 {
			err = &ports.ReportDeliveryHold{Reason: "report_singleton_cas"}
		}
		return err
	})
	if err == nil {
		w.preparedAttempt, w.preparedUntil = expected.AttemptCount+1, at.Add(orchestrationevents.LeaseDuration)
	}
	return err
}

func (w *exactReportWriter) Admit(ctx context.Context, m ports.ExactReportMessage, now func() time.Time) error {
	return w.advance(ctx, m, now, false)
}

func (w *exactReportWriter) Resolve(ctx context.Context, m ports.ExactReportMessage, now func() time.Time) error {
	return w.advance(ctx, m, now, true)
}

func (w *exactReportWriter) advance(ctx context.Context, m ports.ExactReportMessage, now func() time.Time, semantic bool) error {
	return w.write(ctx, func(db gen.DBTX) error {
		r, e, err := w.read(ctx, db)
		if err != nil {
			return err
		}
		if err := m.ValidateSource(r, e); err != nil {
			return err
		}
		if e.LeaseToken != m.Token() || e.DestinationSessionID != m.Parent.SessionID || e.AttemptCount == 0 ||
			(e.State != domain.OrchestrationPending && e.State != domain.OrchestrationLeased && e.State != domain.OrchestrationSubmitted &&
				!(semantic && e.State == domain.OrchestrationDeadLetter)) {
			return &ports.ReportDeliveryHold{Reason: "report_singleton_cas"}
		}
		at := now()
		if !semantic && (e.State != domain.OrchestrationLeased || e.AttemptCount != w.preparedAttempt ||
			!e.LeaseExpiresAt.Equal(w.preparedUntil) || !at.Before(e.LeaseExpiresAt)) {
			return &ports.ReportDeliveryHold{Reason: "report_admission_lease_stale"}
		}
		// An authenticated semantic event can close a lost transport receipt.
		// Retain notification retry evidence atomically with the ACK: only
		// successful notification resolution may clear the attention marker.
		query := `UPDATE orchestration_events SET state='submitted',submitted_at=COALESCE(submitted_at,?)
		 WHERE id=? AND worker_report_id=? AND project_id=? AND lease_token=? AND destination_session_id=?
		 AND state=? AND attempt_count=? AND source_revision=? AND acknowledged_at IS NULL`
		if semantic {
			query = `UPDATE orchestration_events SET state='acknowledged',acknowledged_at=?,
			 lease_expires_at=NULL
			 WHERE id=? AND worker_report_id=? AND project_id=? AND lease_token=? AND destination_session_id=?
			 AND state=? AND attempt_count=? AND source_revision=? AND acknowledged_at IS NULL`
		}
		result, err := db.ExecContext(ctx, query, at, e.ID, w.reportID, w.project, m.Token(), m.Parent.SessionID, e.State, e.AttemptCount, m.SourceRevision)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err == nil && n != 1 {
			err = &ports.ReportDeliveryHold{Reason: "report_singleton_cas"}
		}
		return err
	})
}

func (w *exactReportWriter) Hold(ctx context.Context, at time.Time) error {
	if err := w.s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer w.s.writeMu.Unlock()
	_, err := w.s.writeDB.ExecContext(ctx, `UPDATE orchestration_events
	 SET attention_required_at=COALESCE(attention_required_at,?),last_error=?
	 WHERE project_id=? AND worker_report_id=? AND kind='worker_report'`,
		at, ports.WorkerReportDeliveryHoldReason, w.project, w.reportID)
	return err
}
