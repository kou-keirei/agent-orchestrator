package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// The daemon owns one store. These project-scoped admission locks coordinate
// operator writes with transport submission; only SQLite owns the hold state.
// No global writer lock is held across transport I/O.
var dispatchAdmission sync.Map // domain.ProjectID -> chan struct{}

func withDispatchAdmission(ctx context.Context, project domain.ProjectID, fn func() error) error {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	actual, _ := dispatchAdmission.LoadOrStore(project, gate)
	var ok bool
	gate, ok = actual.(chan struct{})
	if !ok {
		return fmt.Errorf("invalid orchestration dispatch admission gate")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
		defer func() { gate <- struct{}{} }()
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn()
	}
}

// OrchestrationDispatchHeld fails closed for missing projects or schema/errors.
// Absence of an override on an existing project means explicitly unheld by
// default, rather than an unknown project being admitted.
func (s *Store) OrchestrationDispatchHeld(ctx context.Context, project domain.ProjectID) (bool, error) {
	var held int
	err := s.readDB.QueryRowContext(ctx, `SELECT CASE WHEN h.project_id IS NULL THEN 0 ELSE h.held END FROM projects p LEFT JOIN project_dispatch_hold h ON h.project_id=p.id WHERE p.id=?`, project).Scan(&held)
	if err != nil {
		return true, err
	}
	if held != 0 && held != 1 {
		return true, fmt.Errorf("unknown dispatch hold state")
	}
	return held == 1, nil
}

// WithOrchestrationDispatch is the final fence before transport admission.
func (s *Store) WithOrchestrationDispatch(ctx context.Context, project domain.ProjectID, fn func() error) error {
	return withDispatchAdmission(ctx, project, func() error {
		held, err := s.OrchestrationDispatchHeld(ctx, project)
		if err != nil {
			return err
		}
		if held {
			return orchestrationevents.ErrDispatchHeld
		}
		return fn()
	})
}

// SetOrchestrationDispatchHold never modifies events or resumes a session.
// A successful hold waits for any already-admitted local submission to return.
func (s *Store) SetOrchestrationDispatchHold(ctx context.Context, project domain.ProjectID, held bool) error {
	return withDispatchAdmission(ctx, project, func() error {
		if err := s.writeMu.LockContext(ctx); err != nil {
			return err
		}
		defer s.writeMu.Unlock()
		result, err := s.writeDB.ExecContext(ctx, `INSERT INTO project_dispatch_hold(project_id,held) SELECT id,? FROM projects WHERE id=? ON CONFLICT(project_id) DO UPDATE SET held=excluded.held`, held, project)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("unknown dispatch hold project %q", project)
		}
		return nil
	})
}

// InitializeOrchestrationDispatchHold runs before startup recovery for an
// explicitly selected project. It initializes an existing project's missing
// override once; an operator's prior hold or release always survives restart.
func (s *Store) InitializeOrchestrationDispatchHold(ctx context.Context, project domain.ProjectID) error {
	return withDispatchAdmission(ctx, project, func() error {
		if err := s.writeMu.LockContext(ctx); err != nil {
			return err
		}
		defer s.writeMu.Unlock()
		return s.inTxDB(ctx, "initialize orchestration dispatch hold", func(_ *gen.Queries, db gen.DBTX) error {
			result, err := db.ExecContext(ctx, `INSERT INTO project_dispatch_hold(project_id,held) SELECT id,1 FROM projects WHERE id=? ON CONFLICT(project_id) DO NOTHING`, project)
			if err != nil {
				return err
			}
			inserted, err := result.RowsAffected()
			if err != nil {
				return err
			}
			var held int
			err = db.QueryRowContext(ctx, `SELECT held FROM project_dispatch_hold WHERE project_id=?`, project).Scan(&held)
			if errors.Is(err, sql.ErrNoRows) && inserted == 0 {
				var exists bool
				if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE id=?)`, project).Scan(&exists); err != nil {
					return err
				}
				if exists {
					return fmt.Errorf("dispatch hold missing after initialization for existing project %q", project)
				}
				// Only a genuinely unregistered project is a safe no-op.
				return nil
			}
			if err != nil {
				return err
			}
			if (held != 0 && held != 1) || (inserted != 0 && held != 1) {
				return fmt.Errorf("dispatch hold initialization readback failed for %q", project)
			}
			return nil
		})
	})
}

// ReadOrchestrationDispatchHold reports outstanding deliveries conservatively:
// an old submitted/leased batch is not evidence of provider quiescence.
func (s *Store) ReadOrchestrationDispatchHold(ctx context.Context, project domain.ProjectID) (held bool, outstanding int, err error) {
	err = withDispatchAdmission(ctx, project, func() error {
		var readErr error
		held, readErr = s.OrchestrationDispatchHeld(ctx, project)
		if readErr != nil {
			return readErr
		}
		return s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM orchestration_events WHERE project_id=? AND state!='acknowledged' AND (state IN ('leased','submitted') OR submitted_at IS NOT NULL OR COALESCE(lease_token,'')!='')`, project).Scan(&outstanding)
	})
	return
}

const orchestrationColumns = `id, project_id, worker_id, kind, source_revision, state,
 attempt_count, enqueued_at, next_attempt_at, lease_token, lease_expires_at,
 destination_session_id, submitted_at, acknowledged_at, attention_required_at, last_error`

// ReconcileTerminatedOrchestrationEvents deterministically closes the crash
// window between any terminal session mutation and its normalized event.
func (s *Store) ReconcileTerminatedOrchestrationEvents(ctx context.Context, now time.Time) (int, error) {
	sessions, err := s.ListAllSessions(ctx)
	if err != nil {
		return 0, err
	}
	created := 0
	for _, rec := range sessions {
		if rec.Kind != domain.KindWorker || !rec.IsTerminated {
			continue
		}
		if rec.IsStandalone() {
			continue
		}
		source := strings.TrimSpace(rec.Metadata.RuntimeLaunchID)
		if source == "" {
			source = strings.TrimSpace(rec.Metadata.ControllerGeneration)
		}
		if source == "" {
			if rec.TerminalRevision == -1 && rec.CleanupGeneration > 0 {
				// Migration 153 cannot identify a restored legacy episode.
				// Leave its terminal event unknown without blocking startup;
				// later restore/termination transitions get a durable identity.
				continue
			}
			if rec.TerminalRevision < 0 {
				return created, fmt.Errorf("terminal revision unavailable for session %q", rec.ID)
			}
			source = "terminal-revision:" + strconv.FormatInt(rec.TerminalRevision, 10)
		}
		inserted, err := s.RecordOrchestrationSourceState(ctx, rec.ProjectID, rec.ID, domain.OrchestrationWorkerTerminated, source, true, now)
		if err != nil {
			return created, err
		}
		if inserted {
			created++
		}
	}
	// PR facts are persisted before lifecycle reacts to them. Re-synthesize the
	// source-state edges from durable PR rows so a crash between those writes
	// cannot permanently lose a merged or ready-to-merge event.
	for _, rec := range sessions {
		if rec.Kind != domain.KindWorker || rec.IsStandalone() {
			continue
		}
		prs, err := s.ListPRsBySession(ctx, rec.ID)
		if err != nil {
			return created, err
		}
		for _, pr := range prs {
			comments, err := s.ListPRComments(ctx, pr.URL)
			if err != nil {
				return created, err
			}
			ready := !pr.Merged && !pr.Closed && domain.MergeReadiness{
				Draft: pr.Draft, Merged: pr.Merged, Closed: pr.Closed,
				CI: pr.CI, Review: pr.Review, Mergeability: pr.Mergeability,
				UnresolvedComments: hasUnresolvedPRComments(comments),
			}.ReadyToMerge()
			inserted, err := s.RecordOrchestrationSourceState(ctx, rec.ProjectID, rec.ID, domain.OrchestrationWorkerReadyMerge, pr.URL, ready, now)
			if err != nil {
				return created, err
			}
			if inserted {
				created++
			}
			if !pr.Merged {
				continue
			}
			inserted, err = s.RecordOrchestrationSourceState(ctx, rec.ProjectID, rec.ID, domain.OrchestrationPRMerged, pr.URL, true, now)
			if err != nil {
				return created, err
			}
			if inserted {
				created++
			}
		}
	}
	return created, nil
}

func hasUnresolvedPRComments(comments []domain.PullRequestComment) bool {
	for _, comment := range comments {
		if !comment.Resolved && !comment.AutoInjectReview {
			return true
		}
	}
	return false
}

// EnqueueOrchestrationEvent inserts an event unless its source revision already exists.
func (s *Store) EnqueueOrchestrationEvent(ctx context.Context, e domain.OrchestrationEvent) (bool, error) {
	if e.ProjectID == "" {
		return false, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `INSERT INTO orchestration_events
 (id,project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at)
 VALUES (?,?,?,?,?,?,?) ON CONFLICT(project_id,worker_id,kind,source_revision) DO NOTHING`,
		e.ID, e.ProjectID, e.WorkerID, e.Kind, e.SourceRevision, e.EnqueuedAt, e.NextAttemptAt)
	if err != nil {
		return false, fmt.Errorf("enqueue orchestration event: %w", err)
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

// RecordOrchestrationSourceState records source transitions and enqueues newly active generations.
func (s *Store) RecordOrchestrationSourceState(ctx context.Context, project domain.ProjectID, worker domain.SessionID, kind domain.OrchestrationEventKind, sourceID string, active bool, now time.Time) (bool, error) {
	if project == "" {
		return false, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	created := false
	err := s.inTxDB(ctx, "record orchestration source state", func(_ *gen.Queries, db gen.DBTX) error {
		var previous int
		var generation int
		err := db.QueryRowContext(ctx, `SELECT active,generation FROM orchestration_source_states WHERE project_id=? AND worker_id=? AND kind=? AND source_id=?`, project, worker, kind, sourceID).Scan(&previous, &generation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if errors.Is(err, sql.ErrNoRows) {
			if active {
				generation = 1
				created = true
			}
			_, err = db.ExecContext(ctx, `INSERT INTO orchestration_source_states(project_id,worker_id,kind,source_id,active,generation,updated_at)VALUES(?,?,?,?,?,?,?)`, project, worker, kind, sourceID, active, generation, now)
			if err != nil {
				return err
			}
		} else {
			if (previous != 0) == active {
				return nil
			}
			if active {
				generation++
				created = true
			}
			_, err = db.ExecContext(ctx, `UPDATE orchestration_source_states SET active=?,generation=?,updated_at=? WHERE project_id=? AND worker_id=? AND kind=? AND source_id=?`, active, generation, now, project, worker, kind, sourceID)
			if err != nil {
				return err
			}
		}
		if !created {
			return nil
		}
		sourceHash := sha256.Sum256([]byte(sourceID))
		revision := hex.EncodeToString(sourceHash[:8]) + ":" + strconv.Itoa(generation)
		sum := sha256.Sum256([]byte(string(project) + "\x00" + string(worker) + "\x00" + string(kind) + "\x00" + sourceID + "\x00" + revision))
		id := "oe:" + hex.EncodeToString(sum[:16])
		_, err = db.ExecContext(ctx, `INSERT INTO orchestration_events(id,project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at)VALUES(?,?,?,?,?,?,?) ON CONFLICT(project_id,worker_id,kind,source_revision) DO NOTHING`, id, project, worker, kind, revision, now, now)
		return err
	})
	return created, err
}

// ReclaimOrchestrationEventLeases retries or dead-letters events whose delivery leases expired.
func (s *Store) ReclaimOrchestrationEventLeases(ctx context.Context, now time.Time) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `UPDATE orchestration_events SET
		state=CASE
			WHEN (SELECT session_mode FROM sessions WHERE id=destination_session_id)='chat'
				THEN CASE WHEN attempt_count+1>=8 OR enqueued_at<=? THEN 'dead_letter' ELSE 'pending' END
			WHEN (SELECT session_mode FROM sessions WHERE id=destination_session_id)='tui'
				THEN CASE WHEN attempt_count+1>=2 OR enqueued_at<=? THEN 'dead_letter' ELSE 'pending' END
			ELSE 'dead_letter'
		END,
		attempt_count=MIN(attempt_count+1,8),lease_expires_at=NULL,
		attention_required_at=CASE
			WHEN COALESCE((SELECT session_mode FROM sessions WHERE id=destination_session_id),'') NOT IN ('chat','tui')
				OR ((SELECT session_mode FROM sessions WHERE id=destination_session_id)='chat' AND (attempt_count+1>=8 OR enqueued_at<=?))
				OR ((SELECT session_mode FROM sessions WHERE id=destination_session_id)='tui' AND (attempt_count+1>=2 OR enqueued_at<=?))
				THEN COALESCE(attention_required_at,?)
			ELSE attention_required_at
		END,
		next_attempt_at=?
		 WHERE state IN ('leased','submitted') AND lease_expires_at<=?
		 AND NOT EXISTS (SELECT 1 FROM project_dispatch_hold h WHERE h.project_id=orchestration_events.project_id AND h.held IS NOT 0)`, now.Add(-15*time.Minute), now.Add(-15*time.Minute), now.Add(-15*time.Minute), now.Add(-15*time.Minute), now, now, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// MarkProjectNoDestinationAttention marks overdue pending events as requiring attention.
func (s *Store) MarkProjectNoDestinationAttention(ctx context.Context, project domain.ProjectID, now time.Time) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `UPDATE orchestration_events SET attention_required_at=? WHERE project_id=? AND state='pending' AND attention_required_at IS NULL AND enqueued_at<=? AND NOT EXISTS (SELECT 1 FROM project_dispatch_hold h WHERE h.project_id=orchestration_events.project_id AND h.held IS NOT 0)`, now, project, now.Add(-15*time.Minute))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// MarkOrchestrationRetentionOverflow dead-letters pending events exceeding age or count limits.
func (s *Store) MarkOrchestrationRetentionOverflow(ctx context.Context, now time.Time) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `UPDATE orchestration_events SET state='dead_letter',attention_required_at=COALESCE(attention_required_at,?),last_error='retention limit exceeded' WHERE state='pending' AND NOT EXISTS (SELECT 1 FROM project_dispatch_hold h WHERE h.project_id=orchestration_events.project_id AND h.held IS NOT 0) AND (enqueued_at<=? OR id IN (SELECT id FROM orchestration_events AS overflow WHERE overflow.project_id=orchestration_events.project_id AND overflow.state='pending' ORDER BY overflow.enqueued_at DESC,overflow.id DESC LIMIT -1 OFFSET 10000))`, now, now.Add(-30*24*time.Hour))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ListDueOrchestrationEvents loads due events while preserving reclaimed batch membership.
func (s *Store) ListDueOrchestrationEvents(ctx context.Context, project domain.ProjectID, now time.Time, limit int) (events []domain.OrchestrationEvent, err error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.readDB.QueryContext(ctx, `WITH first_due AS (
		SELECT COALESCE(NULLIF(TRIM(lease_token), ''), '') AS lease_token
		FROM orchestration_events
		WHERE project_id=? AND state='pending' AND next_attempt_at<=?
		AND kind<>'worker_report' AND NOT EXISTS (
		  SELECT 1 FROM orchestration_events r WHERE r.project_id=orchestration_events.project_id
		  AND r.kind='worker_report' AND r.lease_token=orchestration_events.lease_token)
		ORDER BY enqueued_at,id LIMIT 1
	)
	SELECT `+orchestrationColumns+` FROM orchestration_events AS event
	 WHERE event.project_id=? AND event.kind<>'worker_report'
	   AND COALESCE(NULLIF(TRIM(event.lease_token), ''), '')=(SELECT lease_token FROM first_due)
	   AND ((SELECT lease_token FROM first_due)<>'' OR (event.state='pending' AND event.next_attempt_at<=?))
	 ORDER BY event.enqueued_at,event.id LIMIT ?`, project, now, project, now, limit+1)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	events, err = scanOrchestrationEvents(rows)
	if err != nil || len(events) == 0 {
		return events, err
	}
	if strings.TrimSpace(events[0].LeaseToken) != "" {
		// Parent-era retries may have different due times. Once any member is
		// due, recover the entire bounded payload under its original token.
		if len(events) > limit {
			return nil, fmt.Errorf("reclaimed orchestration batch exceeds event limit %d", limit)
		}
		for _, event := range events {
			if event.State != domain.OrchestrationPending {
				// Reclaim can split a token across pending/dead_letter at the
				// per-event age limit, even with its destination retained. Return
				// complete membership for the dispatcher's HOLD; never replay a
				// subset or admit an acknowledged or in-flight member.
				if event.State == domain.OrchestrationDeadLetter {
					continue
				}
				return nil, fmt.Errorf("reclaimed orchestration batch contains non-pending event %q", event.ID)
			}
		}
	} else if len(events) > limit {
		events = events[:limit]
	}
	return events, nil
}

// ListOrchestrationEvents lists recent events for a project.
func (s *Store) ListOrchestrationEvents(ctx context.Context, project domain.ProjectID, limit int) (events []domain.OrchestrationEvent, err error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT `+orchestrationColumns+` FROM orchestration_events WHERE project_id=? ORDER BY enqueued_at DESC,id DESC LIMIT ?`, project, limit)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return scanOrchestrationEvents(rows)
}

// ListOrchestrationEventsRequiringAttention returns the durable source for
// reconstructing human-visible alerts after daemon or publisher failure.
func (s *Store) ListOrchestrationEventsRequiringAttention(ctx context.Context, project domain.ProjectID) (events []domain.OrchestrationEvent, err error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT `+orchestrationColumns+` FROM orchestration_events
	 WHERE project_id=? AND attention_required_at IS NOT NULL ORDER BY enqueued_at,id`, project)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return scanOrchestrationEvents(rows)
}

// ClearOrchestrationAttention clears attention markers for the specified events.
func (s *Store) ClearOrchestrationAttention(ctx context.Context, ids []string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.inTxDB(ctx, "clear orchestration attention", func(_ *gen.Queries, db gen.DBTX) error {
		for _, id := range ids {
			if _, err := db.ExecContext(ctx, `UPDATE orchestration_events SET attention_required_at=NULL WHERE id=? AND kind<>'worker_report'`, id); err != nil {
				return err
			}
		}
		return nil
	})
}

// RetryDeadLetterOrchestrationEvent retries a project-scoped dead-letter event and its batch.
func (s *Store) RetryDeadLetterOrchestrationEvent(ctx context.Context, project domain.ProjectID, id string, now time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `UPDATE orchestration_events SET state='pending',attempt_count=0,next_attempt_at=?,lease_expires_at=NULL,submitted_at=NULL,acknowledged_at=NULL,attention_required_at=NULL,last_error=''
		WHERE project_id=? AND state='dead_letter' AND kind<>'worker_report'
		AND NOT EXISTS (SELECT 1 FROM orchestration_events r WHERE r.project_id=orchestration_events.project_id
		 AND r.kind='worker_report' AND (r.id=? OR r.lease_token=orchestration_events.lease_token)) AND (
			id=? OR (lease_token IS NOT NULL AND lease_token=(SELECT lease_token FROM orchestration_events WHERE id=? AND project_id=? AND state='dead_letter'))
		)`, now, project, id, id, id, project)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

// LeaseOrchestrationEvents assigns pending events to a destination under a lease token.
func (s *Store) LeaseOrchestrationEvents(ctx context.Context, ids []string, token string, destination domain.SessionID, expires time.Time) error {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(token) != token || strings.TrimSpace(string(destination)) == "" {
		return fmt.Errorf("missing orchestration batch token or destination")
	}
	return s.updateOrchestrationBatch(ctx, "lease orchestration events", ids, token, func(db gen.DBTX, id string) (sql.Result, error) {
		var held int
		if err := db.QueryRowContext(ctx, `SELECT CASE WHEN h.project_id IS NULL THEN 0 ELSE h.held END FROM orchestration_events e JOIN sessions s ON s.id=? AND s.project_id=e.project_id AND s.kind='orchestrator' LEFT JOIN project_dispatch_hold h ON h.project_id=e.project_id WHERE e.id=?`, destination, id).Scan(&held); err != nil {
			return nil, err
		}
		if held != 0 {
			return nil, orchestrationevents.ErrDispatchHeld
		}
		// The transaction fences stale due reads as well as direct callers. Only
		// untouched work may acquire a binding; retries may only renew their pair.
		return db.ExecContext(ctx, `UPDATE orchestration_events SET state='leased',lease_token=?,lease_expires_at=?,destination_session_id=?
		 WHERE id=? AND state='pending' AND (
			(lease_token IS NULL AND destination_session_id IS NULL AND attempt_count=0 AND submitted_at IS NULL)
			OR (lease_token=? AND destination_session_id=?)
		 ) AND EXISTS (SELECT 1 FROM sessions WHERE id=? AND project_id=orchestration_events.project_id AND kind='orchestrator' AND is_terminated=0)`, token, expires, destination, id, token, destination, destination)
	})
}

// MarkOrchestrationEventsSubmitted records submission for events matching the lease token.
func (s *Store) MarkOrchestrationEventsSubmitted(ctx context.Context, ids []string, token string, at time.Time) error {
	return s.updateOrchestrationBatch(ctx, "submit orchestration events", ids, token, func(db gen.DBTX, id string) (sql.Result, error) {
		return db.ExecContext(ctx, `UPDATE orchestration_events SET state=CASE WHEN state='leased' THEN 'submitted' ELSE state END,submitted_at=COALESCE(submitted_at,?) WHERE id=? AND state IN ('leased','acknowledged') AND lease_token=?`, at, id, token)
	})
}

// AcknowledgeOrchestrationEvents records legacy transport admission only.
// updateOrchestrationBatch rejects reports: transport is not semantic ACK.
func (s *Store) AcknowledgeOrchestrationEvents(ctx context.Context, ids []string, token string, at time.Time) error {
	return s.updateOrchestrationBatch(ctx, "acknowledge orchestration events", ids, token, func(db gen.DBTX, id string) (sql.Result, error) {
		return db.ExecContext(ctx, `UPDATE orchestration_events SET state='acknowledged',acknowledged_at=?,lease_expires_at=NULL,last_error='' WHERE id=? AND state IN ('leased','submitted') AND lease_token=?`, at, id, token)
	})
}

// AcknowledgeOrchestrationBatchAccepted closes a TUI batch only when the
// lifecycle hook reports admission of the exact AO batch id at its destination.
func (s *Store) AcknowledgeOrchestrationBatchAccepted(ctx context.Context, destination domain.SessionID, token string, at time.Time) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `UPDATE orchestration_events SET state='acknowledged',acknowledged_at=?,lease_expires_at=NULL,last_error=''
 WHERE destination_session_id=? AND lease_token=? AND state IN ('leased','submitted')
 AND project_id=(SELECT project_id FROM sessions WHERE id=? AND kind='orchestrator')
	 AND NOT EXISTS (SELECT 1 FROM orchestration_events AS member WHERE member.lease_token=?
	   AND member.project_id=(SELECT project_id FROM sessions WHERE id=?)
	   AND (member.kind='worker_report' OR member.destination_session_id IS NOT ? OR member.state NOT IN ('leased','submitted')))`, at, destination, token, destination, token, destination, destination)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// RetryOrchestrationEvents schedules retries or dead-letters a failed delivery batch.
func (s *Store) RetryOrchestrationEvents(ctx context.Context, events []domain.OrchestrationEvent, token, message string, now time.Time, mode domain.SessionMode) error {
	if len(message) > 512 {
		message = message[:512]
	}
	maxAttempts := 0
	switch mode {
	case domain.SessionModeChat:
		maxAttempts = 8
	case domain.SessionModeTUI:
		maxAttempts = 2
	default:
		return fmt.Errorf("unknown orchestration destination mode %q", mode)
	}
	byID := map[string]domain.OrchestrationEvent{}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
		byID[e.ID] = e
	}
	// The dispatcher submits one immutable batch. A per-event jitter would make
	// only part of that batch due after a reclaim, allowing the same provider
	// idempotency key to name a different payload on retry.
	batchJitter := orchestrationJitter(token, time.Second/4)
	return s.updateOrchestrationBatch(ctx, "retry orchestration events", ids, token, func(db gen.DBTX, id string) (sql.Result, error) {
		e := byID[id]
		attempt := e.AttemptCount + 1
		state := domain.OrchestrationPending
		if attempt >= maxAttempts || now.Sub(e.EnqueuedAt) >= 15*time.Minute {
			state = domain.OrchestrationDeadLetter
		}
		backoff := time.Second << min(attempt-1, 5)
		if backoff > 48*time.Second {
			backoff = 48 * time.Second
		}
		backoff += batchJitter
		var attention any
		if state == domain.OrchestrationDeadLetter {
			attention = now
		}
		return db.ExecContext(ctx, `UPDATE orchestration_events SET state=?,attempt_count=attempt_count+1,next_attempt_at=?,lease_expires_at=NULL,last_error=?,attention_required_at=COALESCE(attention_required_at,?) WHERE id=? AND state IN ('leased','submitted') AND lease_token=? AND attempt_count<?`, state, now.Add(backoff), message, attention, id, token, maxAttempts)
	})
}

func orchestrationJitter(id string, ceiling time.Duration) time.Duration {
	if ceiling <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	jitter := h.Sum64() % uint64(ceiling)
	if jitter > math.MaxInt64 {
		return 0
	}
	return time.Duration(jitter)
}

func (s *Store) updateOrchestrationBatch(ctx context.Context, what string, ids []string, token string, update func(gen.DBTX, string) (sql.Result, error)) error {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(token) != token {
		return fmt.Errorf("missing orchestration batch token")
	}
	encoded, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("encode orchestration batch members: %w", err)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.inTxDB(ctx, what, func(_ *gen.Queries, db gen.DBTX) error {
		// Direct calls derive the project from native event rows, never from a
		// token that can also occur in an unrelated project's history.
		var project string
		var supplied, projects int
		err := db.QueryRowContext(ctx, `SELECT COALESCE(MIN(project_id),''), COUNT(*), COUNT(DISTINCT project_id)
		 FROM orchestration_events WHERE id IN (SELECT value FROM json_each(?))`, string(encoded)).Scan(&project, &supplied, &projects)
		if err != nil {
			return err
		}
		if project == "" || projects != 1 || supplied != len(ids) {
			return fmt.Errorf("orchestration batch %q has invalid project membership", token)
		}
		// Fence direct callers and stale due reads before any row changes. Also
		// reject a legacy-only subset of a previously mixed report batch.
		var reports int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orchestration_events
		 WHERE project_id=? AND kind='worker_report'
		 AND (id IN (SELECT value FROM json_each(?)) OR lease_token=?)`, project, string(encoded), token).Scan(&reports); err != nil {
			return err
		}
		if reports != 0 {
			return ports.ErrWorkerReportDeliveryHeld
		}
		// Within that project a token names one immutable payload and destination.
		// Check its entire membership before the first row update so omitted,
		// substituted, duplicate, or newly appended members cannot change it.
		// A new token has no recorded members and may bind selected fresh work.
		var members, included, destinations, owners int
		var destination string
		err = db.QueryRowContext(ctx, `SELECT COUNT(*),
		 COUNT(CASE WHEN member.id IN (SELECT value FROM json_each(?)) THEN 1 END),
		 COUNT(DISTINCT COALESCE(member.destination_session_id,'')), COALESCE(MIN(member.destination_session_id),''),
		 COUNT(CASE WHEN target.project_id=member.project_id AND target.kind='orchestrator' THEN 1 END)
		 FROM orchestration_events AS member LEFT JOIN sessions AS target ON target.id=member.destination_session_id
		 WHERE member.lease_token=? AND member.project_id=?`, string(encoded), token, project).Scan(&members, &included, &destinations, &destination, &owners)
		if err != nil {
			return err
		}
		if members > 0 && (members != len(ids) || included != members || destinations != 1 || destination == "" || owners != members) {
			return fmt.Errorf("orchestration batch %q failed complete membership fence", token)
		}
		for _, id := range ids {
			result, err := update(db, id)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows != 1 {
				return fmt.Errorf("event %s failed state fence", id)
			}
		}
		return nil
	})
}

type rowScanner interface{ Scan(...any) error }

func scanOrchestrationEvent(row rowScanner) (domain.OrchestrationEvent, error) {
	var e domain.OrchestrationEvent
	var project, worker, kind, state string
	var lease, destination, lastError sql.NullString
	var leaseExpiry, submitted, ack, attention sql.NullTime
	err := row.Scan(&e.ID, &project, &worker, &kind, &e.SourceRevision, &state, &e.AttemptCount, &e.EnqueuedAt, &e.NextAttemptAt, &lease, &leaseExpiry, &destination, &submitted, &ack, &attention, &lastError)
	e.ProjectID = domain.ProjectID(project)
	e.WorkerID = domain.SessionID(worker)
	e.Kind = domain.OrchestrationEventKind(kind)
	e.State = domain.OrchestrationDeliveryState(state)
	e.LeaseToken = lease.String
	e.LeaseExpiresAt = leaseExpiry.Time
	e.DestinationSessionID = domain.SessionID(destination.String)
	e.SubmittedAt = submitted.Time
	e.AcknowledgedAt = ack.Time
	e.AttentionRequiredAt = attention.Time
	e.LastError = strings.TrimSpace(lastError.String)
	return e, err
}
func scanOrchestrationEvents(rows *sql.Rows) ([]domain.OrchestrationEvent, error) {
	var out []domain.OrchestrationEvent
	for rows.Next() {
		e, err := scanOrchestrationEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
