package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// UpsertReview inserts the per-worker, per-harness review row, or reuses the
// existing one by refreshing its handle/native-session state.
func (s *Store) UpsertReview(ctx context.Context, r domain.Review) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return upsertReviewQuery(ctx, s.qw, r)
}

func upsertReviewQuery(ctx context.Context, q *gen.Queries, r domain.Review) error {
	return q.UpsertReview(ctx, gen.UpsertReviewParams{
		ID:                    r.ID,
		SessionID:             r.SessionID,
		ProjectID:             r.ProjectID,
		Harness:               r.Harness,
		PRURL:                 r.PRURL,
		ReviewerHandleID:      r.ReviewerHandleID,
		AgentSessionID:        r.AgentSessionID,
		ReviewerActivityState: string(r.ReviewerActivityState),
		ReviewerLaunchID:      r.ReviewerLaunchID,
		CreatedAt:             r.CreatedAt,
		UpdatedAt:             r.UpdatedAt,
	})
}

// AcceptReviewerReplacement commits a new reviewer generation and the exact
// running predecessors it replaces and the retired-host cleanup obligation as
// one durable transition. Every snapshot
// is checked inside the same transaction so a concurrent hook or completion
// cannot turn a replacement into a partial authority change.
func (s *Store) AcceptReviewerReplacement(ctx context.Context, incumbent, replacement domain.Review, predecessors []domain.ReviewRun, clearAgentSession bool, retiredCleanup domain.ReviewerCleanupObligation) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if strings.TrimSpace(incumbent.ID) == "" || replacement.ID != incumbent.ID || replacement.SessionID != incumbent.SessionID || replacement.ProjectID != incumbent.ProjectID || replacement.Harness != incumbent.Harness {
		return errors.New("reviewer replacement requires one existing review row")
	}
	retiresHandle := incumbent.ReviewerHandleID != "" && incumbent.ReviewerHandleID != replacement.ReviewerHandleID
	if retiresHandle {
		if retiredCleanup.ID == "" || retiredCleanup.SessionID != incumbent.SessionID || retiredCleanup.ReviewID != incumbent.ID || retiredCleanup.Harness != incumbent.Harness || retiredCleanup.HandleID != incumbent.ReviewerHandleID || retiredCleanup.LaunchID != incumbent.ReviewerLaunchID || retiredCleanup.AgentSessionID != incumbent.AgentSessionID || retiredCleanup.Status != domain.ReviewerCleanupUnresolved {
			return errors.New("reviewer replacement requires the exact retired-host cleanup identity")
		}
	} else if retiredCleanup.ID != "" {
		return errors.New("reviewer replacement has no retired handle for cleanup")
	}
	return s.inTxDB(ctx, "accept reviewer replacement", func(q *gen.Queries, db gen.DBTX) error {
		row, err := q.GetReviewByID(ctx, incumbent.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("incumbent review %q disappeared", incumbent.ID)
		}
		if err != nil {
			return err
		}
		if current := reviewFromReview(row); !sameReviewerAuthority(current, incumbent) {
			return fmt.Errorf("incumbent review %q changed before replacement acceptance", incumbent.ID)
		}
		for _, predecessor := range predecessors {
			row, err := q.GetReviewRun(ctx, predecessor.ID)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("predecessor review run %q disappeared", predecessor.ID)
			}
			if err != nil {
				return err
			}
			current := reviewRunFromRow(row)
			if !sameReviewerPredecessor(current, predecessor) {
				return fmt.Errorf("predecessor review run %q changed before replacement acceptance", predecessor.ID)
			}
		}
		if retiresHandle {
			if db == nil {
				return errors.New("reviewer replacement cleanup requires a database transaction")
			}
			if err := insertReviewerCleanupObligation(ctx, db, retiredCleanup); err != nil {
				return err
			}
			// An idempotent insert can encounter an existing id. Acceptance
			// requires that row to retain this exact unresolved cleanup identity.
			var matched int
			if err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM reviewer_cleanup_obligation
WHERE id = ? AND session_id = ? AND review_id = ? AND run_id = ?
  AND harness = ? AND handle_id = ? AND launch_id = ?
  AND agent_session_id = ? AND status = 'unresolved'`,
				retiredCleanup.ID, retiredCleanup.SessionID, retiredCleanup.ReviewID, retiredCleanup.RunID,
				retiredCleanup.Harness, retiredCleanup.HandleID, retiredCleanup.LaunchID, retiredCleanup.AgentSessionID).Scan(&matched); err != nil {
				return fmt.Errorf("verify retired reviewer cleanup obligation %q: %w", retiredCleanup.ID, err)
			}
			if matched != 1 {
				return fmt.Errorf("retired reviewer cleanup obligation %q conflicts with its durable identity or status", retiredCleanup.ID)
			}
		}
		if clearAgentSession {
			updated, err := q.UpdateReviewAgentSessionID(ctx, gen.UpdateReviewAgentSessionIDParams{AgentSessionID: "", ID: replacement.ID})
			if err != nil {
				return err
			}
			if updated != 1 {
				return fmt.Errorf("review row %q was not updated while clearing reviewer agent session", replacement.ID)
			}
		}
		if err := upsertReviewQuery(ctx, q, replacement); err != nil {
			return err
		}
		for _, predecessor := range predecessors {
			updated, err := q.UpdateReviewRunResult(ctx, gen.UpdateReviewRunResultParams{
				Status:           domain.ReviewRunCancelled,
				Verdict:          domain.VerdictNone,
				Body:             "replaced by a launch-bound reviewer generation",
				GithubReviewID:   "",
				AutoInjectReview: predecessor.AutoInjectReview,
				ID:               predecessor.ID,
			})
			if err != nil {
				return err
			}
			if updated != 1 {
				return fmt.Errorf("predecessor review run %q was not running during replacement acceptance", predecessor.ID)
			}
		}
		return nil
	})
}

func sameReviewerAuthority(got, want domain.Review) bool {
	return got.ID == want.ID &&
		got.SessionID == want.SessionID &&
		got.ProjectID == want.ProjectID &&
		got.Harness == want.Harness &&
		got.PRURL == want.PRURL &&
		got.ReviewerHandleID == want.ReviewerHandleID &&
		got.AgentSessionID == want.AgentSessionID &&
		got.ReviewerActivityState == want.ReviewerActivityState &&
		got.ReviewerLaunchID == want.ReviewerLaunchID &&
		got.CreatedAt.Equal(want.CreatedAt)
}

func sameReviewerPredecessor(got, want domain.ReviewRun) bool {
	return got.ID == want.ID &&
		got.ReviewID == want.ReviewID &&
		got.SessionID == want.SessionID &&
		got.BatchID == want.BatchID &&
		got.Harness == want.Harness &&
		got.TriggerSource == want.TriggerSource &&
		got.PRURL == want.PRURL &&
		got.TargetSHA == want.TargetSHA &&
		got.TargetBinding == want.TargetBinding &&
		got.Provenance == want.Provenance &&
		got.LaunchID == want.LaunchID &&
		got.Status == domain.ReviewRunRunning &&
		want.Status == domain.ReviewRunRunning &&
		got.CreatedAt.Equal(want.CreatedAt) &&
		got.AutoInjectReview == want.AutoInjectReview
}

// GetReviewBySession returns the latest review row for a worker session,
// ok=false if none.
func (s *Store) GetReviewBySession(ctx context.Context, id domain.SessionID) (domain.Review, bool, error) {
	row, err := s.qr.GetReviewBySession(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Review{}, false, nil
	}
	if err != nil {
		return domain.Review{}, false, fmt.Errorf("get review by session %s: %w", id, err)
	}
	return reviewFromGetReviewBySessionRow(row), true, nil
}

// GetReviewBySessionAndHarness returns the review row for one reviewer harness
// on a worker session, ok=false if none.
func (s *Store) GetReviewBySessionAndHarness(ctx context.Context, id domain.SessionID, harness domain.ReviewerHarness) (domain.Review, bool, error) {
	row, err := s.qr.GetReviewBySessionAndHarness(ctx, gen.GetReviewBySessionAndHarnessParams{SessionID: id, Harness: harness})
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Review{}, false, nil
	}
	if err != nil {
		return domain.Review{}, false, fmt.Errorf("get review by session %s harness %s: %w", id, harness, err)
	}
	return reviewFromGetReviewBySessionAndHarnessRow(row), true, nil
}

// GetReviewByID returns one reviewer session row by its stable review id.
func (s *Store) GetReviewByID(ctx context.Context, id string) (domain.Review, bool, error) {
	row, err := s.qr.GetReviewByID(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Review{}, false, nil
	}
	if err != nil {
		return domain.Review{}, false, fmt.Errorf("get review %s: %w", id, err)
	}
	return reviewFromReview(row), true, nil
}

// ListReviewsBySession returns every harness-specific review row for a worker
// session, newest first.
func (s *Store) ListReviewsBySession(ctx context.Context, id domain.SessionID) ([]domain.Review, error) {
	rows, err := s.qr.ListReviewsBySession(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list reviews for session %s: %w", id, err)
	}
	out := make([]domain.Review, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewFromListReviewsBySessionRow(row))
	}
	return out, nil
}

// InsertReviewerCleanupObligation durably records a possible reviewer runtime
// handle without creating or changing reviewer authority. Repeating the same
// obligation id is idempotent so crash recovery can safely retry the write.
func (s *Store) InsertReviewerCleanupObligation(ctx context.Context, obligation domain.ReviewerCleanupObligation) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return insertReviewerCleanupObligation(ctx, s.writeDB, obligation)
}

func insertReviewerCleanupObligation(ctx context.Context, db gen.DBTX, obligation domain.ReviewerCleanupObligation) error {
	if strings.TrimSpace(obligation.ID) == "" || strings.TrimSpace(string(obligation.SessionID)) == "" || strings.TrimSpace(obligation.HandleID) == "" {
		return errors.New("reviewer cleanup obligation requires id, session, and handle")
	}
	if obligation.Status == "" {
		obligation.Status = domain.ReviewerCleanupUnresolved
	}
	if obligation.Status != domain.ReviewerCleanupUnresolved {
		return fmt.Errorf("reviewer cleanup obligation %q has invalid status %q", obligation.ID, obligation.Status)
	}
	if obligation.CreatedAt.IsZero() {
		obligation.CreatedAt = time.Now().UTC()
	}
	if obligation.UpdatedAt.IsZero() {
		obligation.UpdatedAt = obligation.CreatedAt
	}
	_, err := db.ExecContext(ctx, `
INSERT INTO reviewer_cleanup_obligation
    (id, session_id, review_id, run_id, harness, handle_id, launch_id,
     agent_session_id, status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO NOTHING`,
		obligation.ID, obligation.SessionID, obligation.ReviewID, obligation.RunID,
		obligation.Harness, obligation.HandleID, obligation.LaunchID,
		obligation.AgentSessionID, obligation.Status, obligation.CreatedAt,
		obligation.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert reviewer cleanup obligation %q: %w", obligation.ID, err)
	}
	return nil
}

// ListUnresolvedReviewerCleanupObligations returns cleanup-only records for a
// session. No live reviewer authority is synthesized from these rows.
func (s *Store) ListUnresolvedReviewerCleanupObligations(ctx context.Context, sessionID domain.SessionID) ([]domain.ReviewerCleanupObligation, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT id, session_id, review_id, run_id, harness, handle_id, launch_id,
       agent_session_id, status, created_at, updated_at, resolved_at
FROM reviewer_cleanup_obligation
WHERE session_id = ? AND status = 'unresolved'
ORDER BY created_at ASC, id ASC`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list unresolved reviewer cleanup obligations for %s: %w", sessionID, err)
	}
	defer rows.Close()
	var out []domain.ReviewerCleanupObligation
	for rows.Next() {
		var obligation domain.ReviewerCleanupObligation
		var createdAt, updatedAt time.Time
		var resolvedAt sql.NullTime
		if err := rows.Scan(
			&obligation.ID, &obligation.SessionID, &obligation.ReviewID,
			&obligation.RunID, &obligation.Harness, &obligation.HandleID,
			&obligation.LaunchID, &obligation.AgentSessionID, &obligation.Status,
			&createdAt, &updatedAt, &resolvedAt); err != nil {
			return nil, fmt.Errorf("scan reviewer cleanup obligation: %w", err)
		}
		obligation.CreatedAt = createdAt
		obligation.UpdatedAt = updatedAt
		if resolvedAt.Valid {
			resolved := resolvedAt.Time
			obligation.ResolvedAt = &resolved
		}
		out = append(out, obligation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reviewer cleanup obligations: %w", err)
	}
	return out, nil
}

// ResolveReviewerCleanupObligation marks one exact cleanup identity cleared.
// Repeating the operation is idempotent and returns ok=false once it is no
// longer unresolved.
func (s *Store) ResolveReviewerCleanupObligation(ctx context.Context, id string, resolvedAt time.Time) (bool, error) {
	if strings.TrimSpace(id) == "" {
		return false, errors.New("reviewer cleanup obligation id is required")
	}
	if resolvedAt.IsZero() {
		resolvedAt = time.Now().UTC()
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `
UPDATE reviewer_cleanup_obligation
SET status = 'cleared', updated_at = ?, resolved_at = ?
WHERE id = ? AND status = 'unresolved'`, resolvedAt, resolvedAt, id)
	if err != nil {
		return false, fmt.Errorf("resolve reviewer cleanup obligation %q: %w", id, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("resolve reviewer cleanup obligation %q rows affected: %w", id, err)
	}
	return n == 1, nil
}

// ResolveReviewRunCleanupMarker clears one exact cleanup marker after its
// runtime handle has been destroyed. The body predicate makes the operation
// idempotent and prevents a retry from rewriting a later ReviewRun result.
func (s *Store) ResolveReviewRunCleanupMarker(ctx context.Context, id, marker, body string) (bool, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(marker) == "" {
		return false, errors.New("review run cleanup marker requires id and marker")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `
UPDATE review_run
SET status = 'failed', verdict = '', body = ?
WHERE id = ? AND instr(body, ?) > 0 AND status IN ('running', 'failed')`, body, id, marker)
	if err != nil {
		return false, fmt.Errorf("resolve review run cleanup marker %q: %w", id, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("resolve review run cleanup marker %q rows affected: %w", id, err)
	}
	return n == 1, nil
}

// ClearReviewerHandle removes all persisted terminal handles for a worker
// after a hard reviewer pane teardown.
func (s *Store) ClearReviewerHandle(ctx context.Context, id domain.SessionID) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.ClearReviewerHandle(ctx, id)
}

// ClearReviewerHandleByHarness removes only the runtime handle for one
// reviewer harness, preserving its native agent session id for later restore.
func (s *Store) ClearReviewerHandleByHarness(ctx context.Context, id domain.SessionID, harness domain.ReviewerHarness) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.ClearReviewerHandleByHarness(ctx, gen.ClearReviewerHandleByHarnessParams{SessionID: id, Harness: harness})
}

// UpdateReviewAgentSessionID records engine-owned reviewer native session
// metadata without applying the reviewer-hook launch generation fence.
func (s *Store) UpdateReviewAgentSessionID(ctx context.Context, id, agentSessionID string) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.qw.UpdateReviewAgentSessionID(ctx, gen.UpdateReviewAgentSessionIDParams{
		AgentSessionID: agentSessionID,
		ID:             id,
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// UpdateReviewActivity records the native reviewer conversation id and/or
// reviewer activity reported by the reviewer harness hooks.
func (s *Store) UpdateReviewActivity(ctx context.Context, id string, state domain.ActivityState, agentSessionID, launchID string) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.qw.UpdateReviewActivity(ctx, gen.UpdateReviewActivityParams{
		Column1:               agentSessionID,
		AgentSessionID:        agentSessionID,
		Column3:               state,
		ReviewerActivityState: string(state),
		Column6:               launchID,
		ReviewerLaunchID:      launchID,
		Column8:               launchID,
		ID:                    id,
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// AppendReviewRunNativeEvidence records facts observed directly from the
// provider hook. The launch and session predicates make this append
// idempotent and reject stale or superseded hooks.
func (s *Store) AppendReviewRunNativeEvidence(ctx context.Context, id, launchID, runSessionID, nativeSessionID, firstTurnID, completionID string, observedAt time.Time) (bool, error) {
	if strings.TrimSpace(nativeSessionID) == "" && strings.TrimSpace(firstTurnID) == "" && strings.TrimSpace(completionID) == "" {
		return false, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var updated int64
	err := s.inTx(ctx, "append review run native evidence", func(q *gen.Queries) error {
		run, err := q.GetReviewRun(ctx, id)
		if err != nil {
			return err
		}
		if run.SessionID != domain.SessionID(runSessionID) || run.LaunchID != launchID || run.Status != domain.ReviewRunRunning {
			return nil
		}
		if (nativeSessionID != "" && run.ProviderNativeSessionID != "" && run.ProviderNativeSessionID != nativeSessionID) ||
			(firstTurnID != "" && run.ProviderNativeFirstTurnID != "" && run.ProviderNativeFirstTurnID != firstTurnID) ||
			(completionID != "" && run.ProviderNativeCompletionID != "" && run.ProviderNativeCompletionID != completionID) {
			return fmt.Errorf("native evidence contradicts stored review run %q", id)
		}
		reviewRow, err := q.GetReviewByID(ctx, run.ReviewID)
		if err != nil {
			return err
		}
		current := reviewFromReview(reviewRow)
		if current.SessionID != domain.SessionID(runSessionID) || current.ReviewerLaunchID != launchID ||
			(nativeSessionID != "" && current.AgentSessionID != "" && current.AgentSessionID != nativeSessionID) {
			return nil
		}
		if nativeSessionID != "" {
			n, err := q.UpdateReviewActivity(ctx, gen.UpdateReviewActivityParams{
				Column1:               nativeSessionID,
				AgentSessionID:        nativeSessionID,
				Column3:               "",
				ReviewerActivityState: "",
				ID:                    run.ReviewID,
				Column6:               launchID,
				ReviewerLaunchID:      launchID,
				Column8:               launchID,
			})
			if err != nil {
				return err
			}
			if n == 0 {
				return fmt.Errorf("native evidence review claim was rejected for review %q", run.ReviewID)
			}
		}
		updated, err = q.AppendReviewRunNativeEvidence(ctx, gen.AppendReviewRunNativeEvidenceParams{
			NativeSessionID: nativeSessionID,
			FirstTurnID:     firstTurnID,
			CompletionID:    completionID,
			ObservedAt:      sql.NullTime{Time: observedAt, Valid: !observedAt.IsZero()},
			ID:              id,
			LaunchID:        launchID,
			SessionID:       domain.SessionID(runSessionID),
		})
		if err != nil {
			return err
		}
		if updated == 0 {
			return fmt.Errorf("native evidence append was rejected for review run %q", id)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated > 0, nil
}

// FindReviewRunForNativeEvidence resolves a follow-up provider hook by the
// immutable first-turn identity recorded on the same launch generation. More
// than one match is ambiguous and therefore fails closed.
func (s *Store) FindReviewRunForNativeEvidence(ctx context.Context, reviewID string, sessionID domain.SessionID, launchID, nativeSessionID, turnID string) (domain.ReviewRun, bool, error) {
	runs, err := s.ListReviewRunsBySession(ctx, sessionID)
	if err != nil {
		return domain.ReviewRun{}, false, err
	}
	var match *domain.ReviewRun
	for i := range runs {
		run := runs[i]
		if run.ReviewID != reviewID || run.LaunchID != launchID || run.Status != domain.ReviewRunRunning || run.Provenance.ProviderNative.FirstTurnID != turnID {
			continue
		}
		if nativeSessionID != "" && run.Provenance.ProviderNative.SessionID != "" && run.Provenance.ProviderNative.SessionID != nativeSessionID {
			continue
		}
		if match != nil {
			return domain.ReviewRun{}, false, fmt.Errorf("ambiguous native evidence for review %s launch %s turn %s", reviewID, launchID, turnID)
		}
		candidate := run
		match = &candidate
	}
	if match == nil {
		return domain.ReviewRun{}, false, nil
	}
	return *match, true, nil
}

// InsertReviewRun records a new review pass. A unique-constraint hit on the
// (session_id, pr_url, target_sha) index (migration 0020) is surfaced as the sentinel
// domain.ErrDuplicateReviewRun so the engine can fall back to the existing run.
func (s *Store) InsertReviewRun(ctx context.Context, r domain.ReviewRun) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if r.TriggerSource == "" {
		r.TriggerSource = domain.ReviewTriggerManual
	}
	err := s.qw.InsertReviewRun(ctx, gen.InsertReviewRunParams{
		ID:                     r.ID,
		ReviewID:               r.ReviewID,
		SessionID:              r.SessionID,
		BatchID:                r.BatchID,
		Harness:                r.Harness,
		TriggerSource:          r.TriggerSource,
		PRURL:                  r.PRURL,
		TargetSha:              r.TargetSHA,
		TargetBindingJson:      marshalReviewEvidence(r.TargetBinding),
		ReviewerProvenanceJson: marshalReviewEvidence(r.Provenance),
		LaunchID:               r.LaunchID,
		Status:                 r.Status,
		Verdict:                r.Verdict,
		Body:                   r.Body,
		GithubReviewID:         r.GithubReviewID,
		CreatedAt:              r.CreatedAt,
		AutoInjectReview:       r.AutoInjectReview,
	})
	if isSQLiteUnique(err) {
		return fmt.Errorf("insert review run for session %s pr %s sha %s: %w", r.SessionID, r.PRURL, r.TargetSHA, domain.ErrDuplicateReviewRun)
	}
	return err
}

// UpdateReviewRunResult sets the status/verdict/body and the GitHub review id of
// a running review pass.
func (s *Store) UpdateReviewRunResult(ctx context.Context, id string, status domain.ReviewRunStatus, verdict domain.ReviewVerdict, body, githubReviewID string, autoInjectReview bool) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.qw.UpdateReviewRunResult(ctx, gen.UpdateReviewRunResultParams{
		Status:           status,
		Verdict:          verdict,
		Body:             body,
		GithubReviewID:   githubReviewID,
		AutoInjectReview: autoInjectReview,
		ID:               id,
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// SupersedeStaleRunningReviewRuns marks older running unverdicted passes for a
// worker failed before starting a review for a newer commit.
func (s *Store) SupersedeStaleRunningReviewRuns(ctx context.Context, sessionID domain.SessionID, prURL, targetSHA, body string) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.SupersedeStaleRunningReviewRuns(ctx, gen.SupersedeStaleRunningReviewRunsParams{
		Body:      body,
		SessionID: sessionID,
		PRURL:     prURL,
		TargetSha: targetSHA,
	})
}

// CancelRunningReviewRunsBySession marks all currently running review passes
// for a worker cancelled.
func (s *Store) CancelRunningReviewRunsBySession(ctx context.Context, sessionID domain.SessionID, body string) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.CancelRunningReviewRunsBySession(ctx, gen.CancelRunningReviewRunsBySessionParams{
		Body:      body,
		SessionID: sessionID,
	})
}

// CancelRunningReviewRunsBySessionAndHarness marks currently running review
// passes for a single reviewer harness as cancelled.
func (s *Store) CancelRunningReviewRunsBySessionAndHarness(ctx context.Context, sessionID domain.SessionID, harness domain.ReviewerHarness, body string) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.CancelRunningReviewRunsBySessionAndHarness(ctx, gen.CancelRunningReviewRunsBySessionAndHarnessParams{
		Body:      body,
		SessionID: sessionID,
		Harness:   harness,
	})
}

// MarkReviewRunDelivered records that lifecycle delivered the worker nudge for
// a completed AO-internal review pass.
func (s *Store) MarkReviewRunDelivered(ctx context.Context, id string, deliveredAt time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.qw.MarkReviewRunDelivered(ctx, gen.MarkReviewRunDeliveredParams{
		DeliveredAt: sql.NullTime{Time: deliveredAt, Valid: true},
		ID:          id,
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// GetReviewRun returns one review pass by id.
func (s *Store) GetReviewRun(ctx context.Context, id string) (domain.ReviewRun, bool, error) {
	row, err := s.qr.GetReviewRun(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ReviewRun{}, false, nil
	}
	if err != nil {
		return domain.ReviewRun{}, false, fmt.Errorf("get review run %s: %w", id, err)
	}
	return reviewRunFromRow(row), true, nil
}

// GetReviewRunBySessionPRAndSHA returns the most recent review pass for a
// worker session, PR, and commit, ok=false if none. It lets a repeat trigger for
// the same PR head short-circuit to the existing run without colliding with
// another PR that happens to share the same head commit.
func (s *Store) GetReviewRunBySessionPRAndSHA(ctx context.Context, id domain.SessionID, prURL, targetSHA string) (domain.ReviewRun, bool, error) {
	row, err := s.qr.GetReviewRunBySessionPRAndSHA(ctx, gen.GetReviewRunBySessionPRAndSHAParams{SessionID: id, PRURL: prURL, TargetSha: targetSHA})
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ReviewRun{}, false, nil
	}
	if err != nil {
		return domain.ReviewRun{}, false, fmt.Errorf("get review run for session %s pr %s sha %s: %w", id, prURL, targetSHA, err)
	}
	return reviewRunFromRow(row), true, nil
}

// GetReviewRunBySessionPRSHAAndHarness returns the most recent review pass for
// a worker session, PR, commit, and reviewer harness, ok=false if none.
func (s *Store) GetReviewRunBySessionPRSHAAndHarness(ctx context.Context, id domain.SessionID, prURL, targetSHA string, harness domain.ReviewerHarness) (domain.ReviewRun, bool, error) {
	row, err := s.qr.GetReviewRunBySessionPRSHAAndHarness(ctx, gen.GetReviewRunBySessionPRSHAAndHarnessParams{SessionID: id, PRURL: prURL, TargetSha: targetSHA, Harness: harness})
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ReviewRun{}, false, nil
	}
	if err != nil {
		return domain.ReviewRun{}, false, fmt.Errorf("get review run for session %s pr %s sha %s harness %s: %w", id, prURL, targetSHA, harness, err)
	}
	return reviewRunFromRow(row), true, nil
}

// ListReviewRunsBySession returns all review passes for a worker session, newest first.
func (s *Store) ListReviewRunsBySession(ctx context.Context, id domain.SessionID) ([]domain.ReviewRun, error) {
	rows, err := s.qr.ListReviewRunsBySession(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list review runs for session %s: %w", id, err)
	}
	out := make([]domain.ReviewRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewRunFromRow(row))
	}
	return out, nil
}

// ListRunningReviewRunsBySession returns only currently running unverdicted
// review passes for a worker session, newest first.
func (s *Store) ListRunningReviewRunsBySession(ctx context.Context, id domain.SessionID) ([]domain.ReviewRun, error) {
	rows, err := s.qr.ListRunningReviewRunsBySession(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list running review runs for session %s: %w", id, err)
	}
	out := make([]domain.ReviewRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewRunFromRow(row))
	}
	return out, nil
}

// ListReviewRunsByBatch returns all passes in one trigger-created batch, oldest first.
func (s *Store) ListReviewRunsByBatch(ctx context.Context, id domain.SessionID, batchID string) ([]domain.ReviewRun, error) {
	rows, err := s.qr.ListReviewRunsByBatch(ctx, gen.ListReviewRunsByBatchParams{SessionID: id, BatchID: batchID})
	if err != nil {
		return nil, fmt.Errorf("list review runs for session %s batch %s: %w", id, batchID, err)
	}
	out := make([]domain.ReviewRun, 0, len(rows))
	for _, row := range rows {
		out = append(out, reviewRunFromRow(row))
	}
	return out, nil
}

func reviewFromGetReviewBySessionRow(r gen.GetReviewBySessionRow) domain.Review {
	return domain.Review{
		ID:                    r.ID,
		SessionID:             r.SessionID,
		ProjectID:             r.ProjectID,
		Harness:               r.Harness,
		PRURL:                 r.PRURL,
		ReviewerHandleID:      r.ReviewerHandleID,
		AgentSessionID:        r.AgentSessionID,
		ReviewerActivityState: domain.ActivityState(r.ReviewerActivityState),
		ReviewerLaunchID:      r.ReviewerLaunchID,
		CreatedAt:             r.CreatedAt,
		UpdatedAt:             r.UpdatedAt,
	}
}

func reviewFromGetReviewBySessionAndHarnessRow(r gen.GetReviewBySessionAndHarnessRow) domain.Review {
	return domain.Review{
		ID:                    r.ID,
		SessionID:             r.SessionID,
		ProjectID:             r.ProjectID,
		Harness:               r.Harness,
		PRURL:                 r.PRURL,
		ReviewerHandleID:      r.ReviewerHandleID,
		AgentSessionID:        r.AgentSessionID,
		ReviewerActivityState: domain.ActivityState(r.ReviewerActivityState),
		ReviewerLaunchID:      r.ReviewerLaunchID,
		CreatedAt:             r.CreatedAt,
		UpdatedAt:             r.UpdatedAt,
	}
}

func reviewFromListReviewsBySessionRow(r gen.ListReviewsBySessionRow) domain.Review {
	return domain.Review{
		ID:                    r.ID,
		SessionID:             r.SessionID,
		ProjectID:             r.ProjectID,
		Harness:               r.Harness,
		PRURL:                 r.PRURL,
		ReviewerHandleID:      r.ReviewerHandleID,
		AgentSessionID:        r.AgentSessionID,
		ReviewerActivityState: domain.ActivityState(r.ReviewerActivityState),
		ReviewerLaunchID:      r.ReviewerLaunchID,
		CreatedAt:             r.CreatedAt,
		UpdatedAt:             r.UpdatedAt,
	}
}

func reviewFromReview(r gen.GetReviewByIDRow) domain.Review {
	return domain.Review{
		ID:                    r.ID,
		SessionID:             r.SessionID,
		ProjectID:             r.ProjectID,
		Harness:               r.Harness,
		PRURL:                 r.PRURL,
		ReviewerHandleID:      r.ReviewerHandleID,
		AgentSessionID:        r.AgentSessionID,
		ReviewerActivityState: domain.ActivityState(r.ReviewerActivityState),
		ReviewerLaunchID:      r.ReviewerLaunchID,
		CreatedAt:             r.CreatedAt,
		UpdatedAt:             r.UpdatedAt,
	}
}

type reviewRunRow interface{}

func reviewRunFromRow(r reviewRunRow) domain.ReviewRun {
	switch v := r.(type) {
	case gen.GetReviewRunRow:
		return reviewRunFromValues(v.ID, v.ReviewID, v.SessionID, v.BatchID, v.Harness, v.TriggerSource, v.PRURL, v.TargetSha, v.TargetBindingJson, v.ReviewerProvenanceJson, v.LaunchID, v.Status, v.Verdict, v.Body, v.GithubReviewID, v.CreatedAt, v.DeliveredAt, v.AutoInjectReview, v.ProviderNativeSessionID, v.ProviderNativeFirstTurnID, v.ProviderNativeCompletionID, v.ProviderNativeObservedAt)
	case gen.GetReviewRunBySessionPRAndSHARow:
		return reviewRunFromValues(v.ID, v.ReviewID, v.SessionID, v.BatchID, v.Harness, v.TriggerSource, v.PRURL, v.TargetSha, v.TargetBindingJson, v.ReviewerProvenanceJson, v.LaunchID, v.Status, v.Verdict, v.Body, v.GithubReviewID, v.CreatedAt, v.DeliveredAt, v.AutoInjectReview, v.ProviderNativeSessionID, v.ProviderNativeFirstTurnID, v.ProviderNativeCompletionID, v.ProviderNativeObservedAt)
	case gen.GetReviewRunBySessionPRSHAAndHarnessRow:
		return reviewRunFromValues(v.ID, v.ReviewID, v.SessionID, v.BatchID, v.Harness, v.TriggerSource, v.PRURL, v.TargetSha, v.TargetBindingJson, v.ReviewerProvenanceJson, v.LaunchID, v.Status, v.Verdict, v.Body, v.GithubReviewID, v.CreatedAt, v.DeliveredAt, v.AutoInjectReview, v.ProviderNativeSessionID, v.ProviderNativeFirstTurnID, v.ProviderNativeCompletionID, v.ProviderNativeObservedAt)
	case gen.ListReviewRunsByBatchRow:
		return reviewRunFromValues(v.ID, v.ReviewID, v.SessionID, v.BatchID, v.Harness, v.TriggerSource, v.PRURL, v.TargetSha, v.TargetBindingJson, v.ReviewerProvenanceJson, v.LaunchID, v.Status, v.Verdict, v.Body, v.GithubReviewID, v.CreatedAt, v.DeliveredAt, v.AutoInjectReview, v.ProviderNativeSessionID, v.ProviderNativeFirstTurnID, v.ProviderNativeCompletionID, v.ProviderNativeObservedAt)
	case gen.ListReviewRunsBySessionRow:
		return reviewRunFromValues(v.ID, v.ReviewID, v.SessionID, v.BatchID, v.Harness, v.TriggerSource, v.PRURL, v.TargetSha, v.TargetBindingJson, v.ReviewerProvenanceJson, v.LaunchID, v.Status, v.Verdict, v.Body, v.GithubReviewID, v.CreatedAt, v.DeliveredAt, v.AutoInjectReview, v.ProviderNativeSessionID, v.ProviderNativeFirstTurnID, v.ProviderNativeCompletionID, v.ProviderNativeObservedAt)
	case gen.ListRunningReviewRunsBySessionRow:
		return reviewRunFromValues(v.ID, v.ReviewID, v.SessionID, v.BatchID, v.Harness, v.TriggerSource, v.PRURL, v.TargetSha, v.TargetBindingJson, v.ReviewerProvenanceJson, v.LaunchID, v.Status, v.Verdict, v.Body, v.GithubReviewID, v.CreatedAt, v.DeliveredAt, v.AutoInjectReview, v.ProviderNativeSessionID, v.ProviderNativeFirstTurnID, v.ProviderNativeCompletionID, v.ProviderNativeObservedAt)
	default:
		panic(fmt.Sprintf("unsupported review run row %T", r))
	}
}

func marshalReviewEvidence(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func unmarshalReviewEvidence(raw string, dst any) {
	if raw == "" || raw == "{}" {
		return
	}
	_ = json.Unmarshal([]byte(raw), dst)
}

func reviewRunFromValues(id, reviewID string, sessionID domain.SessionID, batchID string, harness domain.ReviewerHarness, triggerSource domain.ReviewTriggerSource, prURL, targetSHA, targetBindingJSON, provenanceJSON, launchID string, status domain.ReviewRunStatus, verdict domain.ReviewVerdict, body, githubReviewID string, createdAt time.Time, deliveredAt sql.NullTime, autoInjectReview bool, nativeSessionID, nativeFirstTurnID, nativeCompletionID string, nativeObservedAt sql.NullTime) domain.ReviewRun {
	var delivered *time.Time
	if deliveredAt.Valid {
		t := deliveredAt.Time
		delivered = &t
	}
	var targetBinding domain.ReviewTargetBinding
	var provenance domain.ReviewerProvenance
	unmarshalReviewEvidence(targetBindingJSON, &targetBinding)
	unmarshalReviewEvidence(provenanceJSON, &provenance)
	provenance.ProviderNative.SessionID = nativeSessionID
	provenance.ProviderNative.FirstTurnID = nativeFirstTurnID
	provenance.ProviderNative.CompletionID = nativeCompletionID
	if nativeObservedAt.Valid {
		provenance.ProviderNative.ObservedAt = nativeObservedAt.Time
	}
	return domain.ReviewRun{
		ID: id, ReviewID: reviewID, SessionID: sessionID, BatchID: batchID,
		Harness: harness, TriggerSource: triggerSource, PRURL: prURL, TargetSHA: targetSHA,
		TargetBinding: targetBinding, Provenance: provenance, LaunchID: launchID,
		Status: status, Verdict: verdict, Body: body, GithubReviewID: githubReviewID,
		CreatedAt: createdAt, DeliveredAt: delivered, AutoInjectReview: autoInjectReview,
	}
}

// ListCurrentHeadReviewRunsForSession returns AO's review passes against the
// current head commit of each PR the session owns. Passes recorded for an
// earlier head are filtered out in SQL, so callers cannot let a stale run
// decide a derived column.
func (s *Store) ListCurrentHeadReviewRunsForSession(ctx context.Context, id domain.SessionID) ([]domain.CurrentHeadReviewRun, error) {
	rows, err := s.qr.ListCurrentHeadReviewRunsBySession(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list current-head review runs for %s: %w", id, err)
	}
	out := make([]domain.CurrentHeadReviewRun, 0, len(rows))
	for _, r := range rows {
		out = append(out, domain.CurrentHeadReviewRun{
			SessionID: id,
			Harness:   r.Harness,
			PRURL:     r.PRURL,
			Status:    r.Status,
			Verdict:   r.Verdict,
			ID:        r.ID,
			CreatedAt: r.CreatedAt,
		})
	}
	return out, nil
}

// ListCurrentHeadReviewRunsForSessions batches
// ListCurrentHeadReviewRunsForSession for session-list reads, returning every
// requested session id with an empty slice when the current head has no runs.
func (s *Store) ListCurrentHeadReviewRunsForSessions(ctx context.Context, ids []domain.SessionID) (map[domain.SessionID][]domain.CurrentHeadReviewRun, error) {
	out := make(map[domain.SessionID][]domain.CurrentHeadReviewRun, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	encoded, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("marshal session ids: %w", err)
	}
	rows, err := s.qr.ListCurrentHeadReviewRunsBySessions(ctx, string(encoded))
	if err != nil {
		return nil, fmt.Errorf("list current-head review runs for sessions: %w", err)
	}
	for _, r := range rows {
		out[r.SessionID] = append(out[r.SessionID], domain.CurrentHeadReviewRun{
			SessionID: r.SessionID,
			Harness:   r.Harness,
			PRURL:     r.PRURL,
			Status:    r.Status,
			Verdict:   r.Verdict,
			ID:        r.ID,
			CreatedAt: r.CreatedAt,
		})
	}
	for _, id := range ids {
		if out[id] == nil {
			out[id] = []domain.CurrentHeadReviewRun{}
		}
	}
	return out, nil
}
