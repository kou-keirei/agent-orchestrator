package store

import (
	"context"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ListDueExactParentReports selects existing singleton receipts for the daemon's
// outbox runner. It grants no delivery authority; Prepare owns fresh admission.
// Submitted/dead-letter reports cannot repeatedly wake the original parent.
func (s *Store) ListDueExactParentReports(ctx context.Context, project domain.ProjectID, now time.Time, after string, limit int) (ids []string, err error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT e.worker_report_id
	 FROM orchestration_events e JOIN worker_reports r ON r.id=e.worker_report_id AND r.event_id=e.id
	 WHERE e.project_id=? AND e.kind='worker_report' AND e.state IN ('pending','leased')
	 AND e.submitted_at IS NULL AND e.next_attempt_at<=?
	 AND (e.lease_expires_at IS NULL OR e.lease_expires_at<=?)
	 AND e.worker_report_id>?
	 ORDER BY e.worker_report_id LIMIT ?`, project, now, now, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
