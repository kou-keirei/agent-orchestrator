package store

import (
	"context"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var _ ports.WorkerReportDeliveryStore = (*Store)(nil)

// HoldWorkerReportDeliveries preserves every source and delivery binding. No
// native semantic ACK interface exists yet, so even a transport-admitted report
// stays unresolved. A preexisting mixed batch is indivisible: legacy members
// sharing its client message ID must not be replayed with a changed payload.
func (s *Store) HoldWorkerReportDeliveries(ctx context.Context, project domain.ProjectID, at time.Time) (held bool, err error) {
	err = s.WithOrchestrationDispatch(ctx, project, func() error {
		if err := s.writeMu.LockContext(ctx); err != nil {
			return err
		}
		defer s.writeMu.Unlock()
		result, err := s.writeDB.ExecContext(ctx, `UPDATE orchestration_events
		 SET attention_required_at=COALESCE(attention_required_at,?),last_error=?
		 WHERE project_id=? AND (kind='worker_report' OR state IN ('pending','leased','submitted','dead_letter'))
		 AND (kind='worker_report' OR (NULLIF(TRIM(lease_token),'') IS NOT NULL AND EXISTS (
		   SELECT 1 FROM orchestration_events r WHERE r.project_id=orchestration_events.project_id
		   AND r.kind='worker_report' AND r.lease_token=orchestration_events.lease_token)))`, at, ports.WorkerReportDeliveryHoldReason, project)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		held = rows > 0
		return err
	})
	return held, err
}
