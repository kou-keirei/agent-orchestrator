package ports

import (
	"context"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ErrWorkerReportDeliveryHeld means accepted reports remain unresolved because
// exact native parent continuity and semantic reconciliation cannot be proved.
// Transport admission, a provider turn, and HTTP success are not semantic ACKs.
var ErrWorkerReportDeliveryHeld = errors.New("worker report delivery held: exact parent semantic ACK unavailable")

// WorkerReportDeliveryHoldReason is the bounded durable outbox diagnostic.
const WorkerReportDeliveryHoldReason = "report_parent_semantic_ack_unavailable"

// WorkerReportDeliveryStore is the safe subset until a native semantic ACK port
// exists. It preserves the immutable #8 report/enqueue receipt, work attempt,
// lease revision and owner generation. It must not discover a replacement
// parent, assign a batch/client message ID, submit, or ACK a report. The authored
// OriginalNativeSessionID names worker provenance, not a frozen parent history.
type WorkerReportDeliveryStore interface {
	// HoldWorkerReportDeliveries durably marks unresolved reports and any
	// previously mixed batches for attention without changing their bindings.
	// It returns true while such work exists, including after restart.
	HoldWorkerReportDeliveries(context.Context, domain.ProjectID, time.Time) (bool, error)
}
