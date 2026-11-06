package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkerReportSubmission verifies the caller on every invocation, including
// exact retries, before accessing the report store. The accepting store still
// checks fresh durable identity and lease facts inside its write transaction.
// A receipt proves acceptance/enqueue only; submission never delivers or ACKs.
// Unlike WorkerReportStore, this port must not be implemented by storage alone.
type WorkerReportSubmission interface {
	SubmitWorkerReport(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error)
}
