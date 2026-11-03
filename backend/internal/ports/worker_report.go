package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkerReportStore accepts explicit reports against fresh durable work/lease
// facts. Exact retries read the original receipt without granting fresh effects,
// even after expiry or ownership change. It never delivers or semantically ACKs.
type WorkerReportStore interface {
	AcceptWorkerReport(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error)
	GetWorkerReport(context.Context, string) (domain.WorkerReportReceipt, bool, error)
}
