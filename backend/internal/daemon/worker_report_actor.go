package daemon

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// submitNativeWorkerReport is the private daemon boundary for an authored
// native invocation holding an already-issued Chat actor. It must never be
// called by minting an actor from report JSON, environment, or loopback access.
// The guard pins the selected controller/turn through transactional acceptance.
// No native authored-report callback currently supplies this handle; callers
// without one receive the typed production HOLD before any store access.
// Acceptance enqueues the existing outbox, never semantic parent ACK.
func submitNativeWorkerReport(ctx context.Context, actor ports.WorkerReportActorGuard, st ports.WorkerReportStore, report domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	if actor == nil {
		return domain.WorkerReportReceipt{}, domain.WorkerReportNativeIngressBlock{}
	}
	acceptance, err := reportactor.NewAcceptance(actor, st)
	if err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	return acceptance.SubmitWorkerReport(ctx, report)
}

// workerReportBindings adapts existing authoritative reads without changing the
// WorkItem store. It is for internal native caller binding only; no public report
// entrypoint is wired here. The actor must be authenticated before invoking it.
type workerReportBindings struct{ store *sqlite.Store }

var _ ports.WorkerReportBindingReader = workerReportBindings{}

func (r workerReportBindings) ReadWorkerReportBinding(ctx context.Context, id domain.WorkItemID) (domain.WorkerReportActorBinding, bool, error) {
	item, found, err := r.store.GetWorkItem(ctx, id)
	if err != nil || !found {
		return domain.WorkerReportActorBinding{}, false, err
	}
	lease, found, err := r.store.GetWorkItemLease(ctx, id)
	if err != nil || !found {
		return domain.WorkerReportActorBinding{}, false, err
	}
	return domain.WorkerReportActorBinding{Identity: item.Identity, OriginalNativeSessionID: item.OriginalNativeSessionID, Lease: lease}, true, nil
}
