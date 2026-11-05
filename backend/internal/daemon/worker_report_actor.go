package daemon

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

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
