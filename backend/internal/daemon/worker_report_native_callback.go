package daemon

import (
	"context"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	sessionmanager "github.com/aoagents/agent-orchestrator/backend/internal/session_manager"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// nativeWorkItemCommand is retained by the trusted AO producer, never stored in
// HTTP context or handed to a public route. It captures the original parent at
// authorization and uses the same immutable command for every birth attempt.
// The trusted spawn path offers the Codex report tool after admission. Generic
// tool/activity/text events remain insufficient to submit a report.
type nativeWorkItemCommand struct {
	command *sessionmanager.NativeWorkItemCommand
	chat    *chatsvc.Service
	store   *sqlite.Store
}

func prepareNativeWorkItem(manager *sessionmanager.Manager, chat *chatsvc.Service, store *sqlite.Store,
	spec sessionmanager.NativeWorkItemSpec,
) (*nativeWorkItemCommand, error) {
	if manager == nil || chat == nil || store == nil {
		return nil, domain.ErrWorkAdmissionHeld
	}
	issuer, err := chat.OriginalParentIssuer(spec.Parent)
	if err != nil {
		return nil, err
	}
	command, err := manager.PrepareNativeWorkItem(spec, issuer)
	if err != nil {
		return nil, err
	}
	return &nativeWorkItemCommand{command: command, chat: chat, store: store}, nil
}

func (c *nativeWorkItemCommand) Spawn(ctx context.Context) (domain.SessionRecord, error) {
	if c == nil {
		return domain.SessionRecord{}, domain.ErrWorkAdmissionHeld
	}
	return c.command.Spawn(ctx)
}

// ConnectReporter runs on the trusted invocation owner's committed-turn callback,
// before authored content arrives. It cannot select another work by report ID.
// The installed capability belongs only to this turn; it never follows a later
// controller, owner generation or lease revision. Submission enqueues durably;
// the existing ExactParentReports dispatcher owns delivery to the frozen parent.
func (c *nativeWorkItemCommand) ConnectReporter(ctx context.Context) error {
	if c == nil || c.command == nil || c.store == nil || c.chat == nil {
		return domain.WorkerReportNativeIngressBlock{}
	}
	bindings := workerReportBindings{store: c.store}
	binding, found, err := bindings.ReadWorkerReportBinding(ctx, c.command.WorkItemID())
	if err != nil {
		return err
	}
	if !found || binding.Identity.OwnerGeneration != 1 || binding.Identity.OwnerSessionID != binding.Identity.OriginalWorkerSessionID {
		return domain.ErrWorkerReportActorMismatch
	}
	return c.chat.BindAuthenticatedControllerReportCallback(ctx, binding, bindings, func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
		return reportactor.NewAcceptance(actor, c.store)
	})
}

// prepareWorkerReportTool runs before thread creation, only for the WorkItem ID
// carried by the admitted native spawn. Neither public DTOs nor tool JSON can
// call it. Retain this exact binding; later turns cannot select a newer owner or
// lease from authored content.
func prepareWorkerReportTool(ctx context.Context, store *sqlite.Store, work domain.WorkItemID, worker domain.SessionID) (ports.WorkerReportToolAdmission, error) {
	if store == nil || work == "" || worker == "" {
		return ports.WorkerReportToolAdmission{}, domain.WorkerReportNativeIngressBlock{}
	}
	bindings := workerReportBindings{store: store}
	binding, found, err := bindings.ReadWorkerReportBinding(ctx, work)
	if err != nil {
		return ports.WorkerReportToolAdmission{}, err
	}
	i, lease := binding.Identity, binding.Lease
	if !found || i.Validate() != nil || i.OwnerSessionID != worker || i.OriginalWorkerSessionID != worker || i.OwnerGeneration != 1 ||
		lease.WorkItemID != i.ID || lease.OwnerGeneration != i.OwnerGeneration || lease.State != domain.WorkLeaseActive ||
		lease.Revision <= 0 || lease.Grant.Validate() != nil || !time.Now().Before(lease.ExpiresAt) {
		return ports.WorkerReportToolAdmission{}, domain.ErrWorkerReportActorMismatch
	}
	target, err := store.GetWorkItemOriginalParent(ctx, i.ID, i.AttemptID)
	if err != nil {
		return ports.WorkerReportToolAdmission{}, err
	}
	if err := target.ValidateFor(i); err != nil {
		return ports.WorkerReportToolAdmission{}, err
	}
	// Revalidate through AO's existing idempotent reservation contract. A newer
	// lease must not inherit the original launch's immutable admission.
	if _, err := store.ReserveWorkItemAdmission(ctx, i, lease); err != nil {
		return ports.WorkerReportToolAdmission{}, err
	}
	return ports.WorkerReportToolAdmission{Binding: binding, Bindings: bindings,
		Acceptance: func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
			return reportactor.NewAcceptance(actor, store)
		}}, nil
}
