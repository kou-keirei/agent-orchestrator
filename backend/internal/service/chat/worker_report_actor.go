package chat

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type workerReportNativeTurn struct {
	session      string
	turn         string
	providerTurn string
}

// Called with c.mu held, only after primary turn projection commits. A provider
// dispatch ACK, a local/scoped turn ID, or nested-thread event is insufficient.
func (c *Controller) observeWorkerReportTurnLocked(event ports.ChatEvent) {
	if event.NativeTurnID == "" || event.ProviderTurnID == "" ||
		event.ProviderConversationID == "" || event.ProviderConversationID != c.conv.ProviderConversationID() {
		c.reportTurn = nil
		return
	}
	if c.reportTurn == nil || c.reportTurn.session != event.ProviderConversationID ||
		c.reportTurn.turn != event.NativeTurnID || c.reportTurn.providerTurn != event.ProviderTurnID {
		c.reportTurn = &workerReportNativeTurn{session: event.ProviderConversationID, turn: event.NativeTurnID, providerTurn: event.ProviderTurnID}
	}
}

// BindWorkerReportActor issues an in-process daemon caller handle. binding must
// be the daemon's already admitted WorkItem and lease readback, never decoded
// report/HTTP claims. The native identities are independently captured from the
// selected controller's committed event, not supplied by the caller.
//
// This is an internal capability, not a public worker credential issuer. No HTTP
// route is connected. Production ingress remains blocked by the missing native
// authored-report invocation carrying this handle (WorkerReportNativeIngressBlock).
// Native turn start alone does not prove authorship; only the holder may submit
// an explicit report. Never issue a fresh handle in response to report JSON.
func (s *Service) BindWorkerReportActor(ctx context.Context, binding domain.WorkerReportActorBinding, bindings ports.WorkerReportBindingReader) (ports.WorkerReportActorVerifier, domain.WorkerReportNativeOrigin, error) {
	if err := binding.Identity.Validate(); err != nil {
		return nil, domain.WorkerReportNativeOrigin{}, err
	}
	if bindings == nil || binding.Lease.WorkItemID != binding.Identity.ID ||
		binding.Lease.OwnerGeneration != binding.Identity.OwnerGeneration ||
		binding.Lease.Grant.ID == "" || binding.Lease.Revision <= 0 {
		return nil, domain.WorkerReportNativeOrigin{}, domain.ErrInvalidWorkLease
	}
	gate := s.controllerGate(binding.Identity.OwnerSessionID)
	if err := gate.lock(ctx); err != nil {
		return nil, domain.WorkerReportNativeOrigin{}, err
	}
	defer gate.unlock()
	c, err := s.Controller(binding.Identity.OwnerSessionID)
	if err != nil {
		return nil, domain.WorkerReportNativeOrigin{}, err
	}
	c.mu.Lock()
	turn := c.reportTurn
	ready := turn != nil && c.pendingTurnID == turn.providerTurn && c.handoff == controllerHandoffNone && c.state == ports.ChatControllerBusy
	c.mu.Unlock()
	if !ready {
		return nil, domain.WorkerReportNativeOrigin{}, domain.WorkerReportNativeIngressBlock{}
	}
	// Retain only value identity; discard the caller-owned effect slice.
	// Effects are not report authority.
	binding.Lease.Grant.AllowedEffects = nil
	actor := &workerReportActor{service: s, controller: c, turn: turn, binding: binding, bindings: bindings}
	return actor, domain.WorkerReportNativeOrigin{SessionID: turn.session, TurnID: turn.turn}, nil
}

type workerReportActor struct {
	service    *Service
	controller *Controller
	turn       *workerReportNativeTurn
	binding    domain.WorkerReportActorBinding
	bindings   ports.WorkerReportBindingReader
}

var _ ports.WorkerReportActorGuard = (*workerReportActor)(nil)

// VerifyWorkerReportActor authenticates possession of the daemon-issued handle
// and the captured current controller/turn before any storage read. Durable
// lease and controller facts are then checked even for exact receipt retries.
func (a *workerReportActor) VerifyWorkerReportActor(ctx context.Context, report domain.WorkerReport) error {
	release, err := a.AcquireWorkerReportActor(ctx, report)
	if err == nil {
		release()
	}
	return err
}

// AcquireWorkerReportActor holds the existing controller and dispatch gates
// until Acceptance finishes, excluding retirement during receipt/write access.
func (a *workerReportActor) AcquireWorkerReportActor(ctx context.Context, report domain.WorkerReport) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := a.binding
	if report.Identity != b.Identity || report.WorkerSessionID != b.Identity.OwnerSessionID ||
		report.LeaseID != b.Lease.Grant.ID || report.LeaseRevision != b.Lease.Revision ||
		report.OriginalNativeSessionID != b.OriginalNativeSessionID ||
		report.NativeSessionID != a.turn.session || report.NativeTurnID != a.turn.turn {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	s, c := a.service, a.controller
	gate := s.controllerGate(c.sessionID)
	if err := gate.lock(ctx); err != nil {
		return nil, err
	}
	releaseGate := true
	defer func() {
		if releaseGate {
			gate.unlock()
		}
	}()
	current, err := s.Controller(c.sessionID)
	if err != nil || current != c {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	c.sendMu.Lock()
	defer func() {
		if releaseGate {
			c.sendMu.Unlock()
		}
	}()
	c.mu.Lock()
	currentTurn := c.reportTurn == a.turn && c.pendingTurnID == a.turn.providerTurn && c.handoff == controllerHandoffNone && c.state == ports.ChatControllerBusy
	c.mu.Unlock()
	if !currentTurn {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	// Origin is established above. Lease existence never establishes it.
	currentBinding, found, err := a.bindings.ReadWorkerReportBinding(ctx, b.Identity.ID)
	if err != nil {
		return nil, err
	}
	if !found || currentBinding.Identity != b.Identity || currentBinding.OriginalNativeSessionID != b.OriginalNativeSessionID {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	lease := currentBinding.Lease
	if lease.WorkItemID != b.Identity.ID || lease.OwnerGeneration != b.Identity.OwnerGeneration ||
		lease.Revision != b.Lease.Revision || lease.Grant.ID != b.Lease.Grant.ID || lease.State != domain.WorkLeaseActive {
		return nil, domain.ErrWorkLeaseConflict
	}
	if !s.now().Before(lease.ExpiresAt) || !s.now().Before(b.Lease.ExpiresAt) {
		return nil, domain.ErrWorkLeaseExpired
	}
	rec, found, err := s.sessions.GetSession(ctx, c.sessionID)
	if err != nil {
		return nil, err
	}
	if !found || rec.Kind != domain.KindWorker || rec.ProjectID != b.Identity.ProjectID ||
		rec.IsTerminated || rec.Mode != domain.SessionModeChat || rec.Metadata.ControllerGeneration != c.generation ||
		rec.Metadata.ProviderConversationID != a.turn.session {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	releaseGate = false
	return func() { c.sendMu.Unlock(); gate.unlock() }, nil
}
