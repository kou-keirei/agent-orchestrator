package chat

import (
	"context"
	"sync/atomic"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// BindAuthenticatedControllerReportCallback connects only the native source
// owned by the controller captured by the guarded reporter. The trusted command
// owner calls this after turn projection commits, before authored data arrives.
// Caller-supplied native IDs or report content cannot select the source or turn.
// Unsupported providers remain held; this does not mount a public ingress.
func (s *Service) BindAuthenticatedControllerReportCallback(ctx context.Context, binding domain.WorkerReportActorBinding,
	bindings ports.WorkerReportBindingReader,
	acceptance func(ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error),
) error {
	return s.bindAuthenticatedControllerReportCallback(ctx, binding, bindings, acceptance, nil, "")
}

func (s *Service) bindAuthenticatedControllerReportCallback(ctx context.Context, binding domain.WorkerReportActorBinding,
	bindings ports.WorkerReportBindingReader,
	acceptance func(ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error), expected *Controller, expectedTurn string,
) error {
	bridge, err := s.BindWorkerReportNativeBridge(ctx, binding, bindings, acceptance)
	if err != nil {
		return err
	}
	if expected != nil && (bridge.actor.controller != expected || bridge.actor.turn.providerTurn != expectedTurn) {
		return domain.ErrWorkerReportActorMismatch
	}
	source, ok := bridge.actor.controller.conv.(ports.AuthenticatedControllerReportSource)
	if !ok {
		return domain.WorkerReportNativeIngressBlock{}
	}
	// Reserve under the existing actor guard before handing out even an inactive
	// callback. Keep the slot after failure/cancellation: the source may retain
	// the callback even when installation fails. Do not hold locks over source I/O.
	release, err := bridge.actor.AcquireWorkerReportActor(ctx, bridge.report(NativeWorkerReportContent{}))
	if err != nil {
		return err
	}
	controller := bridge.actor.controller
	controller.mu.Lock()
	previous := controller.reportCallback
	if previous != nil && previous.bridge.actor.turn == bridge.actor.turn {
		controller.mu.Unlock()
		release()
		return domain.WorkerReportNativeIngressBlock{}
	}
	if previous != nil {
		previous.active.Store(false)
	}
	callback := &authenticatedControllerReportCallback{bridge: bridge}
	controller.reportCallback = callback
	controller.mu.Unlock()
	release()

	if err := source.BindAuthenticatedControllerReportCallback(ctx, bridge.actor.turn.providerTurn, callback.submit); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Activation shares the same guard as reservation and submission. A late
	// source return cannot activate an installation retired by a newer turn.
	release, err = bridge.actor.AcquireWorkerReportActor(ctx, bridge.report(NativeWorkerReportContent{}))
	if err != nil {
		return err
	}
	defer release()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.reportCallback != callback {
		return domain.ErrWorkerReportActorMismatch
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	callback.active.Store(true)
	return nil
}

type authenticatedControllerReportCallback struct {
	bridge *WorkerReportNativeBridge
	active atomic.Bool
}

func (c *authenticatedControllerReportCallback) submit(ctx context.Context, content ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error) {
	if !c.active.Load() {
		return domain.WorkerReportReceipt{}, domain.WorkerReportNativeIngressBlock{}
	}
	return c.bridge.Submit(ctx, content)
}
