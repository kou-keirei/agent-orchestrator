package chat

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// NativeWorkerReportContent contains only explicitly authored report data.
// It cannot select work, ownership, a lease, a parent or native provenance.
type NativeWorkerReportContent = ports.AuthoredWorkerReportContent

// WorkerReportNativeBridge is a per-committed-turn in-process capability. The
// native invocation owner must retain it before receiving report content. Never
// mint one in response to JSON, model text or a generic provider dispatch ACK.
// Possession authenticates invocation; content and native IDs do not.
type WorkerReportNativeBridge struct {
	binding    domain.WorkerReportActorBinding
	origin     domain.WorkerReportNativeOrigin
	submission ports.WorkerReportSubmission
	actor      *workerReportActor
}

// BindWorkerReportNativeBridge connects the admitted command to its current
// committed worker turn. The daemon factory supplies existing guarded Acceptance
// through a port, keeping persistence/adapter composition out of Chat. The actor
// and its origin are never exposed to the report content producer.
func (s *Service) BindWorkerReportNativeBridge(ctx context.Context, binding domain.WorkerReportActorBinding,
	bindings ports.WorkerReportBindingReader,
	acceptance func(ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error),
) (*WorkerReportNativeBridge, error) {
	if acceptance == nil {
		return nil, domain.WorkerReportNativeIngressBlock{}
	}
	actor, origin, err := s.BindWorkerReportActor(ctx, binding, bindings)
	if err != nil {
		return nil, err
	}
	nativeActor, ok := actor.(*workerReportActor)
	if !ok {
		return nil, domain.WorkerReportNativeIngressBlock{}
	}
	submission, err := acceptance(actor)
	if err != nil {
		return nil, err
	}
	if submission == nil {
		return nil, domain.WorkerReportNativeIngressBlock{}
	}
	binding.Lease.Grant.AllowedEffects = nil
	return &WorkerReportNativeBridge{binding: binding, origin: origin, submission: submission, actor: nativeActor}, nil
}

// Submit accepts only an explicit invocation by the native capability holder.
// Guarded acceptance revalidates the retained turn/owner/lease before all receipt
// access and holds the controller gates through the atomic report/outbox write.
// Completion/retirement makes even exact retries fail; callers cannot rebind here.
func (b *WorkerReportNativeBridge) Submit(ctx context.Context, content NativeWorkerReportContent) (domain.WorkerReportReceipt, error) {
	if b == nil || b.submission == nil {
		return domain.WorkerReportReceipt{}, domain.WorkerReportNativeIngressBlock{}
	}
	return b.submission.SubmitWorkerReport(ctx, b.report(content))
}

func (b *WorkerReportNativeBridge) report(content NativeWorkerReportContent) domain.WorkerReport {
	binding := b.binding
	return domain.WorkerReport{
		ID: content.ID, Type: content.Type, Version: content.Version,
		Identity: binding.Identity, WorkerSessionID: binding.Identity.OwnerSessionID,
		LeaseID: binding.Lease.Grant.ID, LeaseRevision: binding.Lease.Revision,
		OriginalNativeSessionID: binding.OriginalNativeSessionID,
		NativeSessionID:         b.origin.SessionID, NativeTurnID: b.origin.TurnID,
		Content: content.Content, ArtifactReference: content.ArtifactReference, Digest: content.Digest,
	}
}
