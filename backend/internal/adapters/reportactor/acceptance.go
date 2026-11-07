package reportactor

import (
	"context"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var _ ports.WorkerReportSubmission = (*Acceptance)(nil)

// Acceptance composes invocation-specific caller verification with the existing
// transactional report writer. It does not issue caller authority. A daemon
// caller must supply a qualified runtime-bound verifier, never derive one from
// the report or HTTP request. No such issuer is currently wired in production.
// Its zero value holds without accessing storage.
type Acceptance struct {
	verifier ports.WorkerReportActorVerifier
	store    ports.WorkerReportStore
}

// NewAcceptance requires a report store and defaults missing caller issuance to
// the existing typed HOLD. The result exposes no unverified receipt lookup.
func NewAcceptance(verifier ports.WorkerReportActorVerifier, store ports.WorkerReportStore) (*Acceptance, error) {
	if store == nil {
		return nil, errors.New("worker report acceptance requires a store")
	}
	return &Acceptance{verifier: verifier, store: store}, nil
}

// SubmitWorkerReport verifies before all store access, even historical receipt
// readback. Verifier errors cannot expose a receipt or enqueue an event. A nil
// result delegates only this invocation to the writer, never a reusable permit.
func (a *Acceptance) SubmitWorkerReport(ctx context.Context, report domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	if a.verifier == nil {
		return domain.WorkerReportReceipt{}, domain.ErrWorkerReportActorBindingUnavailable
	}
	if guard, ok := a.verifier.(ports.WorkerReportActorGuard); ok {
		release, err := guard.AcquireWorkerReportActor(ctx, report)
		if err != nil {
			return domain.WorkerReportReceipt{}, err
		}
		if release == nil {
			return domain.WorkerReportReceipt{}, domain.ErrWorkerReportActorBindingUnavailable
		}
		defer release()
	} else {
		if err := a.verifier.VerifyWorkerReportActor(ctx, report); err != nil {
			return domain.WorkerReportReceipt{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	receipt, err := a.store.AcceptWorkerReport(ctx, report)
	if err != nil {
		return domain.WorkerReportReceipt{}, err
	}
	return receipt, nil
}
