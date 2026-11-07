// Package reportactor supplies the fail-closed worker-report caller adapter.
// It does not issue credentials or mount a report route.
package reportactor

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var _ ports.WorkerReportActorVerifier = Unavailable{}

// Unavailable is the zero-value verifier while AO has no qualified report
// caller issuer. Existing native-host/browser capabilities have other purposes;
// this adapter never upgrades those capabilities or authored references into
// report authority. It has no accepting branch, even for a well-formed report
// matching durable work/lease facts. It is not mounted in production.
type Unavailable struct{}

// VerifyWorkerReportActor always returns the typed actor-binding HOLD. Neither
// context cancellation nor report validation can turn missing proof into success.
func (Unavailable) VerifyWorkerReportActor(context.Context, domain.WorkerReport) error {
	return domain.ErrWorkerReportActorBindingUnavailable
}
