package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkItemAdmissionStore reserves capacity one across all WorkItems in one AO
// database. It consumes the existing admitted identity and exact live lease;
// it does not issue either, schedule work, or grant provider permissions.
// Native spawn integration is unsupported until an AO owner supplies that
// boundary. A successful reservation/readback must never itself launch a worker.
type WorkItemAdmissionStore interface {
	// ReserveWorkItemAdmission returns the same receipt for an exact live retry,
	// without extending its lease. Other attempts HOLD while the slot is held,
	// including after expiry or ownership change. Released attempts stay closed.
	ReserveWorkItemAdmission(context.Context, domain.WorkItemIdentity, domain.WorkItemLease) (domain.WorkItemAdmission, error)
	// ReleaseWorkItemAdmission requires the exact receipt and current original
	// reservation owner. The caller must first reconcile that no worker can
	// continue or start from this reservation; timeout alone is insufficient.
	// Release is idempotent and does not release/renew the WorkItem lease.
	ReleaseWorkItemAdmission(context.Context, domain.WorkItemIdentity, domain.WorkItemAdmission) (domain.WorkItemAdmission, error)
}
