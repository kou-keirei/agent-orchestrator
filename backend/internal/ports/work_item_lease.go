package ports

import (
	"context"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkItemLeaseStore consumes identities admitted into the authoritative
// WorkItem store. It does not admit identities or observe native permissions.
// Supersession is explicit admission by the caller, even after expiry/release;
// completion is terminal. All mutations return transactional durable readback.
type WorkItemLeaseStore interface {
	AcquireWorkItemLease(context.Context, domain.WorkItemIdentity, domain.WorkLeaseGrant) (domain.WorkItemLease, error)
	GetWorkItemLease(context.Context, domain.WorkItemID) (domain.WorkItemLease, bool, error)
	SupersedeWorkItemLease(context.Context, domain.WorkItemIdentity, domain.WorkItemLease, domain.SessionID, domain.WorkLeaseGrant) (domain.WorkItemLease, error)
	RenewWorkItemLease(context.Context, domain.WorkItemIdentity, domain.WorkItemLease, time.Time) (domain.WorkItemLease, error)
	ReleaseWorkItemLease(context.Context, domain.WorkItemIdentity, domain.WorkItemLease) (domain.WorkItemLease, error)
	CompleteWorkItemLease(context.Context, domain.WorkItemIdentity, domain.WorkItemLease) (domain.WorkItemLease, error)
	// CheckWorkItemEffect checks a hypothetical effect against fresh durable
	// identity, revision, expiry and allowlist. It never executes an effect or
	// grants a reusable permit: callers must not use it as a runtime TOCTOU gate.
	// Effective native/provider permission remains UNPROVEN.
	CheckWorkItemEffect(context.Context, domain.WorkItemCandidate, domain.WorkItemLease, string) error
}
