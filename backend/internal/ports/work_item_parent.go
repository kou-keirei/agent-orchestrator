package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkItemParentIssuer is an AO-owned issuance boundary, not a worker/report/HTTP
// DTO sink. Implementations must independently resolve the live ORIGINAL parent
// from AO's accepted work authorization and verify its native session, genuine
// controller generation, provider conversation and scope under AO's controller
// ownership gate. Unknown provenance, restored history without unique live
// continuity, or a substituted coordinator must HOLD before any work is created.
//
// The WorkItem and target must be inserted and read back atomically at original
// issuance. Exact retries may read the same target, never replace or backfill it;
// existing legacy WorkItems must HOLD. Owner/lease changes and report retries
// must not modify it. The input deliberately has no asserted parent target or
// provenance. The worker native ID is worker bookkeeping only.
//
// Chat supplies this capability bound to one live controller instance. A bare
// SQLite Store cannot observe a live controller and refuses this operation.
type WorkItemParentIssuer interface {
	IssueWorkItemFromOriginalParent(context.Context, domain.WorkItemIdentity, string) (domain.WorkItemParentTarget, error)
}

// WorkItemParentPersistence is an internal adapter boundary, never an ingress
// DTO sink. Only an AO controller-bound issuer may supply the observed target,
// holding its existing ownership/dispatch gates through commit. SQLite checks
// the durable controller, conversation and workspace fences transactionally.
type WorkItemParentPersistence interface {
	PersistWorkItemOriginalParent(context.Context, domain.WorkItemIdentity, string, domain.WorkItemParentTarget) (domain.WorkItemParentTarget, error)
}

// WorkItemSpawnReceipt is exact durable readback, never a reusable launch permit.
type WorkItemSpawnReceipt struct {
	Parent    domain.WorkItemParentTarget
	Lease     domain.WorkItemLease
	Admission domain.WorkItemAdmission
}

// WorkItemSpawnIssuer is an opt-in internal live-controller capability. It
// commits original issuance, the first lease and capacity reservation together.
// It accepts no asserted parent target or old lease. Lost acknowledgement must
// leave capacity occupied until separately confirmed reconciliation.
type WorkItemSpawnIssuer interface {
	IssueWorkItemSpawn(context.Context, domain.WorkItemIdentity, string, domain.WorkLeaseGrant) (WorkItemSpawnReceipt, error)
}

// WorkItemSpawnPersistence has the same trusted controller-only boundary as
// WorkItemParentPersistence. All four facts share one writer-fenced transaction;
// existing incomplete issuance must HOLD rather than acquire launch authority.
type WorkItemSpawnPersistence interface {
	PersistWorkItemSpawn(context.Context, domain.WorkItemIdentity, string, domain.WorkItemParentTarget, domain.WorkLeaseGrant) (WorkItemSpawnReceipt, error)
}

// WorkItemParentReader supplies immutable durable issuance readback for a later
// report consumer. Missing legacy targets return typed HOLD, never a lookup of
// today's parent/coordinator or a worker native reference. A successful read is
// only destination provenance: it grants neither a current live delivery permit
// nor semantic ACK authority. #7 delivery and #9 wake require their own future
// continuity checks and explicit semantic reconciliation; transport success is
// not ACK. Neither this port nor its current adapter sends or wakes anything.
type WorkItemParentReader interface {
	GetWorkItemOriginalParent(context.Context, domain.WorkItemID, domain.WorkAttemptID) (domain.WorkItemParentTarget, error)
}
