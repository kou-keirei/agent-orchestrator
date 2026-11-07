package domain

import "errors"

// ErrWorkerReportActorMismatch rejects a claim or handle outside its captured
// worker, native turn, WorkItem, attempt, lease, or controller ownership.
var ErrWorkerReportActorMismatch = errors.New("worker report caller binding mismatch")

// WorkerReportActorBinding is an internal admission readback, not a JSON request.
// The issuer must receive it from the daemon's WorkItem/lease owner.
type WorkerReportActorBinding struct {
	Identity                WorkItemIdentity
	Lease                   WorkItemLease
	OriginalNativeSessionID string
}

// WorkerReportNativeOrigin contains observed native references, not credentials.
type WorkerReportNativeOrigin struct {
	SessionID string
	TurnID    string
}

// WorkerReportNativeIngressBlock identifies the remaining production edge. The
// native event stream has turn events but no explicit authored-report invocation
// carrying the daemon's per-WorkItem caller handle. Public ingress must not mint
// such a handle from JSON, a generic ACK, or knowledge of a running native turn.
type WorkerReportNativeIngressBlock struct{}

func (WorkerReportNativeIngressBlock) Error() string {
	return "BLOCKS_PRODUCTION: native authored worker-report invocation carrying daemon-issued WorkItem caller handle is unavailable"
}

func (WorkerReportNativeIngressBlock) Unwrap() error { return ErrWorkerReportActorBindingUnavailable }

// ErrWorkerReportActorBindingUnavailable means no qualified caller binding is
// available. It must never be treated as a successful report verification.
var ErrWorkerReportActorBindingUnavailable error = WorkerReportActorBindingHold{}

// WorkerReportActorBindingHold is a typed source HOLD, not evidence that the
// named worker or provider is dead, unauthorized, or has completed its work.
type WorkerReportActorBindingHold struct{}

func (WorkerReportActorBindingHold) Error() string {
	return "HOLD_WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE"
}
