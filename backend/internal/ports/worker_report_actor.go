package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// WorkerReportActorVerifier verifies the caller of one report submission.
// The report is an untrusted claim, including all native references. A qualified
// implementation must obtain the caller independently from AO's authenticated
// worker runtime binding and trusted native session handle, and correlate it to
// AO's actual worker session, admitted WorkItem/attempt/owner generation, issued
// lease ID/revision, and observed native session AND native turn. Stale, expired,
// released or superseded ownership must not authorize a fresh submission.
//
// JSON fields, Host/Origin, loopback peers, caller-supplied context values or
// environment variables, and the existence of a lease are never positive origin
// evidence. Neither browser access nor a Chat host attachment alone establishes
// worker-report authority. Missing or unqualified issuance/binding must return
// domain.ErrWorkerReportActorBindingUnavailable, never nil.
//
// Verification must precede receipt lookup as well as fresh acceptance; exact
// retry payloads do not authenticate their submitter. Any error stops acceptance
// and outbox effects. Nil would establish only caller identity for this invocation,
// not a reusable lease/effect permit, provider attestation, delivery or parent ACK.
// The accepting writer still owns its transactional identity/lease freshness
// checks. Chat can issue an in-process handle from a committed native turn and
// independently admitted WorkItem binding. Public authored-report ingress is
// still unwired; native turn observation alone never authenticates report JSON.
type WorkerReportActorVerifier interface {
	VerifyWorkerReportActor(context.Context, domain.WorkerReport) error
}

// WorkerReportActorGuard keeps an authenticated controller/turn selected through
// the accepting call using AO's existing controller gate. Acceptance releases it
// on every outcome. It grants no durable lease or provider-effect authority.
type WorkerReportActorGuard interface {
	WorkerReportActorVerifier
	AcquireWorkerReportActor(context.Context, domain.WorkerReport) (release func(), err error)
}

// WorkerReportBindingReader exposes authoritative admission and lease readback
// after caller authentication. Lease rows alone cannot prove current ownership.
type WorkerReportBindingReader interface {
	ReadWorkerReportBinding(context.Context, domain.WorkItemID) (domain.WorkerReportActorBinding, bool, error)
}
