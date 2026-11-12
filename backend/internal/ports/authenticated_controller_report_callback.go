package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// AuthoredWorkerReportContent contains authored data, never caller authority.
type AuthoredWorkerReportContent struct {
	ID                string
	Type              string
	Version           int
	Content           string
	ArtifactReference string
	Digest            string
}

// WorkerReportToolAdmission is internal launch wiring from the trusted WorkItem
// spawn path. It is never decoded from an API request or provider arguments.
type WorkerReportToolAdmission struct {
	Binding    domain.WorkerReportActorBinding
	Bindings   WorkerReportBindingReader
	Acceptance func(WorkerReportActorVerifier) (WorkerReportSubmission, error)
}

// CommittedWorkerReportTool receives only committed primary native turn starts.
// The source arranges binding outside the projection loop and enables dispatch
// only after connect succeeds. Report requests cannot initiate binding.
type CommittedWorkerReportTool interface {
	ConnectCommittedWorkerReportTurn(context.Context, string, func(context.Context) error)
}

// AuthenticatedControllerReportSource is an optional native conversation port.
// The AO-owned conversation must associate the callback with providerTurnID
// before receiving an explicit authored report. It must not derive invocation
// from prose, generic tool/activity events, HTTP claims or dispatch ACKs.
// The callback is in-process authority: never serialize or expose it to a route.
// It becomes callable only after binding returns successfully; a synchronous
// invocation during binding is held. A failed binding must not be retried by
// selecting another controller or turn on the strength of report content.
// Codex implements this only for an admitted dynamic report tool. Other providers
// remain held rather than synthesizing authored events.
type AuthenticatedControllerReportSource interface {
	BindAuthenticatedControllerReportCallback(ctx context.Context, providerTurnID string,
		submit func(context.Context, AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error)) error
}
