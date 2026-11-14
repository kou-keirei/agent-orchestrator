package ports

import "context"

// ParentSemanticACKBinding is captured by the AO owner, never decoded from tool
// arguments or HTTP claims. Message includes immutable work/attempt, original
// parent, controller generation, report, outbox and lease identities. The turn
// is the committed primary turn that received that exact durable message.
type ParentSemanticACKBinding struct {
	Message                                                        ExactReportMessage
	ConversationID, BranchID, TurnID, ProviderTurnID, NativeTurnID string
}

// CommittedParentSemanticACK is an explicit native reconciliation fact, not a
// tool request, model answer, idle/completion event or successful transport.
type CommittedParentSemanticACK struct {
	NativeEventID string
	Binding       ParentSemanticACKBinding
}

// CommittedReportParentSemanticACKReader reads an authenticated durable event
// from this selected primary conversation. Events must survive restart and be
// reread by stable ID. Missing/ambiguous commitment must return an error. Neither
// an authored ACK body nor an AO conversation projection implements this port.
type CommittedReportParentSemanticACKReader interface {
	ReadCommittedReportParentSemanticACK(context.Context, string) (CommittedParentSemanticACK, error)
}

// AuthenticatedParentSemanticACKSource is an optional AO-owned conversation
// capability. Binding happens before notification, outside authored arguments.
// The in-process callback carries only an event reference and must be invoked
// after binding returns, after native commitment and outside provider locks.
// The source owns durable event readback/retry; callback success alone must never
// manufacture a native fact. A failed binding grants no callback authority.
//
// No current Codex conversation implements this port: PRODUCER_UNBOUND HOLD.
// PR90's worker report acceptance tool is not a parent semantic producer.
type AuthenticatedParentSemanticACKSource interface {
	CommittedReportParentSemanticACKReader
	BindAuthenticatedParentSemanticACK(context.Context, ParentSemanticACKBinding, func(context.Context, string) error) error
}
