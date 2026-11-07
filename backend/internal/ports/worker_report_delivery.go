package ports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ErrWorkerReportDeliveryHeld means accepted reports remain unresolved because
// exact native parent continuity and semantic reconciliation cannot be proved.
// Transport admission, a provider turn, and HTTP success are not semantic ACKs.
var ErrWorkerReportDeliveryHeld = errors.New("worker report delivery held: exact parent semantic ACK unavailable")

// WorkerReportDeliveryHoldReason is the bounded durable outbox diagnostic.
const WorkerReportDeliveryHoldReason = "report_parent_semantic_ack_unavailable"

// WorkerReportDeliveryStore is the safe subset until a native semantic ACK port
// exists. It preserves the immutable #8 report/enqueue receipt, work attempt,
// lease revision and owner generation. It must not discover a replacement
// parent, assign a batch/client message ID, submit, or ACK a report. The authored
// OriginalNativeSessionID names worker provenance, not a frozen parent history.
type WorkerReportDeliveryStore interface {
	// HoldWorkerReportDeliveries durably marks unresolved reports and any
	// previously mixed batches for attention without changing their bindings.
	// It returns true while such work exists, including after restart.
	HoldWorkerReportDeliveries(context.Context, domain.ProjectID, time.Time) (bool, error)
	// WorkerReportSemanticallyResolved reads the existing singleton binding and
	// immutable receipt. Only the internal exact semantic writer's acknowledged
	// state qualifies; generic/legacy ACKs and mixed bindings return false.
	// This read does not authenticate a new event or mutate delivery state.
	WorkerReportSemanticallyResolved(context.Context, domain.OrchestrationEvent) (bool, error)
	// ClearResolvedWorkerReportAttention follows successful notification resolution.
	// It rechecks semantic readback transactionally and preserves every unresolved
	// report marker. Hold passes must retain markers until this step succeeds.
	ClearResolvedWorkerReportAttention(context.Context, []domain.OrchestrationEvent) error
}

// ReportDeliveryHold is a typed refusal by the opt-in delivery engine. It is
// never evidence of parent authentication, transport admission or resolution.
type ReportDeliveryHold struct{ Reason string }

func (h *ReportDeliveryHold) Error() string { return "worker report delivery HOLD: " + h.Reason }
func (h *ReportDeliveryHold) Unwrap() error { return ErrWorkerReportDeliveryHeld }

// ExactReportParent is a delivery projection of W557's immutable
// GetWorkItemOriginalParent(work, attempt) readback, NOT another WorkItem owner
// or issuance API. The adapter must validate the complete donor target (including
// issuer/provenance) before projecting these fields. Metadata or authored report
// native IDs cannot supply this capability. No production adapter is bound here.
type ExactReportParent struct {
	WorkItemID             domain.WorkItemID
	AttemptID              domain.WorkAttemptID
	SessionID              domain.SessionID
	NativeSessionID        string
	ControllerGeneration   string
	ProviderConversationID string
	ProviderScopeID        string
}

// ExactReportParentReader must ONLY adapt validated immutable original-parent
// readback. Missing, ambiguous or legacy history returns an error; it must never
// discover a current orchestrator or backfill an original target.
type ExactReportParentReader interface {
	ReadReportOriginalParent(context.Context, domain.WorkItemID, domain.WorkAttemptID) (ExactReportParent, error)
}

// ExactReportMessage is a bounded snapshot persisted in the existing outbox's
// lease_token before Send. The token is an opaque singleton batch binding, not
// a new outbox. Payload is stored verbatim; recovery never renders it again.
// ClientMessageID is the stable digest of this same durable snapshot.
type ExactReportMessage struct {
	ReportID, EventID, SourceRevision, LeaseID string
	LeaseRevision                              int64
	Parent                                     ExactReportParent
	Payload                                    string
}

const exactReportTokenPrefix = "ao-exact-report-v1:"

// Token encodes the durable singleton binding within the existing TEXT column.
func (m ExactReportMessage) Token() string {
	b, _ := json.Marshal(m) // strings and integers only
	return exactReportTokenPrefix + string(b)
}

// ClientMessageID is independent of attempt time, process and transport result.
func (m ExactReportMessage) ClientMessageID() string {
	sum := sha256.Sum256([]byte(m.Token()))
	return "ao-report-" + hex.EncodeToString(sum[:])
}

// ParseExactReportMessage rejects legacy/mixed batch tokens and noncanonical
// encodings. It does not authenticate the source or original-parent authority.
func ParseExactReportMessage(token string) (ExactReportMessage, error) {
	var m ExactReportMessage
	if len(token) > 64*1024 || !strings.HasPrefix(token, exactReportTokenPrefix) {
		return m, &ReportDeliveryHold{Reason: "report_binding_unavailable"}
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(token, exactReportTokenPrefix)), &m); err != nil || m.Token() != token {
		return ExactReportMessage{}, &ReportDeliveryHold{Reason: "report_binding_invalid"}
	}
	return m, nil
}

// ValidateSource checks the snapshot against immutable acceptance and event
// readback. Parent authority and current continuity remain separate native ports.
func (m ExactReportMessage) ValidateSource(r domain.WorkerReportReceipt, e domain.OrchestrationEvent) error {
	if err := r.Report.Validate(); err != nil {
		return err
	}
	b, _ := json.Marshal(r.Report)
	sum := sha256.Sum256(b)
	source := "worker-report:v1:" + hex.EncodeToString(sum[:])
	if m.ReportID != r.Report.ID || m.EventID != r.EventID || e.ID != r.EventID ||
		e.Kind != domain.OrchestrationWorkerReport || e.SourceRevision != source || m.SourceRevision != source ||
		e.ProjectID != r.Report.Identity.ProjectID || e.WorkerID != r.Report.WorkerSessionID ||
		m.LeaseID != r.Report.LeaseID || m.LeaseRevision != r.Report.LeaseRevision ||
		m.Parent.WorkItemID != r.Report.Identity.ID || m.Parent.AttemptID != r.Report.Identity.AttemptID ||
		m.Parent.SessionID != r.Report.Identity.OriginalParentSessionID {
		return &ReportDeliveryHold{Reason: "report_source_mismatch"}
	}
	for _, value := range []string{string(m.Parent.SessionID), m.Parent.NativeSessionID,
		m.Parent.ControllerGeneration, m.Parent.ProviderConversationID, m.Parent.ProviderScopeID} {
		if value == "" || len(value) > 1024 || strings.TrimSpace(value) != value ||
			strings.ContainsAny(value, "\x00\r\n") || !utf8.ValidString(value) {
			return &ReportDeliveryHold{Reason: "report_parent_incomplete"}
		}
	}
	if m.Payload == "" || len(m.Payload) > 32*1024 || !utf8.ValidString(m.Payload) || len(m.Token()) > 64*1024 {
		return &ReportDeliveryHold{Reason: "report_payload_limit"}
	}
	return nil
}

// ExactReportReconciliation is produced ONLY by an authenticated parent-native
// semantic event reader. Admission/turn completion/HTTP success cannot supply it.
// Message binds the exact event, report, lease revision, work/attempt and native
// controller/conversation. The native event reference is opaque, not a credential.
type ExactReportReconciliation struct {
	NativeEventID   string
	ClientMessageID string
	Message         ExactReportMessage
}

// ExactReportChat is usable only inside WithLiveReportParent's ownership gate.
// ReadReportReconciliation must authenticate the event's parent-native origin
// and explicit semantic reconciliation meaning, not trust a worker DTO or echo.
type ExactReportChat interface {
	// SendReport uses Chat's existing durable ClientMessageID replay contract:
	// an identical ID/payload is the same delivery, including after restart.
	// It must not bypass that boundary with a raw provider-driver Send.
	SendReport(context.Context, string, string) (admitted bool, err error)
	ReadReportReconciliation(context.Context, string) (ExactReportReconciliation, error)
}

// ExactReportNative delegates live ownership to Chat's native controller gate.
// It must reject terminated, replaced, restored-without-continuity or ambiguous
// parents and keep the SAME genuine controller/conversation fenced throughout
// fn, including Send and semantic readback. observed is independently obtained
// live identity, never a copy of expected. This is an unbound integration hook;
// ordinary Chat Send and stored session metadata do not implement the contract.
type ExactReportNative interface {
	WithLiveReportParent(context.Context, ExactReportParent, func(observed ExactReportParent, chat ExactReportChat) error) error
}

// ErrExactReportParentExited is a positive native observation, not a failed
// probe, idle signal or missing controller. WithLiveReportParent may return it
// only before invoking its callback and without admitting a send.
var ErrExactReportParentExited = errors.New("exact report parent positively exited")

// ExactReportResumer is an optional, unbound adapter to accepted native Resume.
// Under the SAME native ownership gate it must verify genuine provider history
// for the issuance-time native ID, conversation and scope BEFORE resuming that
// exact parent. It must refuse terminated/replaced/foreign/ambiguous history,
// busy controllers and any generation change that the accepted owner contract
// cannot prove continuous with issuance. It must never create a conversation,
// discover a coordinator, backfill provenance or copy expected into observed.
// The callback runs only after exact continuity is verified, with the genuine
// resumed controller fenced through Send and readback. No production adapter
// is supplied here; PR61 issuance and PR60 Resume acceptance are prerequisites.
type ExactReportResumer interface {
	WithResumedReportParent(context.Context, ExactReportParent, func(observed ExactReportParent, chat ExactReportChat) error) error
}

// ExactReportWriter is scoped to one report and one project dispatch gate.
// Implementations CAS the singleton source/binding and never modify reports.
// Resolve is internal to the semantic consumer, never a transport ACK endpoint.
// Prepare rejects stale work/owner/lease and outbox snapshots; Admit requires
// this invocation's still-live prepared outbox lease. Resolve may consume a
// historical prepared event after reclaim without admitting another send, and
// retains attention until the notifier's durable resolution succeeds.
// Writers sample now inside the write transaction, after reading the state
// being checked. A timestamp captured before writer admission is not fresh.
type ExactReportWriter interface {
	Read(context.Context) (domain.WorkerReportReceipt, domain.OrchestrationEvent, error)
	Prepare(context.Context, ExactReportMessage, domain.OrchestrationEvent, func() time.Time) error
	Admit(context.Context, ExactReportMessage, func() time.Time) error
	Resolve(context.Context, ExactReportMessage, func() time.Time) error
	Hold(context.Context, time.Time) error
}

// ExactReportStore serializes the entire callback with the existing project
// dispatch/hold gate. Writer must not escape fn. No global DB writer is held
// over native I/O. The legacy dispatcher's report refusal remains unchanged.
type ExactReportStore interface {
	WithExactParentReport(context.Context, domain.ProjectID, string, func(ExactReportWriter) error) error
}
