package orchestrationevents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// These are disposable boundary fixtures, not production native authentication.
type exactReportFixture struct {
	receipt                                                                                       domain.WorkerReportReceipt
	event                                                                                         domain.OrchestrationEvent
	parent                                                                                        ports.ExactReportParent
	observed                                                                                      ports.ExactReportParent
	ack                                                                                           ports.ExactReportReconciliation
	now                                                                                           time.Time
	readErr, parentErr, liveErr, prepareErr, sendErr, admitErr, semanticErr, resolveErr, holdErr  error
	reads, parentReads, liveCalls, prepares, sends, admissions, semanticReads, resolutions, holds int
	admitted                                                                                      bool
	clientID, payload                                                                             string
}

func newExactReportFixture() *exactReportFixture {
	f := &exactReportFixture{now: time.Unix(2000000000, 0).UTC(), admitted: true}
	r := domain.WorkerReport{ID: "report", Type: "checkpoint", Version: 1,
		Identity: domain.WorkItemIdentity{ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo",
			ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent", OriginalWorkerSessionID: "worker",
			OwnerSessionID: "worker", OwnerGeneration: 1, WorktreePath: "/work", Branch: "branch"},
		LeaseID: "lease", LeaseRevision: 3, WorkerSessionID: "worker", NativeSessionID: "worker-native",
		NativeTurnID: "worker-turn", OriginalNativeSessionID: "worker-native", Content: "authored report prose"}
	sum := sha256.Sum256([]byte(r.Content))
	r.Digest = hex.EncodeToString(sum[:])
	body, _ := json.Marshal(r)
	sum = sha256.Sum256(body)
	source := "worker-report:v1:" + hex.EncodeToString(sum[:])
	f.receipt = domain.WorkerReportReceipt{Report: r, EventID: "oe:" + source, AcceptedAtMS: f.now.UnixMilli()}
	f.event = domain.OrchestrationEvent{ID: f.receipt.EventID, SourceRevision: source, Kind: domain.OrchestrationWorkerReport,
		ProjectID: "project", WorkerID: "worker", State: domain.OrchestrationPending, EnqueuedAt: f.now, NextAttemptAt: f.now}
	f.parent = ports.ExactReportParent{WorkItemID: "work", AttemptID: "attempt", SessionID: "parent", NativeSessionID: "parent-native",
		ControllerGeneration: "parent-generation", ProviderConversationID: "parent-conversation", ProviderScopeID: "parent-scope"}
	f.observed = f.parent
	return f
}

func (f *exactReportFixture) engine() *ExactParentReports {
	return &ExactParentReports{Store: f, Parents: f, Native: f, Now: func() time.Time { return f.now }}
}

func (f *exactReportFixture) WithExactParentReport(_ context.Context, _ domain.ProjectID, _ string, fn func(ports.ExactReportWriter) error) error {
	return fn(f)
}
func (f *exactReportFixture) Read(context.Context) (domain.WorkerReportReceipt, domain.OrchestrationEvent, error) {
	f.reads++
	return f.receipt, f.event, f.readErr
}
func (f *exactReportFixture) ReadReportOriginalParent(_ context.Context, work domain.WorkItemID, attempt domain.WorkAttemptID) (ports.ExactReportParent, error) {
	f.parentReads++
	if work != f.receipt.Report.Identity.ID || attempt != f.receipt.Report.Identity.AttemptID {
		return ports.ExactReportParent{}, errors.New("wrong parent read key")
	}
	return f.parent, f.parentErr
}
func (f *exactReportFixture) WithLiveReportParent(_ context.Context, expected ports.ExactReportParent, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	f.liveCalls++
	if f.liveErr != nil {
		return f.liveErr
	}
	if expected != f.parent {
		return errors.New("wrong native target")
	}
	return fn(f.observed, f)
}
func (f *exactReportFixture) Prepare(_ context.Context, m ports.ExactReportMessage, _ domain.OrchestrationEvent, now func() time.Time) error {
	at := now()
	f.prepares++
	if f.prepareErr != nil {
		return f.prepareErr
	}
	f.event.LeaseToken, f.event.DestinationSessionID = m.Token(), m.Parent.SessionID
	f.event.State, f.event.LeaseExpiresAt = domain.OrchestrationLeased, at.Add(LeaseDuration)
	f.event.AttemptCount++
	return nil
}
func (f *exactReportFixture) SendReport(_ context.Context, id, payload string) (bool, error) {
	f.sends++
	f.clientID, f.payload = id, payload
	return f.admitted, f.sendErr
}
func (f *exactReportFixture) Admit(_ context.Context, _ ports.ExactReportMessage, now func() time.Time) error {
	at := now()
	f.admissions++
	if f.admitErr == nil {
		f.event.State, f.event.SubmittedAt = domain.OrchestrationSubmitted, at
	}
	return f.admitErr
}
func (f *exactReportFixture) ReadReportReconciliation(context.Context, string) (ports.ExactReportReconciliation, error) {
	f.semanticReads++
	return f.ack, f.semanticErr
}
func (f *exactReportFixture) Resolve(_ context.Context, _ ports.ExactReportMessage, now func() time.Time) error {
	at := now()
	f.resolutions++
	if f.resolveErr == nil {
		f.event.State, f.event.AcknowledgedAt = domain.OrchestrationAcknowledged, at
	}
	return f.resolveErr
}
func (f *exactReportFixture) Hold(_ context.Context, at time.Time) error {
	f.holds++
	f.event.AttentionRequiredAt = at
	return f.holdErr
}

func (f *exactReportFixture) semanticEvent(t *testing.T) {
	t.Helper()
	m, err := ports.ParseExactReportMessage(f.event.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	f.ack = ports.ExactReportReconciliation{NativeEventID: "parent-semantic-event", ClientMessageID: m.ClientMessageID(), Message: m}
}

func TestExactReportDeliveryAdmissionAndSeparateSemanticEvent(t *testing.T) {
	f := newExactReportFixture()
	d := f.engine()
	if err := d.Deliver(t.Context(), "project", "report"); !errors.Is(err, ErrReportDeliveryHeld) {
		t.Fatalf("transport result: %v", err)
	}
	if f.prepares != 1 || f.sends != 1 || f.admissions != 1 || f.resolutions != 0 || f.semanticReads != 0 ||
		f.event.State != domain.OrchestrationSubmitted || !f.event.AcknowledgedAt.IsZero() || f.event.AttentionRequiredAt.IsZero() ||
		strings.Contains(f.payload, f.receipt.Report.Content) || !strings.Contains(f.payload, f.receipt.Report.ID) {
		t.Fatalf("admission advanced semantics or lost source: %+v", f)
	}
	if err := d.Deliver(t.Context(), "project", "report"); !errors.Is(err, ErrReportDeliveryHeld) || f.sends != 1 {
		t.Fatalf("admitted replay sent another wake: %v sends=%d", err, f.sends)
	}
	f.semanticEvent(t)
	if err := d.Reconcile(t.Context(), "project", "report", f.ack.NativeEventID); err != nil {
		t.Fatal(err)
	}
	if f.semanticReads != 1 || f.resolutions != 1 || f.sends != 1 || f.event.State != domain.OrchestrationAcknowledged ||
		f.event.AcknowledgedAt.IsZero() || f.event.AttentionRequiredAt.IsZero() {
		t.Fatalf("semantic result: %+v", f)
	}
}

func TestExactReportDeliveryHoldsBeforeWriterOrSend(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*exactReportFixture, *ExactParentReports)
	}{
		{"unbound reader", func(_ *exactReportFixture, d *ExactParentReports) { d.Parents = nil }},
		{"unbound native", func(_ *exactReportFixture, d *ExactParentReports) { d.Native = nil }},
		{"missing legacy history", func(f *exactReportFixture, _ *ExactParentReports) {
			f.parentErr = errors.New("missing original history")
		}},
		{"ambiguous history", func(f *exactReportFixture, _ *ExactParentReports) {
			f.parentErr = errors.New("ambiguous original history")
		}},
		{"foreign work", func(f *exactReportFixture, _ *ExactParentReports) { f.parent.WorkItemID = "other" }},
		{"foreign attempt", func(f *exactReportFixture, _ *ExactParentReports) { f.parent.AttemptID = "other" }},
		{"current orchestrator", func(f *exactReportFixture, _ *ExactParentReports) { f.parent.SessionID = "replacement" }},
		{"worker native provenance", func(f *exactReportFixture, _ *ExactParentReports) { f.parent.NativeSessionID = "" }},
		{"terminated parent", func(f *exactReportFixture, _ *ExactParentReports) {
			f.liveErr = errors.New("terminated original controller")
		}},
		{"replaced controller", func(f *exactReportFixture, _ *ExactParentReports) { f.observed.ControllerGeneration = "replacement" }},
		{"replaced conversation", func(f *exactReportFixture, _ *ExactParentReports) { f.observed.ProviderConversationID = "replacement" }},
		{"foreign scope", func(f *exactReportFixture, _ *ExactParentReports) { f.observed.ProviderScopeID = "foreign" }},
		{"foreign native", func(f *exactReportFixture, _ *ExactParentReports) { f.observed.NativeSessionID = "worker-native" }},
		{"legacy batch", func(f *exactReportFixture, _ *ExactParentReports) { f.event.LeaseToken = "legacy-batch" }},
		{"corrupt source", func(f *exactReportFixture, _ *ExactParentReports) { f.event.SourceRevision = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExactReportFixture()
			d := f.engine()
			tc.change(f, d)
			err := d.Deliver(t.Context(), "project", "report")
			var held *ports.ReportDeliveryHold
			if !errors.Is(err, ErrReportDeliveryHeld) || !errors.As(err, &held) || f.prepares != 0 || f.sends != 0 ||
				f.admissions != 0 || f.resolutions != 0 || f.holds != 1 || f.event.AttentionRequiredAt.IsZero() || !f.event.AcknowledgedAt.IsZero() {
				t.Fatalf("err=%v fixture=%+v", err, f)
			}
		})
	}
}

func TestExactReportDeliveryRetainsActualErrorsAndWriterCounts(t *testing.T) {
	for _, stage := range []string{"read", "parent", "native", "prepare", "send", "admitted_error", "admit", "hold"} {
		t.Run(stage, func(t *testing.T) {
			f := newExactReportFixture()
			failure := errors.New("injected " + stage)
			wantSends, wantPrepare, wantAdmit := 0, 0, 0
			switch stage {
			case "read":
				f.readErr = failure
			case "parent":
				f.parentErr = failure
			case "native":
				f.liveErr = failure
			case "prepare":
				f.prepareErr, wantPrepare = failure, 1
			case "send":
				f.sendErr, f.admitted, wantPrepare, wantSends = failure, false, 1, 1
			case "admitted_error":
				f.sendErr, wantPrepare, wantSends, wantAdmit = failure, 1, 1, 1
			case "admit":
				f.admitErr, wantPrepare, wantSends, wantAdmit = failure, 1, 1, 1
			case "hold":
				f.holdErr, wantPrepare, wantSends, wantAdmit = failure, 1, 1, 1
			}
			err := f.engine().Deliver(t.Context(), "project", "report")
			if !errors.Is(err, failure) || !errors.Is(err, ErrReportDeliveryHeld) || f.sends != wantSends ||
				f.prepares != wantPrepare || f.admissions != wantAdmit || f.resolutions != 0 || f.holds != 1 {
				t.Fatalf("err=%v fixture=%+v", err, f)
			}
		})
	}
}

func TestExactReportSemanticEventRejectsEchoAndForeignBindings(t *testing.T) {
	for _, state := range []domain.OrchestrationDeliveryState{domain.OrchestrationSubmitted, domain.OrchestrationDeadLetter} {
		t.Run(string(state), func(t *testing.T) {
			for _, field := range []string{"authentication", "native_event", "client_id", "event", "report", "source", "lease", "lease_revision", "work", "attempt", "parent", "generation", "conversation", "scope", "payload", "resolve_error"} {
				t.Run(field, func(t *testing.T) {
					f := newExactReportFixture()
					_ = f.engine().Deliver(t.Context(), "project", "report")
					f.event.State = state
					f.semanticEvent(t)
					failure := errors.New("native semantic read refused")
					switch field {
					case "authentication":
						f.semanticErr = failure
					case "native_event":
						f.ack.NativeEventID = "echo"
					case "client_id":
						f.ack.ClientMessageID = "other"
					case "event":
						f.ack.Message.EventID = "other"
					case "report":
						f.ack.Message.ReportID = "other"
					case "source":
						f.ack.Message.SourceRevision = "other"
					case "lease":
						f.ack.Message.LeaseID = "other"
					case "lease_revision":
						f.ack.Message.LeaseRevision++
					case "work":
						f.ack.Message.Parent.WorkItemID = "other"
					case "attempt":
						f.ack.Message.Parent.AttemptID = "other"
					case "parent":
						f.ack.Message.Parent.SessionID = "other"
					case "generation":
						f.ack.Message.Parent.ControllerGeneration = "other"
					case "conversation":
						f.ack.Message.Parent.ProviderConversationID = "other"
					case "scope":
						f.ack.Message.Parent.ProviderScopeID = "other"
					case "payload":
						f.ack.Message.Payload = "other"
					case "resolve_error":
						f.resolveErr = failure
					}
					err := f.engine().Reconcile(t.Context(), "project", "report", "parent-semantic-event")
					wantResolves := 0
					if field == "resolve_error" {
						wantResolves = 1
					}
					if !errors.Is(err, ErrReportDeliveryHeld) || f.sends != 1 || f.semanticReads != 1 || f.resolutions != wantResolves ||
						!f.event.AcknowledgedAt.IsZero() || f.event.AttentionRequiredAt.IsZero() {
						t.Fatalf("err=%v fixture=%+v", err, f)
					}
					if (field == "authentication" || field == "resolve_error") && !errors.Is(err, failure) {
						t.Fatalf("actual error lost: %v", err)
					}
				})
			}
		})
	}
}

func TestExactReportDeadLetterOnlyReconcilesWithoutTransport(t *testing.T) {
	f := newExactReportFixture()
	d := f.engine()
	_ = d.Deliver(t.Context(), "project", "report")
	f.event.State = domain.OrchestrationDeadLetter
	f.semanticEvent(t)
	if err := d.Deliver(t.Context(), "project", "report"); !errors.Is(err, ErrReportDeliveryHeld) || f.sends != 1 || f.prepares != 1 || f.semanticReads != 0 {
		t.Fatalf("dead-letter delivery escaped: %v %+v", err, f)
	}
	if err := d.Reconcile(t.Context(), "project", "report", f.ack.NativeEventID); err != nil || f.resolutions != 1 || f.sends != 1 || f.prepares != 1 || f.admissions != 1 {
		t.Fatalf("semantic-only recovery: %v %+v", err, f)
	}
}
