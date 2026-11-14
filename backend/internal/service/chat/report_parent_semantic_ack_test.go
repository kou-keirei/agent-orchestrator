package chat_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// Synthetic provider contract only: the JSON file models durable native event
// readback independently of AO rows. It does not qualify any real provider.
type parentSemanticProvider struct {
	*reportParentProvider
	path     string
	mu       sync.Mutex
	callback func(context.Context, string) error
	binding  ports.ParentSemanticACKBinding
	bindErr  error
	onBind   func(func(context.Context, string) error)
	onRead   func()
	reads    int
	history  string
}

func (p *parentSemanticProvider) ReadHistory(ctx context.Context) ([]ports.ChatEvent, error) {
	events, err := p.reportParentProvider.ReadHistory(ctx)
	for i := range events {
		events[i].ProviderConversationID = p.ProviderConversationID()
		if events[i].Kind == ports.ChatEventTurnStarted {
			events[i].NativeTurnID = "native-" + events[i].ProviderTurnID
			switch p.history {
			case "nested":
				events[i].ProviderConversationID = "nested-child"
			case "missing-native":
				events[i].NativeTurnID = ""
			case "missing-event":
				events[i].ProviderEventID = ""
			}
		}
	}
	return events, err
}

func (p *parentSemanticProvider) BindAuthenticatedParentSemanticACK(_ context.Context, b ports.ParentSemanticACKBinding, cb func(context.Context, string) error) error {
	p.mu.Lock()
	p.binding, p.callback = b, cb
	p.mu.Unlock()
	if p.onBind != nil {
		p.onBind(cb)
	}
	return p.bindErr
}

func (p *parentSemanticProvider) ReadCommittedReportParentSemanticACK(_ context.Context, _ string) (ports.CommittedParentSemanticACK, error) {
	p.mu.Lock()
	p.reads++
	p.mu.Unlock()
	if p.onRead != nil {
		p.onRead()
	}
	b, err := os.ReadFile(p.path)
	var event ports.CommittedParentSemanticACK
	if err == nil {
		err = json.Unmarshal(b, &event)
	}
	return event, err
}

func (p *parentSemanticProvider) commit(t *testing.T, event ports.CommittedParentSemanticACK) {
	t.Helper()
	b, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func semanticParentFixture(t *testing.T) (*reportParentFixture, *parentSemanticProvider, ports.ExactReportMessage) {
	t.Helper()
	p := &parentSemanticProvider{path: filepath.Join(t.TempDir(), "synthetic-native-event.json")}
	f := newReportParentFixtureWithNative(t, func(base *reportParentProvider) ports.ChatConversation {
		p.reportParentProvider = base
		return p
	})
	if err := f.engine().Deliver(t.Context(), testProject, f.receipt.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	f.settle(t)
	m, err := ports.ParseExactReportMessage(f.event(t).LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	return f, p, m
}

func bindSemanticParent(ctx context.Context, f *reportParentFixture, m ports.ExactReportMessage) error {
	return f.service.BindReportParentSemanticACK(ctx, m, func(callCtx context.Context, native ports.ExactReportNative, id string) error {
		d := f.engine()
		d.Native = native
		return d.Reconcile(callCtx, testProject, f.receipt.Report.ID, id)
	})
}

func TestReportParentSemanticACKCallbackDurableReplay(t *testing.T) {
	f, p, m := semanticParentFixture(t)
	before := f.event(t)
	if err := bindSemanticParent(t.Context(), f, m); err != nil {
		t.Fatal(err)
	}
	if p.binding.Message != m || p.binding.ConversationID == "" || p.binding.BranchID == "" ||
		p.binding.TurnID == "" || p.binding.ProviderTurnID == "" || p.binding.NativeTurnID == "" {
		t.Fatalf("incomplete owner binding: %+v", p.binding)
	}
	// Binding, an event reference, and turn completion do not commit semantics.
	if err := p.callback(t.Context(), "native-semantic-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	if !f.event(t).AcknowledgedAt.IsZero() {
		t.Fatal("uncommitted event acknowledged")
	}
	p.commit(t, ports.CommittedParentSemanticACK{NativeEventID: "native-semantic-event", Binding: p.binding})
	if err := p.callback(t.Context(), "native-semantic-event"); err != nil {
		t.Fatal(err)
	}
	resolved := f.event(t)
	if resolved.State != domain.OrchestrationAcknowledged || resolved.AcknowledgedAt.IsZero() ||
		resolved.AttentionRequiredAt.IsZero() || resolved.LeaseToken != before.LeaseToken ||
		resolved.AttemptCount != before.AttemptCount || resolved.DestinationSessionID != before.DestinationSessionID {
		t.Fatalf("incorrect semantic transition: before=%+v after=%+v", before, resolved)
	}
	// Concurrent callback replay uses the real project gate and singleton writer.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := p.callback(t.Context(), "native-semantic-event"); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if after := f.event(t); !reflect.DeepEqual(after, resolved) {
		t.Fatalf("callback replay changed durable state: %+v", after)
	}
	// Reopen the durable AO writer and reread the native fixture file. This proves
	// storage/readback replay, not live-controller restart continuity.
	reopened, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	d := f.engine()
	d.Store, d.Parents = reopened, reopened
	d.Native = chatsvc.ReportParentSemanticACKSource{Chat: f.service}
	if err := d.Reconcile(t.Context(), testProject, m.ReportID, "native-semantic-event"); err != nil {
		t.Fatal(err)
	}
	if p.reads != 11 || !reflect.DeepEqual(f.event(t), resolved) || f.provider.sendCallCount() != 1 || f.starts.Load() != 1 || f.resumes.Load() != 0 {
		t.Fatalf("replay cached, resent or replaced: reads=%d", p.reads)
	}
	// Removing native proof after ACK must refuse replay, without undoing ACK.
	if err := os.Remove(p.path); err != nil {
		t.Fatal(err)
	}
	if err := p.callback(t.Context(), "native-semantic-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !reflect.DeepEqual(f.event(t), resolved) {
		t.Fatalf("unverified replay escaped: %v", err)
	}
}

func TestReportParentSemanticACKRejectsEventIdentity(t *testing.T) {
	for _, field := range []string{"event", "work", "attempt", "parent", "native", "generation", "thread", "scope", "report", "outbox", "source", "lease", "lease-revision", "payload", "conversation", "branch", "turn", "provider-turn", "native-turn"} {
		t.Run(field, func(t *testing.T) {
			f, p, m := semanticParentFixture(t)
			if err := bindSemanticParent(t.Context(), f, m); err != nil {
				t.Fatal(err)
			}
			e := ports.CommittedParentSemanticACK{NativeEventID: "semantic-event", Binding: p.binding}
			b := &e.Binding
			switch field {
			case "event":
				e.NativeEventID = "foreign"
			case "work":
				b.Message.Parent.WorkItemID = "foreign"
			case "attempt":
				b.Message.Parent.AttemptID = "foreign"
			case "parent":
				b.Message.Parent.SessionID = "foreign"
			case "native":
				b.Message.Parent.NativeSessionID = "foreign"
			case "generation":
				b.Message.Parent.ControllerGeneration = "foreign"
			case "thread":
				b.Message.Parent.ProviderConversationID = "foreign"
			case "scope":
				b.Message.Parent.ProviderScopeID = "foreign"
			case "report":
				b.Message.ReportID = "foreign"
			case "outbox":
				b.Message.EventID = "foreign"
			case "source":
				b.Message.SourceRevision = "foreign"
			case "lease":
				b.Message.LeaseID = "foreign"
			case "lease-revision":
				b.Message.LeaseRevision++
			case "payload":
				b.Message.Payload = "model says acknowledged"
			case "conversation":
				b.ConversationID = "foreign"
			case "branch":
				b.BranchID = "foreign"
			case "turn":
				b.TurnID = "foreign"
			case "provider-turn":
				b.ProviderTurnID = "foreign"
			case "native-turn":
				b.NativeTurnID = "foreign"
			}
			p.commit(t, e)
			if err := p.callback(t.Context(), "semantic-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !f.event(t).AcknowledgedAt.IsZero() {
				t.Fatalf("foreign binding escaped: %v", err)
			}
			// Recovery without a captured callback must independently revalidate
			// the event against the durable turn and frozen outbox as well.
			d := f.engine()
			d.Native = chatsvc.ReportParentSemanticACKSource{Chat: f.service}
			if err := d.Reconcile(t.Context(), testProject, m.ReportID, "semantic-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !f.event(t).AcknowledgedAt.IsZero() {
				t.Fatalf("foreign recovery event escaped: %v", err)
			}
		})
	}
}

func TestReportParentSemanticACKBindingFailureNeverArmsCallback(t *testing.T) {
	for _, mode := range []string{"success", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f, p, m := semanticParentFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p.onBind = func(cb func(context.Context, string) error) {
				if err := cb(t.Context(), "early-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Errorf("synchronous callback escaped: %v", err)
				}
				if mode == "cancel" {
					cancel()
				}
			}
			if mode == "error" {
				p.bindErr = errors.New("injected binding error")
			}
			err := bindSemanticParent(ctx, f, m)
			if mode == "success" && err != nil {
				t.Fatal(err)
			}
			if mode != "success" {
				if err == nil {
					t.Fatal("failed binding succeeded")
				}
				if err := p.callback(t.Context(), "late-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatal(err)
				}
			}
			if p.reads != 0 || !f.event(t).AcknowledgedAt.IsZero() {
				t.Fatal("binding created semantic proof")
			}
		})
	}
}

func TestReportParentSemanticACKCancellationAndStaleOwner(t *testing.T) {
	for _, mode := range []string{"cancel-before", "cancel-during", "generation", "stopped", "rollback", "failed", "interrupted"} {
		t.Run(mode, func(t *testing.T) {
			f, p, m := semanticParentFixture(t)
			if err := bindSemanticParent(t.Context(), f, m); err != nil {
				t.Fatal(err)
			}
			p.commit(t, ports.CommittedParentSemanticACK{NativeEventID: "semantic-event", Binding: p.binding})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "cancel-before":
				cancel()
			case "cancel-during":
				p.onRead = cancel
			case "generation":
				if err := f.store.ClaimChatControllerGeneration(t.Context(), testSession, "replacement-generation"); err != nil {
					t.Fatal(err)
				}
			case "stopped":
				if err := f.service.Stop(t.Context(), testSession); err != nil {
					t.Fatal(err)
				}
			case "rollback":
				if _, err := f.store.RollbackTurns(t.Context(), p.binding.ConversationID, p.binding.TurnID, time.Now()); err != nil {
					t.Fatal(err)
				}
			case "failed", "interrupted":
				state := domain.TurnStateFailed
				if mode == "interrupted" {
					state = domain.TurnStateInterrupted
				}
				if err := f.store.SettleTurnByID(t.Context(), p.binding.TurnID, state, "fixture cancellation", time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			err := p.callback(ctx, "semantic-event")
			if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !f.event(t).AcknowledgedAt.IsZero() {
				t.Fatalf("invalid callback escaped: %v", err)
			}
			if strings.HasPrefix(mode, "cancel-") && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestReportParentSemanticACKRejectsNonPrimaryHistory(t *testing.T) {
	for _, history := range []string{"nested", "missing-native", "missing-event"} {
		t.Run(history, func(t *testing.T) {
			f, p, m := semanticParentFixture(t)
			p.history = history
			if err := bindSemanticParent(t.Context(), f, m); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || p.callback != nil || !f.event(t).AcknowledgedAt.IsZero() {
				t.Fatalf("uncommitted primary binding escaped: %v", err)
			}
		})
	}
}

func TestReportParentSemanticACKUnboundAndInvalidReferences(t *testing.T) {
	f := newReportParentFixture(t)
	d := f.engine()
	d.Native = chatsvc.ReportParentSemanticACKSource{Chat: f.service}
	_ = d.Deliver(t.Context(), testProject, f.receipt.Report.ID)
	f.settle(t)
	m, err := ports.ParseExactReportMessage(f.event(t).LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindSemanticParent(t.Context(), f, m); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !strings.Contains(err.Error(), "producer_unbound") {
		t.Fatal(err)
	}
	for _, id := range []string{"http-200", "fixture parent answer", "native-turn-completed", "asserted-parent-ack"} {
		if err := d.Reconcile(t.Context(), testProject, m.ReportID, id); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !strings.Contains(err.Error(), "producer_unbound") {
			t.Fatal(err)
		}
	}
	if f.event(t).State != domain.OrchestrationSubmitted || !f.event(t).AcknowledgedAt.IsZero() || f.provider.sendCallCount() != 1 {
		t.Fatal("unbound source altered delivery")
	}
	var escaped ports.ExactReportChat
	if err := d.Native.WithLiveReportParent(t.Context(), m.Parent, func(_ ports.ExactReportParent, chat ports.ExactReportChat) error {
		escaped = chat
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := escaped.ReadReportReconciliation(t.Context(), "late-event"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !strings.Contains(err.Error(), "scope_expired") {
		t.Fatal(err)
	}

	bound, p, message := semanticParentFixture(t)
	if err := bindSemanticParent(t.Context(), bound, message); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", " event", "event\n", "event\x00", string([]byte{0xff}), strings.Repeat("a", 1025)} {
		if err := p.callback(t.Context(), id); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
			t.Fatal(err)
		}
	}
	if p.reads != 0 || !bound.event(t).AcknowledgedAt.IsZero() {
		t.Fatal("invalid reference reached native source")
	}
}
