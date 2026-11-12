package sessionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// Synthetic native port; the acknowledgement is a projection barrier only.
// It never supplies a parent semantic ACK or native provider qualification.
type workBridgeConversation struct {
	*integrationChatConversation
	committed  chan string
	report     func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error)
	reportTurn string
}

func (c *workBridgeConversation) BindAuthenticatedControllerReportCallback(_ context.Context, turn string,
	submit func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error),
) error {
	c.report, c.reportTurn = submit, turn
	return nil
}

func (c *workBridgeConversation) Submit(ctx context.Context, content chatsvc.NativeWorkerReportContent) (domain.WorkerReportReceipt, error) {
	return c.report(ctx, content)
}

func (c *workBridgeConversation) AcknowledgeProviderEvent(ctx context.Context, id string) error {
	select {
	case c.committed <- id:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *workBridgeConversation) ReadHistory(context.Context) ([]ports.ChatEvent, error) {
	return nil, nil
}

func (c *workBridgeConversation) event(t *testing.T, kind ports.ChatEventKind, id, turn, native string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	event := ports.ChatEvent{Kind: kind, ProviderEventID: id, ProviderTurnID: turn, NativeTurnID: native,
		ProviderConversationID: c.providerID, TurnState: domain.TurnStateCompleted}
	select {
	case c.events <- event:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case got := <-c.committed:
		if got != id {
			t.Fatalf("projection barrier: %s", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type workBridgeBindings struct{ store *sqlite.Store }

func (b workBridgeBindings) ReadWorkerReportBinding(ctx context.Context, id domain.WorkItemID) (domain.WorkerReportActorBinding, bool, error) {
	item, found, err := b.store.GetWorkItem(ctx, id)
	if err != nil || !found {
		return domain.WorkerReportActorBinding{}, false, err
	}
	lease, found, err := b.store.GetWorkItemLease(ctx, id)
	return domain.WorkerReportActorBinding{Identity: item.Identity, OriginalNativeSessionID: item.OriginalNativeSessionID, Lease: lease}, found, err
}

type workBridgeFixture struct {
	*workSpawnFixture
	providers []*workBridgeConversation
	command   *NativeWorkItemCommand
	worker    domain.SessionRecord
	bindings  workBridgeBindings
	binding   domain.WorkerReportActorBinding
}

func newWorkBridgeFixture(t *testing.T) *workBridgeFixture {
	t.Helper()
	b := &workBridgeFixture{}
	b.workSpawnFixture = newWorkSpawnFixture(t, func(f *workSpawnFixture, opts *chatsvc.Options) {
		opts.Drivers = integrationChatRegistry{domain.HarnessCodex: integrationChatDriver{harness: domain.HarnessCodex, start: func() ports.ChatConversation {
			c := &workBridgeConversation{integrationChatConversation: newIntegrationChatConversation(fmt.Sprintf("native-%d", f.births.Add(1))), committed: make(chan string, 16)}
			b.providers = append(b.providers, c)
			return c
		}}}
		opts.Reader = chatsvc.SnapshotReaderFunc(func(ctx context.Context, id string) (chatsvc.ConversationRows, error) {
			r, err := f.store.LoadConversationSnapshot(ctx, id)
			return chatsvc.ConversationRows{Conversation: r.Conversation, Turns: r.Turns, Messages: r.Messages, Activities: r.Activities}, err
		})
	})
	var err error
	b.command, err = b.manager.PrepareNativeWorkItem(NativeWorkItemSpec{Project: b.auth.project, Parent: b.parent.ID,
		Base: b.auth.base, Harness: domain.HarnessCodex, Grant: b.auth.grant}, b.auth.issuer)
	if err != nil {
		t.Fatal(err)
	}
	b.worker, err = b.command.Spawn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b.bindings = workBridgeBindings{b.store}
	var found bool
	b.binding, found, err = b.bindings.ReadWorkerReportBinding(t.Context(), b.command.WorkItemID())
	if err != nil || !found {
		t.Fatalf("binding: %v %v", found, err)
	}
	return b
}

func (b *workBridgeFixture) reporter(t *testing.T) *workBridgeConversation {
	t.Helper()
	err := b.service.BindAuthenticatedControllerReportCallback(t.Context(), b.binding, b.bindings, func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
		return reportactor.NewAcceptance(actor, b.store)
	})
	if err != nil {
		t.Fatal(err)
	}
	if b.providers[1].report == nil || b.providers[1].reportTurn == "" || b.providers[0].report != nil {
		t.Fatal("callback not bound exclusively to worker native source")
	}
	return b.providers[1]
}

func workBridgeContent() chatsvc.NativeWorkerReportContent {
	content := "explicit native checkpoint"
	digest := sha256.Sum256([]byte(content))
	return chatsvc.NativeWorkerReportContent{ID: "report", Type: "checkpoint", Version: 1, Content: content, Digest: hex.EncodeToString(digest[:])}
}

func TestNativeWorkItemBridgeBirthReportAndExactOriginalParent(t *testing.T) {
	b := newWorkBridgeFixture(t)
	if b.births.Load() != 2 || b.runtime.created != 0 {
		t.Fatal("expected one parent and one worker native birth")
	}
	// A provider exists, but no native committed turn: binding must still HOLD.
	_, err := b.service.BindWorkerReportNativeBridge(t.Context(), b.binding, b.bindings, func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
		return reportactor.NewAcceptance(actor, b.store)
	})
	if !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
		t.Fatalf("bound before native turn: %v", err)
	}
	b.providers[1].event(t, ports.ChatEventTurnStarted, "start", "scoped-worker-turn", "native-worker-turn")
	r := b.reporter(t)
	content := workBridgeContent()
	receipt, err := r.Submit(t.Context(), content)
	if err != nil || receipt.EventID == "" || receipt.Report.Identity != b.binding.Identity ||
		receipt.Report.NativeSessionID != b.providers[1].providerID || receipt.Report.NativeTurnID != "native-worker-turn" {
		t.Fatalf("bound report: %+v %v", receipt, err)
	}
	// Exact retries reuse the immutable receipt and its one atomic outbox event.
	for range 2 {
		got, err := r.Submit(t.Context(), content)
		if err != nil || got != receipt {
			t.Fatalf("receipt retry: %+v %v", got, err)
		}
		if _, err := b.command.Spawn(t.Context()); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
			t.Fatalf("birth retry: %v", err)
		}
	}
	// Concurrent callback retries still share the real atomic report/outbox row.
	results := make(chan error, 4)
	for range 4 {
		go func() {
			got, err := r.Submit(t.Context(), content)
			if err == nil && got != receipt {
				err = fmt.Errorf("concurrent callback returned a different receipt: %+v", got)
			}
			results <- err
		}()
	}
	for range 4 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	// Close/reopen an independent reader after acceptance (simulated lost ACK).
	cold, err := sqlite.Open(b.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cold.Close()
	got, found, err := cold.GetWorkerReport(t.Context(), content.ID)
	if err != nil || !found || got != receipt {
		t.Fatalf("durable acceptance: %+v %v %v", got, found, err)
	}
	events, err := cold.ListOrchestrationEvents(t.Context(), b.auth.project, 10)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationPending {
		t.Fatalf("atomic outbox: %+v %v", events, err)
	}
	delivery := &orchestrationevents.ExactParentReports{Store: b.store, Source: b.store, Parents: b.store, Native: b.service}
	if err := delivery.Deliver(t.Context(), b.auth.project, content.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatalf("semantic ACK must HOLD: %v", err)
	}
	events, err = cold.ListOrchestrationEvents(t.Context(), b.auth.project, 10)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationSubmitted ||
		events[0].DestinationSessionID != b.parent.ID || !events[0].AcknowledgedAt.IsZero() {
		t.Fatalf("exact parent delivery: %+v %v", events, err)
	}
	b.providers[0].mu.Lock()
	if len(b.providers[0].sent) != 1 {
		t.Fatalf("parent received %d reports", len(b.providers[0].sent))
	}
	b.providers[0].mu.Unlock()
	// Delivery replay never wakes the original parent again while awaiting ACK.
	_ = delivery.Deliver(t.Context(), b.auth.project, content.ID)
	b.providers[0].mu.Lock()
	defer b.providers[0].mu.Unlock()
	if len(b.providers[0].sent) != 1 || b.births.Load() != 2 {
		t.Fatal("replay sent or birthed twice")
	}
}

func TestNativeWorkItemBridgeStaleOrForeignInvocationCannotReadReceipt(t *testing.T) {
	for _, change := range []string{"turn", "owner", "generation", "lease", "foreign-binding", "report-conflict"} {
		t.Run(change, func(t *testing.T) {
			b := newWorkBridgeFixture(t)
			b.providers[1].event(t, ports.ChatEventTurnStarted, "start", "turn", "native-turn")
			r := b.reporter(t)
			content := workBridgeContent()
			if _, err := r.Submit(t.Context(), content); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "turn":
				b.providers[1].event(t, ports.ChatEventTurnCompleted, "done", "turn", "native-turn")
			case "owner", "generation":
				item, _, err := b.store.GetWorkItem(t.Context(), b.command.WorkItemID())
				if err != nil {
					t.Fatal(err)
				}
				owner := item.Identity.OwnerSessionID
				if change == "owner" {
					owner = b.parent.ID
				}
				if _, err := b.store.ClaimWorkItemOwner(t.Context(), item, owner); err != nil {
					t.Fatal(err)
				}
			case "lease":
				if _, err := b.store.ReleaseWorkItemLease(t.Context(), b.binding.Identity, b.binding.Lease); err != nil {
					t.Fatal(err)
				}
			case "foreign-binding":
				foreign := b.binding
				foreign.Identity.OwnerSessionID = b.parent.ID
				other, err := b.service.BindWorkerReportNativeBridge(t.Context(), foreign, b.bindings, func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
					return reportactor.NewAcceptance(actor, b.store)
				})
				if err == nil {
					if got, err := other.Submit(t.Context(), content); err == nil || got.EventID != "" {
						t.Fatal("foreign binding accepted")
					}
				}
				return
			case "report-conflict":
				content.Type = "failure"
			}
			if got, err := r.Submit(t.Context(), content); err == nil || got.EventID != "" {
				t.Fatalf("stale/conflicting submission: %+v %v", got, err)
			}
			events, err := b.store.ListOrchestrationEvents(t.Context(), b.auth.project, 10)
			if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationPending || b.births.Load() != 2 {
				t.Fatalf("rejected invocation effects: %+v %v", events, err)
			}
		})
	}
}
