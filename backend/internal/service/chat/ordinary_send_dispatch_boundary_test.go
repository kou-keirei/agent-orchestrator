package chat_test

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func requireCommittedCallBoundary(t *testing.T, h *harness, client string) {
	t.Helper()
	m, found, err := h.st.ConversationMessageByClientID(t.Context(), h.ctrl.ConversationID(), client)
	if err != nil || !found {
		t.Fatalf("admission: %v %v", found, err)
	}
	a, err := h.st.OrdinarySendAttempt(t.Context(), m.TurnID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.st.OrdinarySendDispatchBoundary(t.Context(), a.ConversationSendOwner)
	if err != nil || b.State != domain.ConversationSendDispatchCallCommitted || b.CallCommittedAt == nil || b.AdmittedAt == nil ||
		b.SessionID != testSession || b.ControllerGeneration != h.ctrl.Generation() || b.ClientMessageID != client {
		t.Fatalf("native call lacks committed exact boundary: %+v %v", b, err)
	}
}

func TestOrdinarySendDispatchBoundaryDominatesEagerAndDeferred(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		name := "eager"
		if deferred {
			name = "deferred"
		}
		t.Run(name, func(t *testing.T) {
			p := newFakeConversation()
			var h *harness
			var native ports.ChatConversation = p
			starts := 0
			if deferred {
				native = &deferredConversation{fakeConversation: p, start: func(string) error {
					requireCommittedCallBoundary(t, h, "boundary-live")
					starts++
					return nil
				}}
			}
			h = newHarnessWithConversation(t, native)
			p.onSend = func(string) { requireCommittedCallBoundary(t, h, "boundary-live") }
			msg := ports.ChatUserMessage{Text: "exact original prompt", ClientMessageID: "boundary-live"}
			if _, err := h.svc.Send(t.Context(), testSession, msg); err != nil {
				t.Fatal(err)
			}
			if _, err := h.svc.Send(t.Context(), testSession, msg); err != nil {
				t.Fatal(err)
			}
			if p.sendCallCount() != 1 || deferred && starts != 1 {
				t.Fatalf("calls=%d starts=%d", p.sendCallCount(), starts)
			}
			requireCommittedCallBoundary(t, h, "boundary-live") // binding never becomes positive acceptance
		})
	}
}

type boundaryLostResult struct {
	*fakeConversation
	timeout bool
}

func (p boundaryLostResult) SendTurn(ctx context.Context, msg ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	_, _ = p.fakeConversation.SendTurn(ctx, msg)
	if p.timeout {
		return ports.ChatTurnRef{}, context.DeadlineExceeded
	}
	return ports.ChatTurnRef{}, nil
}

func TestOrdinarySendDispatchBoundaryUnknownNativeResultsNeverReplay(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "lost-reference"
		if timeout {
			name = "timeout-after-send"
		}
		t.Run(name, func(t *testing.T) {
			p := boundaryLostResult{fakeConversation: newFakeConversation(), timeout: timeout}
			h := newHarnessWithConversation(t, p)
			p.onSend = func(string) { requireCommittedCallBoundary(t, h, "boundary-unknown") }
			msg := ports.ChatUserMessage{Text: "unknown", ClientMessageID: "boundary-unknown"}
			for range 2 {
				_, err := h.svc.Send(t.Context(), testSession, msg)
				requireSafeSendUnknown(t, err)
			}
			if p.sendCallCount() != 1 {
				t.Fatal("unknown result replayed")
			}
			requireCommittedCallBoundary(t, h, msg.ClientMessageID)
		})
	}
}

func TestOrdinarySendDispatchBoundaryQueueDrain(t *testing.T) {
	p := newFakeConversation()
	h := newHarnessWithConversation(t, p)
	first, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "first", ClientMessageID: "boundary-first"})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "queued", ClientMessageID: "boundary-queued", Origin: domain.MessageOriginAutomation})
	if err != nil {
		t.Fatal(err)
	}
	a, err := h.st.OrdinarySendAttempt(t.Context(), queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	a.ControllerGeneration = h.ctrl.Generation()
	b, err := h.st.OrdinarySendDispatchBoundary(t.Context(), a.ConversationSendOwner)
	if err != nil || b.State != domain.ConversationSendDispatchNeverCalled {
		t.Fatalf("untouched queue: %+v %v", b, err)
	}
	p.onSend = func(string) { requireCommittedCallBoundary(t, h, "boundary-queued") }
	p.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: first.ProviderTurnID, TurnState: domain.TurnStateCompleted})
	h.awaitSnapshot(t, func(s store.ConversationSnapshot) bool {
		for _, turn := range s.Turns {
			if turn.ID == queued.ID && turn.ProviderTurnID != "" {
				return true
			}
		}
		return false
	})
	if p.sendCallCount() != 2 {
		t.Fatalf("calls=%d", p.sendCallCount())
	}
	requireCommittedCallBoundary(t, h, "boundary-queued")
}
