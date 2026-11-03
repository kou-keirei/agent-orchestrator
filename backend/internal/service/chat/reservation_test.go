package chat_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

type safeSendUnknown struct{}

func (safeSendUnknown) Error() string         { return "reference lost" }
func (safeSendUnknown) DeliveryUnknown() bool { return true }

func isReconnectUnknown(err error) bool {
	var unknown interface{ DeliveryUnknown() bool }
	return errors.Is(err, domain.ErrConversationSendUncertain) || errors.As(err, &unknown) && unknown.DeliveryUnknown()
}

func requireSafeSendUnknown(t *testing.T, err error) {
	t.Helper()
	if !isReconnectUnknown(err) {
		t.Errorf("expected typed uncertainty: %v", err)
	}
}

type safeSendWithoutRef struct{ *fakeConversation }

func (c safeSendWithoutRef) SendTurn(ctx context.Context, msg ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	if _, err := c.fakeConversation.SendTurn(ctx, msg); err != nil {
		return ports.ChatTurnRef{}, err
	}
	return ports.ChatTurnRef{}, fmt.Errorf("reference lost: %w", safeSendUnknown{})
}

type safeSendBindingStore struct {
	*sqlite.Store
	fail  atomic.Bool
	cause error
}

func (s *safeSendBindingStore) BindTurnToProvider(ctx context.Context, turnID, providerTurnID string, now time.Time) error {
	if s.fail.Load() {
		return s.cause
	}
	return s.Store.BindTurnToProvider(ctx, turnID, providerTurnID, now)
}

// Accepted main predates W449's unknown-result handling. Preserve the same
// queued/no-ref outage window when that version tries to settle the lost ACK.
func (s *safeSendBindingStore) SettleTurnByID(ctx context.Context, turnID string, state domain.TurnState, message string, now time.Time) error {
	if s.fail.Load() {
		return s.cause
	}
	return s.Store.SettleTurnByID(ctx, turnID, state, message, now)
}

func (s *safeSendBindingStore) BindReservedOrdinarySend(ctx context.Context, owner domain.ConversationSendOwner, providerTurnID string, now time.Time) error {
	if s.fail.Load() {
		return s.cause
	}
	return s.Store.BindReservedOrdinarySend(ctx, owner, providerTurnID, now)
}

type reservationFailureStore struct{ *sqlite.Store }

func (s reservationFailureStore) ReserveOrdinarySend(context.Context, domain.ConversationSendOwner, time.Time) (domain.QueuedTurn, error) {
	return domain.QueuedTurn{}, errors.New("reservation commit unavailable")
}

func TestOrdinarySendReservationFailureMakesZeroProviderCalls(t *testing.T) {
	provider := newFakeConversation()
	h := newHarnessWithConversationAndStore(t, provider, func(st *sqlite.Store) chatsvc.Store { return reservationFailureStore{st} })
	if _, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "do not deliver", ClientMessageID: "persist-failed"}); err == nil {
		t.Fatal("reservation failure acknowledged")
	}
	if provider.sendCallCount() != 0 {
		t.Fatal("provider called before reservation committed")
	}
	snapshot, err := h.st.LoadConversationSnapshot(t.Context(), h.ctrl.ConversationID())
	if err != nil || len(snapshot.Turns) != 1 {
		t.Fatalf("admission: %+v %v", snapshot, err)
	}
	attempt, err := h.st.OrdinarySendAttempt(t.Context(), snapshot.Turns[0].ID)
	if err != nil || attempt.State != domain.ConversationSendPending {
		t.Fatalf("failed reservation changed attempt: %+v %v", attempt, err)
	}
}

func TestOrdinarySendCommitsExactOwnerBeforeProviderCall(t *testing.T) {
	provider := newFakeConversation()
	h := newHarnessWithConversation(t, provider)
	provider.onSend = func(string) {
		message, found, err := h.st.ConversationMessageByClientID(t.Context(), h.ctrl.ConversationID(), "owner-message")
		if err != nil || !found {
			t.Fatalf("message at provider: %v %v", found, err)
		}
		attempt, err := h.st.OrdinarySendAttempt(t.Context(), message.TurnID)
		turn, turnErr := h.st.TurnByID(t.Context(), message.TurnID)
		if err != nil || turnErr != nil || attempt.State != domain.ConversationSendReserved || attempt.ReservedAt == nil ||
			attempt.ConversationID != h.ctrl.ConversationID() || attempt.TurnID != turn.ID ||
			attempt.SessionID != testSession || attempt.BranchID != turn.BranchID ||
			attempt.ControllerGeneration != h.ctrl.Generation() || attempt.ClientMessageID != "owner-message" {
			t.Fatalf("uncommitted or foreign owner: %+v %+v %v %v", attempt, turn, err, turnErr)
		}
	}
	turn, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "durable", ClientMessageID: "owner-message"})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := h.st.OrdinarySendAttempt(t.Context(), turn.ID)
	if err != nil || attempt.State != domain.ConversationSendBound {
		t.Fatalf("provider proof not bound: %+v %v", attempt, err)
	}
}

func TestOrdinarySendStaleGenerationCannotSendOrResolve(t *testing.T) {
	for _, replaceDuringSend := range []bool{false, true} {
		t.Run(fmt.Sprint(replaceDuringSend), func(t *testing.T) {
			provider := newFakeConversation()
			h := newHarnessWithConversation(t, provider)
			replace := func() {
				if err := h.st.ClaimChatControllerGeneration(t.Context(), testSession, "successor"); err != nil {
					t.Fatal(err)
				}
			}
			if replaceDuringSend {
				provider.onSend = func(string) { replace() }
			} else {
				replace()
			}
			_, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "stale", ClientMessageID: "stale-request"})
			if err == nil {
				t.Fatal("stale owner acknowledged")
			}
			want := 0
			if replaceDuringSend {
				want = 1
				requireSafeSendUnknown(t, err)
			}
			if provider.sendCallCount() != want {
				t.Fatalf("calls=%d want=%d", provider.sendCallCount(), want)
			}
			message, found, err := h.st.ConversationMessageByClientID(t.Context(), h.ctrl.ConversationID(), "stale-request")
			if err != nil || !found {
				t.Fatalf("message: %v %v", found, err)
			}
			attempt, err := h.st.OrdinarySendAttempt(t.Context(), message.TurnID)
			state := domain.ConversationSendPending
			if replaceDuringSend {
				state = domain.ConversationSendReserved
			}
			if err != nil || attempt.State != state {
				t.Fatalf("attempt: %+v %v", attempt, err)
			}
		})
	}
}

type nonDeliveryError struct{ evidence string }

func (e nonDeliveryError) Error() string               { return "native request refused before delivery" }
func (e nonDeliveryError) NonDeliveryEvidence() string { return e.evidence }

func TestOrdinarySendNonDeliveryRequiresCommittedCorrelatedEvidence(t *testing.T) {
	for _, evidence := range []string{"", "native-refusal-for-this-request"} {
		t.Run(fmt.Sprint(evidence != ""), func(t *testing.T) {
			provider := newFakeConversation()
			provider.sendErr = nonDeliveryError{evidence}
			h := newHarnessWithConversation(t, provider)
			_, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "refused", ClientMessageID: "refused-request"})
			if err == nil {
				t.Fatal("refusal acknowledged as success")
			}
			message, _, loadErr := h.st.ConversationMessageByClientID(t.Context(), h.ctrl.ConversationID(), "refused-request")
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			attempt, loadErr := h.st.OrdinarySendAttempt(t.Context(), message.TurnID)
			state := domain.ConversationSendNotDelivered
			if evidence == "" {
				state = domain.ConversationSendReserved
				requireSafeSendUnknown(t, err)
			}
			if loadErr != nil || attempt.State != state {
				t.Fatalf("resolution: %+v %v", attempt, loadErr)
			}
			provider.sendErr = nil
			_, err = h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "next", ClientMessageID: "next-request"})
			if evidence == "" {
				requireSafeSendUnknown(t, err)
			} else if err != nil {
				t.Fatal(err)
			}
			want := 1
			if evidence != "" {
				want = 2
			}
			if provider.sendCallCount() != want {
				t.Fatalf("calls=%d want=%d", provider.sendCallCount(), want)
			}
		})
	}
}

func TestOrdinarySendKnownReferenceBindingFailureStaysReserved(t *testing.T) {
	provider := newFakeConversation()
	h := newHarnessWithConversationAndStore(t, provider, func(st *sqlite.Store) chatsvc.Store {
		s := &safeSendBindingStore{Store: st, cause: errors.New("binding commit failed")}
		s.fail.Store(true)
		return s
	})
	_, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "lost binding", ClientMessageID: "binding-request"})
	requireSafeSendUnknown(t, err)
	_, err = h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "new request", ClientMessageID: "binding-B"})
	requireSafeSendUnknown(t, err)
	if provider.sendCallCount() != 1 {
		t.Fatal("binding failure allowed another provider call")
	}
}

func TestOrdinarySendUnrelatedNativeTurnCannotAuthorizeG1(t *testing.T) {
	provider := safeSendWithoutRef{newFakeConversation()}
	h := newHarnessWithConversation(t, provider)
	_, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "lost ACK", ClientMessageID: "uncertain-A"})
	requireSafeSendUnknown(t, err)
	provider.emit(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderTurnID: "unrelated-native-turn", ProviderConversationID: "thread-1"})
	h.awaitSnapshot(t, func(s store.ConversationSnapshot) bool {
		for _, turn := range s.Turns {
			if turn.ProviderTurnID == "unrelated-native-turn" {
				return true
			}
		}
		return false
	})
	_, err = h.svc.SteerOrSend(t.Context(), testSession, ports.ChatUserMessage{Text: "B must wait", ClientMessageID: "uncertain-B"}, false)
	requireSafeSendUnknown(t, err)
	_, err = h.svc.Steer(t.Context(), testSession, ports.ChatUserMessage{Text: "guidance must wait", ClientMessageID: "uncertain-steer"})
	requireSafeSendUnknown(t, err)
	if provider.sendCallCount() != 1 {
		t.Fatal("unrelated native lifecycle released the unresolved attempt")
	}
}

type emptySendRefConversation struct{ *fakeConversation }

func (c emptySendRefConversation) SendTurn(ctx context.Context, msg ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	_, err := c.fakeConversation.SendTurn(ctx, msg)
	return ports.ChatTurnRef{}, err
}

func TestOrdinarySendEmptySuccessIsNotDeliveryProof(t *testing.T) {
	provider := emptySendRefConversation{newFakeConversation()}
	h := newHarnessWithConversation(t, provider)
	message := ports.ChatUserMessage{Text: "no receipt", ClientMessageID: "empty-receipt"}
	_, err := h.svc.Send(t.Context(), testSession, message)
	requireSafeSendUnknown(t, err)
	_, err = h.svc.Send(t.Context(), testSession, message)
	requireSafeSendUnknown(t, err)
	if provider.sendCallCount() != 1 {
		t.Fatal("empty reference replayed")
	}
}
