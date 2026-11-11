package chat_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func firstSendIdentity(t *testing.T, h *harness) chatsvc.FirstSendIdentity {
	t.Helper()
	ctx := t.Context()
	if err := lifecycle.New(h.st, nil).MarkSpawned(ctx, testSession, domain.SessionMetadata{
		ControllerGeneration: h.ctrl.Generation(), ProviderConversationID: h.ctrl.ProviderConversationID(),
	}); err != nil {
		t.Fatal(err)
	}
	rec, found, err := h.st.GetSession(ctx, testSession)
	if err != nil || !found {
		t.Fatalf("session: %v %v", found, err)
	}
	conversation, err := h.st.ConversationForSession(ctx, testSession)
	if err != nil {
		t.Fatal(err)
	}
	return chatsvc.FirstSendIdentity{ProjectID: testProject, SessionID: testSession, CreatedAt: rec.CreatedAt,
		Owner: rec.ControllerOwner(), ConversationID: conversation.ID, BranchID: conversation.ActiveBranchID,
		ClientMessageID: "original-first-send"}
}

func readFirstSend(t *testing.T, h *harness, i chatsvc.FirstSendIdentity) chatsvc.FirstSendAttestation {
	t.Helper()
	r, err := h.svc.BindFirstSendReadback(t.Context(), i)
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFirstSendAbsentAndRealPendingRemainUnknown(t *testing.T) {
	h := newHarness(t)
	i := firstSendIdentity(t, h)
	result := readFirstSend(t, h, i)
	if result.Disposition != chatsvc.FirstSendUnknown || result.Reason != chatsvc.FirstSendNoAdmission || result.JournalState != "" {
		t.Fatalf("absence became evidence: %+v", result)
	}
	// Real positive admission, not a mocked empty transcript. Pending still lacks
	// the immutable original request/generation binding required by this seam.
	created, err := h.st.AppendUserMessage(t.Context(), i.ConversationID, i.SessionID, i.Owner.ControllerGeneration,
		domain.ConversationMessage{ID: "first-message", Role: domain.MessageRoleUser, Origin: domain.MessageOriginHuman,
			Text: "original", ClientMessageID: i.ClientMessageID}, "first-turn", h.now())
	if err != nil || !created {
		t.Fatalf("append: %v %v", created, err)
	}
	result = readFirstSend(t, h, i)
	if result.Disposition != chatsvc.FirstSendUnknown || result.Reason != chatsvc.FirstSendAdmissionUnbound || result.JournalState != domain.ConversationSendPending {
		t.Fatalf("pending overclaimed immutable admission: %+v", result)
	}
	if h.conv.sendCallCount() != 0 {
		t.Fatal("readback sent pending work")
	}
}

type firstSendTimeoutConversation struct{ *fakeConversation }

func (c firstSendTimeoutConversation) SendTurn(ctx context.Context, msg ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	if _, err := c.fakeConversation.SendTurn(ctx, msg); err != nil {
		return ports.ChatTurnRef{}, err
	}
	return ports.ChatTurnRef{}, context.DeadlineExceeded
}

func TestFirstSendPostSendTimeoutRemainsUnknown(t *testing.T) {
	provider := firstSendTimeoutConversation{newFakeConversation()}
	h := newHarnessWithConversation(t, provider)
	i := firstSendIdentity(t, h)
	_, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "original", ClientMessageID: i.ClientMessageID})
	requireSafeSendUnknown(t, err)
	for range 2 {
		result := readFirstSend(t, h, i)
		if result.Disposition != chatsvc.FirstSendUnknown || result.Reason != chatsvc.FirstSendAttemptUncertain || result.JournalState != domain.ConversationSendReserved {
			t.Fatalf("lost ACK upgraded: %+v", result)
		}
	}
	if provider.sendCallCount() != 1 {
		t.Fatal("uncertain original was resent")
	}
}

func TestFirstSendBoundCorrelationDoesNotClaimNativeAcceptance(t *testing.T) {
	h := newHarness(t)
	i := firstSendIdentity(t, h)
	turn, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "original", ClientMessageID: i.ClientMessageID})
	if err != nil {
		t.Fatal(err)
	}
	if turn.ProviderTurnID == "" {
		t.Fatal("fixture has no provider correlation")
	}
	for range 2 {
		result := readFirstSend(t, h, i)
		if result.Disposition != chatsvc.FirstSendUnknown || result.Reason != chatsvc.FirstSendAcceptanceUnbound || result.JournalState != domain.ConversationSendBound {
			t.Fatalf("dispatch reference became native acceptance: %+v", result)
		}
	}
	message, found, err := h.st.ConversationMessageByClientID(t.Context(), i.ConversationID, i.ClientMessageID)
	if err != nil || !found || message.TurnID != turn.ID || h.conv.sendCallCount() != 1 {
		t.Fatalf("exact-once correlation changed: %+v %v %v", message, found, err)
	}
}

func TestFirstSendDeferredBindingBeforeNativeDispatchRemainsUnknown(t *testing.T) {
	provider := &deferredConversation{fakeConversation: newFakeConversation(), start: func(string) error {
		return context.DeadlineExceeded
	}}
	h := newHarnessWithConversation(t, provider)
	i := firstSendIdentity(t, h)
	if _, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "original", ClientMessageID: i.ClientMessageID}); err == nil {
		t.Fatal("fixture must fail after durable binding")
	}
	result := readFirstSend(t, h, i)
	if result.Disposition != chatsvc.FirstSendUnknown || result.Reason != chatsvc.FirstSendAcceptanceUnbound || result.JournalState != domain.ConversationSendBound {
		t.Fatalf("pre-native binding became acceptance: %+v", result)
	}
	if provider.sendCallCount() != 1 {
		t.Fatal("readback invoked deferred provider")
	}
}

func TestFirstSendBinderRejectsForeignOrIncompleteIdentity(t *testing.T) {
	h := newHarness(t)
	identity := firstSendIdentity(t, h)
	for name, mutate := range map[string]func(*chatsvc.FirstSendIdentity){
		"project":      func(i *chatsvc.FirstSendIdentity) { i.ProjectID = "foreign" },
		"session":      func(i *chatsvc.FirstSendIdentity) { i.SessionID = "foreign" },
		"creation":     func(i *chatsvc.FirstSendIdentity) { i.CreatedAt = i.CreatedAt.Add(1) },
		"generation":   func(i *chatsvc.FirstSendIdentity) { i.Owner.ControllerGeneration = "stale" },
		"provider":     func(i *chatsvc.FirstSendIdentity) { i.Owner.ProviderConversationID = "foreign" },
		"conversation": func(i *chatsvc.FirstSendIdentity) { i.ConversationID = "foreign" },
		"empty-client": func(i *chatsvc.FirstSendIdentity) { i.ClientMessageID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			i := identity
			mutate(&i)
			reader, err := h.svc.BindFirstSendReadback(t.Context(), i)
			if !errors.Is(err, chatsvc.ErrFirstSendIdentity) || reader != nil {
				t.Fatalf("foreign identity admitted: %v %v", reader, err)
			}
		})
	}
}

func TestFirstSendCapturedReaderHoldsOnOwnerOrHistoryDrift(t *testing.T) {
	for _, kind := range []string{"generation", "provider", "branch", "stopped"} {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t)
			i := firstSendIdentity(t, h)
			if kind == "branch" {
				i.BranchID = "foreign"
			}
			r, err := h.svc.BindFirstSendReadback(t.Context(), i)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "generation":
				err = h.st.ClaimChatControllerGeneration(t.Context(), testSession, "replacement")
			case "provider":
				rec, _, readErr := h.st.GetSession(t.Context(), testSession)
				if readErr != nil {
					t.Fatal(readErr)
				}
				rec.Metadata.ProviderConversationID = "foreign"
				err = h.st.UpdateSession(t.Context(), rec)
			case "stopped":
				err = h.svc.Stop(t.Context(), testSession)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := r.Read(t.Context())
			if err != nil || result.Disposition != chatsvc.FirstSendHold || result.JournalState != "" {
				t.Fatalf("drift exposed/approved readback: %+v %v", result, err)
			}
		})
	}
}

type firstSendMutationStore struct {
	*sqlite.Store
	afterRead func()
}

func (s *firstSendMutationStore) OrdinarySendAttempt(ctx context.Context, turnID string) (domain.ConversationSendAttempt, error) {
	attempt, err := s.Store.OrdinarySendAttempt(ctx, turnID)
	if s.afterRead != nil {
		s.afterRead()
	}
	return attempt, err
}

func TestFirstSendConcurrentMutationCannotUpgradeSnapshot(t *testing.T) {
	var wrapped *firstSendMutationStore
	h := newHarnessWithConversationAndStore(t, nil, func(st *sqlite.Store) chatsvc.Store {
		wrapped = &firstSendMutationStore{Store: st}
		return wrapped
	})
	i := firstSendIdentity(t, h)
	if _, err := h.svc.Send(t.Context(), testSession, ports.ChatUserMessage{Text: "original", ClientMessageID: i.ClientMessageID}); err != nil {
		t.Fatal(err)
	}
	r, err := h.svc.BindFirstSendReadback(t.Context(), i)
	if err != nil {
		t.Fatal(err)
	}
	// Deterministically interleave a durable owner mutation after journal read.
	wrapped.afterRead = func() {
		if err := h.st.ClaimChatControllerGeneration(t.Context(), testSession, "replacement"); err != nil {
			t.Fatal(err)
		}
	}
	result, err := r.Read(t.Context())
	if err != nil || result.Disposition != chatsvc.FirstSendHold || result.JournalState != "" {
		t.Fatalf("mixed snapshot upgraded: %+v %v", result, err)
	}
}

func TestFirstSendJournalMutationCannotCertifyNeverAttempted(t *testing.T) {
	var wrapped *firstSendMutationStore
	h := newHarnessWithConversationAndStore(t, nil, func(st *sqlite.Store) chatsvc.Store {
		wrapped = &firstSendMutationStore{Store: st}
		return wrapped
	})
	i := firstSendIdentity(t, h)
	created, err := h.st.AppendUserMessage(t.Context(), i.ConversationID, i.SessionID, i.Owner.ControllerGeneration,
		domain.ConversationMessage{ID: "first-message", Role: domain.MessageRoleUser, Origin: domain.MessageOriginHuman,
			Text: "original", ClientMessageID: i.ClientMessageID}, "first-turn", h.now())
	if err != nil || !created {
		t.Fatalf("append: %v %v", created, err)
	}
	wrapped.afterRead = func() {
		_, err := h.st.ReserveOrdinarySend(t.Context(), domain.ConversationSendOwner{
			ConversationID: i.ConversationID, TurnID: "first-turn", ClientMessageID: i.ClientMessageID,
			BranchID: i.BranchID, SessionID: i.SessionID, ControllerGeneration: i.Owner.ControllerGeneration,
		}, h.now())
		if err != nil {
			t.Fatal(err)
		}
	}
	result := readFirstSend(t, h, i)
	if result.Disposition != chatsvc.FirstSendUnknown || result.Reason != chatsvc.FirstSendAdmissionUnbound {
		t.Fatalf("stale pending snapshot granted negative proof: %+v", result)
	}
	attempt, err := h.st.OrdinarySendAttempt(t.Context(), "first-turn")
	if err != nil || attempt.State != domain.ConversationSendReserved {
		t.Fatalf("fixture did not reserve: %+v %v", attempt, err)
	}
}

func TestFirstSendCanceledReadRemainsTypedUnknown(t *testing.T) {
	h := newHarness(t)
	r, err := h.svc.BindFirstSendReadback(t.Context(), firstSendIdentity(t, h))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := r.Read(ctx)
	if !errors.Is(err, context.Canceled) || result.Disposition != chatsvc.FirstSendUnknown {
		t.Fatalf("cancel: %+v %v", result, err)
	}
}
