package chat_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

type reconnectUnknownConversation struct{ safeSendWithoutRef }

func (reconnectUnknownConversation) PreservesProviderOnClose() bool { return true }

// Replace the service as well as the controller: no volatile request ownership
// from the detached daemon is available to the live reconnect path.
func reconnectSafeSendService(t *testing.T, h *harness, provider *fakeConversation) (*chatsvc.Service, *chatsvc.Controller) {
	t.Helper()
	ctx := context.Background()
	h.svc.StopAll(ctx)
	var ids atomic.Int32
	svc := chatsvc.New(chatsvc.Options{
		Store: h.st, Reader: fullSnapshotReader(h.st), Sessions: h.st,
		Drivers: fakeRegistry{driver: fakeDriver{conv: &liveReconnectedConversation{
			nativeHistoryConversation: &nativeHistoryConversation{fakeConversation: provider},
		}}},
		NewID: func() string { return fmt.Sprintf("replacement-%d", ids.Add(1)) },
	})
	t.Cleanup(func() { svc.StopAll(ctx) })
	ctrl, err := svc.Start(ctx, chatsvc.StartConfig{
		SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex,
		WorkspacePath: t.TempDir(), ProviderConversationID: "thread-1",
		ControllerReady: func(result chatsvc.StartResult) (chatsvc.ControllerCommit, error) {
			if !result.LiveReconnect || result.ProviderConversationID != "thread-1" ||
				result.ControllerGeneration == h.ctrl.Generation() {
				t.Fatalf("replacement lost native identity or generation fence: %+v", result)
			}
			return chatsvc.ControllerCommit{}, nil
		},
	})
	if err != nil {
		// A conclusive typed recovery hold is also safe. The controls below
		// independently require an ordinary healthy reconnect to succeed.
		if errors.Is(err, ports.ErrChatRecoveryInconclusive) || isReconnectUnknown(err) {
			return svc, nil
		}
		t.Fatalf("replacement Start: %v", err)
	}
	if ctrl == h.ctrl || ctrl.ConversationID() != h.ctrl.ConversationID() || ctrl.Generation() == h.ctrl.Generation() {
		t.Fatal("test did not replace the controller of the same conversation")
	}
	return svc, ctrl
}

func TestSafeSendLiveReconnectPreservesUnboundUnknown(t *testing.T) {
	ctx := context.Background()
	first := reconnectUnknownConversation{safeSendWithoutRef{newFakeConversation()}}
	var binding *safeSendBindingStore
	h := newHarnessWithConversationAndStore(t, first, func(st *sqlite.Store) chatsvc.Store {
		binding = &safeSendBindingStore{Store: st, cause: errors.New("binding unavailable")}
		binding.fail.Store(true)
		return binding
	})
	message := ports.ChatUserMessage{Text: "execute once across daemon reconnect", ClientMessageID: "reconnect-A"}
	var atProviderSend store.ConversationSnapshot
	first.onSend = func(string) {
		var err error
		atProviderSend, err = h.st.LoadConversationSnapshot(ctx, h.ctrl.ConversationID())
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := h.svc.Send(ctx, testSession, message)
	requireSafeSendUnknown(t, err)
	before, err := h.st.LoadConversationSnapshot(ctx, h.ctrl.ConversationID())
	if err != nil || len(before.Turns) != 1 || len(before.Messages) != 1 {
		t.Fatalf("original request missing: %+v, %v", before, err)
	}
	original := before.Turns[0]
	if original.State != domain.TurnStateQueued || original.ProviderTurnID != "" ||
		before.Messages[0].ClientMessageID != message.ClientMessageID || before.Messages[0].TurnID != original.ID {
		t.Fatalf("invalid uncertain-binding fixture: %+v", before)
	}
	if !reflect.DeepEqual(atProviderSend, before) {
		t.Fatal("fixture unexpectedly persisted a dispatch/result marker")
	}
	second := newFakeConversation()
	second.turnSeq = 10 // Distinct native work cannot correlate the lost A reference.
	svc, ctrl := reconnectSafeSendService(t, h, second)
	after, err := h.st.TurnByID(ctx, original.ID)
	if err != nil || !reflect.DeepEqual(original, after) {
		t.Fatalf("replacement changed the original uncertain row: %+v, %v", after, err)
	}
	stored, found, err := h.st.ConversationMessageByClientID(ctx, h.ctrl.ConversationID(), message.ClientMessageID)
	if err != nil || !found || !reflect.DeepEqual(before.Messages[0], stored) {
		t.Fatalf("replacement changed original client identity: %+v, found=%v, %v", stored, found, err)
	}
	if ctrl == nil {
		if len(first.sentMessages()) != 1 || len(second.sentMessages()) != 0 || svc.HasLiveChatController(testSession) {
			t.Fatal("recovery hold sent work or published an unsafe controller")
		}
		return
	}
	_, err = svc.Send(ctx, testSession, message)
	if !isReconnectUnknown(err) {
		t.Errorf("ordinary same-ID recovery falsely acknowledged A: %v", err)
	}
	recovered, err := svc.SteerOrSend(ctx, testSession, message, true)
	if !isReconnectUnknown(err) {
		t.Errorf("G1 same-ID recovery falsely acknowledged A: %+v, %v", recovered, err)
	}
	result, err := svc.SteerOrSend(ctx, testSession, ports.ChatUserMessage{Text: "B must wait", ClientMessageID: "reconnect-B"}, false)
	if err != nil && !isReconnectUnknown(err) && !errors.Is(err, chatsvc.ErrTurnRunning) {
		t.Errorf("B refusal lost typed uncertainty/busy semantics: %v", err)
	} else if err == nil && result.Turn.State != domain.TurnStateQueued {
		t.Errorf("B escaped queue admission: %+v", result)
	}
	if len(first.sentMessages())+len(second.sentMessages()) != 1 {
		t.Errorf("replacement dispatched another prompt: original=%d replacement=%d", len(first.sentMessages()), len(second.sentMessages()))
	}
	// Exercise actual projection-driven drain, whether B was refused or escaped.
	second.emit(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderTurnID: "provider-turn-11", ProviderConversationID: "thread-1"})
	second.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: "provider-turn-11", ProviderConversationID: "thread-1", TurnState: domain.TurnStateCompleted})
	// Cold is an explicit end-of-stream barrier distinct from the replacement's
	// initial ready/busy state, so checking it cannot race the preceding drain.
	second.emit(ports.ChatEvent{Kind: ports.ChatEventControllerState, ControllerState: ports.ChatControllerCold})
	deadline := time.Now().Add(5 * time.Second)
	for ctrl.State() != ports.ChatControllerCold && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ctrl.State() != ports.ChatControllerCold {
		t.Fatal("replacement did not project the lifecycle/drain barrier")
	}
	if len(second.sentMessages()) != 0 {
		t.Errorf("live completion/drain sent work over uncertain A: %+v", second.sentMessages())
	}
	after, err = h.st.TurnByID(ctx, original.ID)
	if err != nil || !reflect.DeepEqual(original, after) {
		t.Errorf("drain rebound or changed uncertain A: %+v, %v", after, err)
	}
}

func TestSafeSendLiveReconnectPreservesUntouchedQueue(t *testing.T) {
	ctx := context.Background()
	first := &terminatingConversation{fakeConversation: newFakeConversation()}
	h := newHarnessWithConversation(t, first)
	message := ports.ChatUserMessage{Text: "admitted before daemon disconnected", ClientMessageID: "untouched-queue"}
	// This is the actual ordinary enqueue transaction, with the daemon stopping
	// before dispatch. It must not be mistaken for a provider delivery attempt.
	created, err := h.st.AppendUserMessage(ctx, h.ctrl.ConversationID(), testSession, h.ctrl.Generation(), domain.ConversationMessage{
		ID: "untouched-message", Text: message.Text, Origin: domain.MessageOriginHuman, ClientMessageID: message.ClientMessageID,
	}, "untouched-turn", h.now())
	if err != nil || !created || len(first.sentMessages()) != 0 {
		t.Fatalf("untouched queue fixture: created=%v, %v", created, err)
	}
	second := newFakeConversation()
	svc, ctrl := reconnectSafeSendService(t, h, second)
	if ctrl == nil {
		t.Fatal("untouched queue must not cause a blanket reconnect hold")
	}
	if _, err := svc.Send(ctx, testSession, message); err != nil {
		t.Fatalf("ordinary untouched queue recovery: %v", err)
	}
	recovered, err := svc.SteerOrSend(ctx, testSession, message, true)
	if err != nil || !recovered.Duplicate || recovered.Turn.ID != "untouched-turn" || recovered.Turn.State != domain.TurnStateQueued {
		t.Fatalf("untouched queue admission lost: %+v, %v", recovered, err)
	}
	if len(second.sentMessages()) != 0 {
		t.Fatal("queue recovery unexpectedly dispatched")
	}
	second.emit(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderTurnID: "surviving-native-work", ProviderConversationID: "thread-1"})
	second.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: "surviving-native-work", ProviderConversationID: "thread-1", TurnState: domain.TurnStateCompleted})
	h.awaitSnapshot(t, func(s store.ConversationSnapshot) bool {
		for _, turn := range s.Turns {
			if turn.ID == "untouched-turn" {
				return turn.State == domain.TurnStateRunning
			}
		}
		return false
	})
	if sent := second.sentMessages(); len(sent) != 1 || sent[0].ClientMessageID != message.ClientMessageID {
		t.Fatalf("untouched queue did not drain exactly once: %+v", sent)
	}
}

func TestSafeSendLiveReconnectPreservesFreshQueue(t *testing.T) {
	ctx := context.Background()
	first := &terminatingConversation{fakeConversation: newFakeConversation()}
	h := newHarnessWithConversation(t, first)
	active, err := h.svc.Send(ctx, testSession, ports.ChatUserMessage{Text: "healthy running work"})
	if err != nil {
		t.Fatal(err)
	}
	first.emit(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderTurnID: active.ProviderTurnID, ProviderConversationID: "thread-1"})
	h.awaitSnapshot(t, func(s store.ConversationSnapshot) bool {
		return len(s.Turns) == 1 && s.Turns[0].State == domain.TurnStateRunning
	})
	message := ports.ChatUserMessage{Text: "legitimately queued", ClientMessageID: "fresh-queue"}
	queued, err := h.svc.Send(ctx, testSession, message)
	if err != nil || queued.State != domain.TurnStateQueued || len(first.sentMessages()) != 1 {
		t.Fatalf("fresh queue admission: %+v, %v", queued, err)
	}
	second := newFakeConversation()
	second.turnSeq = 10
	svc, ctrl := reconnectSafeSendService(t, h, second)
	if ctrl == nil {
		t.Fatal("healthy running turn and untouched queue must reconnect")
	}
	recovered, err := svc.SteerOrSend(ctx, testSession, message, true)
	if err != nil || !recovered.Duplicate || recovered.Turn.ID != queued.ID || recovered.Turn.State != domain.TurnStateQueued {
		t.Fatalf("legitimate queue admission lost: %+v, %v", recovered, err)
	}
	if len(second.sentMessages()) != 0 {
		t.Fatal("fresh queue sent before surviving active turn completed")
	}
	second.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: active.ProviderTurnID, ProviderConversationID: "thread-1", TurnState: domain.TurnStateCompleted})
	h.awaitSnapshot(t, func(s store.ConversationSnapshot) bool {
		for _, turn := range s.Turns {
			if turn.ID == queued.ID {
				return turn.State == domain.TurnStateRunning
			}
		}
		return false
	})
	sent := second.sentMessages()
	if len(sent) != 1 || sent[0].ClientMessageID != message.ClientMessageID || sent[0].Text != message.Text {
		t.Fatalf("fresh queued request did not drain exactly once: %+v", sent)
	}
}
