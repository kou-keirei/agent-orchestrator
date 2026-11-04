package chat_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestExternalChatWritersRequireInputAdmission(t *testing.T) {
	for _, action := range []string{"send", "relay", "relay-with-id", "steer", "steer-or-send", "promote", "settings", "config", "retry", "compact", "reload-mcp"} {
		t.Run(action, func(t *testing.T) {
			provider := &modeConversation{fakeConversation: newFakeConversation(), mode: "build"}
			h := newAdmissionHarness(t, provider)
			calls := 0
			h.svc.SetInputAdmission(func(ctx context.Context, id domain.SessionID, generation string) (func(), error) {
				calls++
				if id != testSession || generation != h.ctrl.Generation() {
					t.Fatalf("wrong admission owner: %s %s", id, generation)
				}
				return nil, ports.ErrChatInputBlocked
			})
			ctx := t.Context()
			var err error
			switch action {
			case "send":
				_, err = h.svc.Send(ctx, testSession, ports.ChatUserMessage{Text: "peer"})
			case "relay":
				_, err = h.svc.RelayChatTurn(ctx, testSession, "peer")
			case "relay-with-id":
				_, err = h.svc.RelayChatTurnWithID(ctx, testSession, "peer", "peer-retry")
			case "steer":
				_, err = h.svc.Steer(ctx, testSession, ports.ChatUserMessage{Text: "peer"})
			case "steer-or-send":
				_, err = h.svc.SteerOrSend(ctx, testSession, ports.ChatUserMessage{Text: "peer", ClientMessageID: "peer"}, false)
			case "promote":
				_, err = h.svc.PromoteQueuedTurn(ctx, testSession, "queued")
			case "settings":
				_, err = h.svc.SetTurnSettings(ctx, testSession, domain.ConversationSettings{Model: "new-model"})
			case "config":
				_, err = h.svc.SetConfigOption(ctx, testSession, "mode", ports.ChatConfigOptionValue{Select: "plan"})
			case "retry":
				_, err = h.svc.RetryTurn(ctx, testSession, "failed-turn")
			case "compact":
				_, err = h.svc.Compact(ctx, testSession)
			case "reload-mcp":
				_, err = h.svc.ReloadMCPServers(ctx, testSession)
			}
			if !errors.Is(err, ports.ErrChatInputBlocked) || calls != 1 {
				t.Fatalf("admission calls=%d error=%v", calls, err)
			}
			if len(provider.sentMessages()) != 0 || provider.mode != "build" || h.ctrl.Settings().Model == "new-model" {
				t.Fatal("refused writer changed provider or settings")
			}
		})
	}
}

func TestRelayCustomAdmissionHoldsItsLeaseWithoutOrdinaryAdmission(t *testing.T) {
	h := newAdmissionHarness(t, newFakeConversation())
	h.svc.SetInputAdmission(func(context.Context, domain.SessionID, string) (func(), error) {
		t.Fatal("custom relay reacquired ordinary admission")
		return nil, ports.ErrChatInputBlocked
	})
	held, admissions, releases := false, 0, 0
	h.conv.onSend = func(string) {
		if !held {
			t.Error("custom admission lease released before provider delivery")
		}
	}
	_, err := h.svc.RelayChatTurnWithAdmission(t.Context(), testSession, "automation", "automation-key", func() (func(), error) {
		admissions++
		held = true
		return func() { held = false; releases++ }, nil
	})
	if err != nil || admissions != 1 || releases != 1 || held || len(h.conv.sentMessages()) != 1 {
		t.Fatalf("custom relay: err=%v admissions=%d releases=%d held=%v sends=%d", err, admissions, releases, held, len(h.conv.sentMessages()))
	}
}

// Mirror production ownership publication for admission-boundary tests.
func newAdmissionHarness(t *testing.T, provider ports.ChatConversation) *harness {
	svc, _, st, _, ctrl := w168AdmissionFixture(t, provider)
	h := &harness{svc: svc, st: st, ctrl: ctrl}
	if p, ok := provider.(*fakeConversation); ok {
		h.conv = p
	}
	return h
}
