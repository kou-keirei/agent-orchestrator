package chat_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type deferredRelayConversation struct {
	*fakeConversation
	wait func(context.Context, string) error
}

func (*deferredRelayConversation) StartDeferredTurn(string) error { return nil }
func (*deferredRelayConversation) DiscardDeferredTurn(string)     {}
func (c *deferredRelayConversation) WaitTurnAccepted(ctx context.Context, id string) error {
	return c.wait(ctx, id)
}

func TestSwitchRelayRequiresAcceptanceAndCurrentOwner(t *testing.T) {
	for _, outcome := range []string{"accepted", "rejected", "cancelled", "owner-lost", "stale-generation", "wrong-provider", "switch-refused", "switch-lost", "missing-authority"} {
		t.Run(outcome, func(t *testing.T) {
			provider := &deferredRelayConversation{fakeConversation: newFakeConversation()}
			h := newAdmissionHarness(t, provider)
			rec, _, err := h.st.GetSession(t.Context(), testSession)
			if err != nil {
				t.Fatal(err)
			}
			rec.Metadata.ProviderConversationID = provider.ProviderConversationID()
			if err := h.st.UpdateSession(t.Context(), rec); err != nil {
				t.Fatal(err)
			}
			h.svc.SetInputAdmission(func(context.Context, domain.SessionID, string) (func(), error) {
				t.Fatal("switch relay reacquired ordinary input admission")
				return nil, ports.ErrChatInputBlocked
			})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			waits := 0
			provider.wait = func(ctx context.Context, id string) error {
				waits++
				if id != "provider-turn-1" {
					t.Fatalf("uncorrelated acceptance wait %q", id)
				}
				switch outcome {
				case "rejected":
					return errors.New("native rejection")
				case "cancelled":
					cancel()
					return ctx.Err()
				case "owner-lost":
					rec.Metadata.ControllerGeneration = "new-owner"
					return h.st.UpdateSession(ctx, rec)
				}
				return nil
			}
			generation := h.ctrl.Generation()
			if outcome == "stale-generation" {
				generation = "stale"
			}
			if outcome == "wrong-provider" {
				rec.Metadata.ProviderConversationID = "other-history"
				if err := h.st.UpdateSession(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			checks := 0
			validate := func() error {
				checks++
				if outcome == "switch-refused" || (outcome == "switch-lost" && checks > 1) {
					return ports.ErrChatInputBlocked
				}
				return nil
			}
			if outcome == "missing-authority" {
				validate = nil
			}
			_, err = h.svc.RelayChatTurnAccepted(ctx, testSession, "continue", "activation", generation, validate)
			if (err == nil) != (outcome == "accepted") {
				t.Fatalf("outcome=%s error=%v", outcome, err)
			}
			wantCalls := 1
			if outcome == "stale-generation" || outcome == "wrong-provider" || outcome == "switch-refused" || outcome == "missing-authority" {
				wantCalls = 0
			}
			if waits != wantCalls || len(provider.sentMessages()) != wantCalls {
				t.Fatalf("waits=%d sends=%d want=%d", waits, len(provider.sentMessages()), wantCalls)
			}
			if outcome == "accepted" || outcome == "rejected" || outcome == "cancelled" {
				// A lost caller ACK never authorizes a second native prompt. A
				// durable/local duplicate also cannot manufacture acceptance.
				_, retryErr := h.svc.RelayChatTurnAccepted(t.Context(), testSession, "continue", "activation", generation, func() error { return nil })
				if !errors.Is(retryErr, domain.ErrConversationSendUncertain) || len(provider.sentMessages()) != 1 || waits != 1 {
					t.Fatalf("retry redelivered or acknowledged: error=%v sends=%d waits=%d", retryErr, len(provider.sentMessages()), waits)
				}
			}
		})
	}
}
