package acp

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestDeferredPromptAcceptanceRequiresNativeResponse(t *testing.T) {
	for _, outcome := range []string{"accepted", "rejected", "metadata-refusal"} {
		rejected := outcome != "accepted"
		t.Run(outcome, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			agent := &fakeAgent{customPrompt: func(ctx context.Context, _ acpsdk.PromptRequest) (acpsdk.PromptResponse, error) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return acpsdk.PromptResponse{}, ctx.Err()
				}
				if outcome == "rejected" {
					return acpsdk.PromptResponse{}, acpsdk.NewAuthRequired(nil)
				}
				if outcome == "metadata-refusal" {
					return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonEndTurn, Meta: testPromptFailureMeta(map[string]any{
						"id": "denied", "revision": 1, "category": "access", "severity": "error", "title": "Access denied",
					})}, nil
				}
				return acpsdk.PromptResponse{StopReason: acpsdk.StopReasonEndTurn}, nil
			}}
			driver := New(Config{Harness: domain.HarnessClaudeCode,
				Launch: func(context.Context, LaunchConfig) (Launch, error) { return Launch{Command: "fake"}, nil },
			}, slog.New(slog.DiscardHandler))
			driver.useTestProcess(fakeSpawn(agent))
			opened, err := driver.Start(t.Context(), ports.ChatStartConfig{WorkspacePath: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			ref, err := opened.SendTurn(t.Context(), ports.ChatUserMessage{Text: "activation"})
			if err != nil {
				t.Fatal(err)
			}
			acceptance := opened.(ports.ChatTurnAcceptance)
			// A valid local id is still unaccepted before StartDeferredTurn.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := acceptance.WaitTurnAccepted(ctx, ref.ProviderTurnID); !errors.Is(err, context.Canceled) {
				t.Fatalf("local preparation acknowledged: %v", err)
			}
			if err := opened.(ports.ChatDeferredTurnStarter).StartDeferredTurn(ref.ProviderTurnID); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("native prompt did not start")
			}
			if err := acceptance.WaitTurnAccepted(ctx, ref.ProviderTurnID); !errors.Is(err, context.Canceled) {
				t.Fatalf("local turn-started acknowledged: %v", err)
			}
			if err := acceptance.WaitTurnAccepted(t.Context(), "stale-or-unknown"); err == nil {
				t.Fatal("unknown prompt acknowledged")
			}
			close(release)
			waitCtx, waitCancel := context.WithTimeout(t.Context(), time.Second)
			defer waitCancel()
			err = acceptance.WaitTurnAccepted(waitCtx, ref.ProviderTurnID)
			if (err != nil) != rejected {
				t.Fatalf("rejected=%v acceptance=%v", rejected, err)
			}
			if err := opened.Close(); err != nil {
				t.Fatal(err)
			}
			if err := acceptance.WaitTurnAccepted(waitCtx, ref.ProviderTurnID); err == nil {
				t.Fatal("closed owner acknowledged")
			}
		})
	}
}
