package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestPromptlessChatSpawnOwnsFirstConversation(t *testing.T) {
	launcher := &recordingLauncher{}
	m, _, runtime := newChatManager(launcher)
	rec, _, _, err := m.Spawn(t.Context(), ports.SpawnConfig{
		ProjectID: chatTestProject, Kind: domain.KindWorker, Harness: domain.HarnessCodex,
		RequestedMode: domain.SessionModeChat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(launcher.started) != 1 || len(launcher.turns) != 0 || runtime.created != 0 {
		t.Fatalf("promptless spawn must open exactly one conversation without input: %+v", launcher)
	}
	if rec.Metadata.ProviderConversationID == "" || rec.Metadata.ControllerGeneration == "" ||
		launcher.started[0].ProviderConversationID != "" || launcher.started[0].SessionID != rec.ID {
		t.Fatalf("first birth did not commit its provider identity and generation: %+v", rec)
	}
}

func TestPromptlessChatMissingIdentityRefusesWithoutRetryOrFreshBirth(t *testing.T) {
	for _, prior := range []string{"zero turns", "prior generation", "prior user direction", "exited"} {
		t.Run(prior, func(t *testing.T) {
			m, svc, st, driver, rec := lazyChatFixture(t)
			ctx := t.Context()
			rec.Metadata.ProviderConversationID = ""
			rec.Metadata.ControllerGeneration = ""
			rec.Metadata.Permissions = domain.PermissionModeBypassPermissions
			rec.Metadata.BrowserCapabilityVerifier = "fixture-authenticated-owner"
			rec.FirstSignalAt = time.Time{}
			switch prior {
			case "prior generation":
				rec.Metadata.ControllerGeneration = "previous-owner"
			case "prior user direction":
				rec.Metadata.LatestUserPrompt = "possibly accepted direction"
				rec.Metadata.ConversationCheckpointUnsettled = true
			case "exited":
				rec.Activity.State = domain.ActivityExited
			}
			if err := st.UpdateSession(ctx, rec); err != nil {
				t.Fatal(err)
			}
			before, _, err := st.GetSession(ctx, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			conversation, err := st.ConversationForSession(ctx, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := st.LoadConversationSnapshot(ctx, conversation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if prior != "exited" {
				for range 2 {
					_, err := svc.Send(ctx, rec.ID, ports.ChatUserMessage{Text: "original direction", ClientMessageID: "original-id"})
					if !errors.Is(err, ports.ErrChatResumeFailed) || errors.Is(err, ports.ErrChatControllerRestore) {
						t.Fatalf("missing identity must require recovery rather than suggest retry: %v", err)
					}
				}
			}
			if _, err := m.ResumeAgentWithMode(ctx, rec.ID); !errors.Is(err, ports.ErrChatResumeFailed) || !errors.Is(err, ErrIncompleteHandle) {
				t.Fatalf("explicit resume lost missing-identity refusal: %v", err)
			}
			after, _, err := st.GetSession(ctx, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			afterRows, err := st.LoadConversationSnapshot(ctx, conversation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rows, afterRows) ||
				driver.starts.Load() != 0 || driver.resumes.Load() != 0 || svc.HasLiveChatController(rec.ID) || m.SessionMutationInProgress(rec.ID) {
				t.Fatalf("missing identity refusal changed owner/history or contacted provider: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestPromptlessChatRefusalPreservesInputAdmission(t *testing.T) {
	m, svc, st, driver, rec := lazyChatFixture(t)
	rec.Metadata.ProviderConversationID = ""
	if err := st.UpdateSession(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	if err := m.beginAgentSwitch(t.Context(), rec.ID); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Send(t.Context(), rec.ID, ports.ChatUserMessage{Text: "peer writer"})
	if !errors.Is(err, ErrSwitchInProgress) {
		t.Fatalf("peer operation lost its input fence: %v", err)
	}
	m.endAgentSwitch(rec.ID)
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	cancel()
	if _, err := svc.Send(ctx, rec.ID, ports.ChatUserMessage{Text: "expired input"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired input was admitted: %v", err)
	}
	if _, err := svc.Send(t.Context(), "wrong-session", ports.ChatUserMessage{Text: "wrong owner"}); err == nil {
		t.Fatal("wrong session was admitted")
	}
	if driver.starts.Load() != 0 || driver.resumes.Load() != 0 {
		t.Fatal("refused input created a provider conversation")
	}
}

func TestPromptlessChatMissingHistoryDoesNotAuthorizeBirth(t *testing.T) {
	_, svc, st, driver, rec := lazyChatFixture(t)
	rec.ID = ""
	rec.FirstSignalAt = time.Time{}
	rec.Metadata.ProviderConversationID = ""
	rec.Metadata.ControllerGeneration = ""
	created, err := st.CreateSession(t.Context(), rec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Send(t.Context(), created.ID, ports.ChatUserMessage{Text: "first direction", ClientMessageID: "unstarted"})
	if !errors.Is(err, ports.ErrChatResumeFailed) || errors.Is(err, ports.ErrChatControllerRestore) {
		t.Fatalf("missing history was misclassified: %v", err)
	}
	if _, err := st.ConversationForSession(t.Context(), created.ID); !errors.Is(err, domain.ErrNoConversation) {
		t.Fatalf("refusal materialized a conversation: %v", err)
	}
	if driver.starts.Load() != 0 || driver.resumes.Load() != 0 {
		t.Fatal("absence of history authorized provider birth")
	}
}
