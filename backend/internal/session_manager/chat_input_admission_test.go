package sessionmanager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestChatInputLeaseFencesSwitchAndValidatesRestoredGeneration(t *testing.T) {
	m, svc, st, driver, rec := lazyChatFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := m.EnsureChatController(ctx, rec.ID); err != nil {
		t.Fatal(err)
	}
	current, _, err := st.GetSession(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.AcquireChatInput(ctx, rec.ID, rec.Metadata.ControllerGeneration); !errors.Is(err, ports.ErrChatControllerRestore) {
		t.Fatalf("stale generation admitted: %v", err)
	}
	release, err := m.AcquireChatInput(ctx, rec.ID, current.Metadata.ControllerGeneration)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.beginAgentSwitch(ctx, rec.ID) }()
	eventuallySessionInput(t, time.Second, func() bool { return m.SessionMutationInProgress(rec.ID) })
	select {
	case err := <-done:
		t.Fatalf("switch did not drain peer sender: %v", err)
	default:
	}
	_, err = svc.SteerOrSend(ctx, rec.ID, ports.ChatUserMessage{Text: "peer sender", ClientMessageID: "peer"}, false)
	if !errors.Is(err, ports.ErrChatInputBlocked) {
		t.Fatalf("switch allowed external writer: %v", err)
	}
	if _, err := svc.SetTurnSettings(ctx, rec.ID, domain.ConversationSettings{Model: "peer-model"}); !errors.Is(err, ports.ErrChatInputBlocked) {
		t.Fatalf("switch allowed settings: %v", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m.endAgentSwitch(rec.ID)
	if driver.resumes.Load() != 1 {
		t.Fatalf("unexpected peer restore count %d", driver.resumes.Load())
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := m.AcquireChatInput(cancelled, rec.ID, current.Metadata.ControllerGeneration); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled writer admitted: %v", err)
	}
}

func TestChatInputRejectsOwnerLoss(t *testing.T) {
	m, _, st, _, rec := lazyChatFixture(t)
	if err := m.EnsureChatController(t.Context(), rec.ID); err != nil {
		t.Fatal(err)
	}
	current, _, err := st.GetSession(t.Context(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation := current.Metadata.ControllerGeneration
	current.Metadata.ControllerGeneration = "replacement"
	if err := st.UpdateSession(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AcquireChatInput(t.Context(), rec.ID, generation); !errors.Is(err, ports.ErrChatControllerRestore) {
		t.Fatalf("lost owner admitted: %v", err)
	}
	if err := m.beginAgentSwitch(t.Context(), rec.ID); err != nil {
		t.Fatalf("refusal leaked input lease: %v", err)
	}
	m.endAgentSwitch(rec.ID)
}
