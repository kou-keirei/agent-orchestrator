package sessionmanager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type blockingChatPreflightLauncher struct {
	integrationChatLauncher
	entered chan struct{}
	release chan struct{}
}

func (l blockingChatPreflightLauncher) PreflightChat(ctx context.Context, harness domain.AgentHarness, path string, permission ports.PermissionMode) error {
	close(l.entered)
	select {
	case <-l.release:
		return l.integrationChatLauncher.PreflightChat(ctx, harness, path, permission)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestManagerOrdinaryChatSendRejectedDuringSwitchPreflight(t *testing.T) {
	f := newChatSwitchIntegrationFixture(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	launcher := blockingChatPreflightLauncher{
		integrationChatLauncher: integrationChatLauncher{service: f.service},
		entered:                 make(chan struct{}), release: make(chan struct{}),
	}
	f.manager.chat = launcher
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(launcher.release) }) }
	t.Cleanup(func() {
		release()
		waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = f.manager.WaitAgentSwitchWorkers(waitCtx)
	})
	sw, err := f.manager.SwitchAgent(ctx, f.session.ID, SwitchAgentConfig{
		TargetHarness: domain.HarnessCodex, IdempotencyKey: "ordinary-peer-preflight",
	})
	if err != nil {
		t.Fatal(err)
	}
	awaitSwitchTestSignal(t, launcher.entered, "target Chat preflight")
	if !f.manager.SessionMutationInProgress(f.session.ID) {
		t.Fatal("preflight did not close input admission")
	}
	if err := f.manager.Send(ctx, f.session.ID, "ordinary peer", nil); !errors.Is(err, ports.ErrChatInputBlocked) || !errors.Is(err, ErrSwitchInProgress) {
		t.Fatalf("ordinary peer bypassed switch admission: %v", err)
	}
	if err := f.manager.send(ctx, f.session.ID, "durable relay retry", "ordinary-retry"); !errors.Is(err, ports.ErrChatInputBlocked) {
		t.Fatalf("keyed relay bypassed switch admission: %v", err)
	}
	for _, provider := range []*integrationChatConversation{f.sourceChat, f.targetChat} {
		provider.mu.Lock()
		sends := len(provider.sent)
		provider.mu.Unlock()
		if sends != 0 {
			t.Fatalf("refused peer reached provider %s: sends=%d", provider.providerID, sends)
		}
	}
	release()
	if err := f.manager.WaitAgentSwitchWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	completed, found, err := f.store.GetAgentSwitch(ctx, sw.ID)
	if err != nil || !found || completed.State != domain.AgentSwitchCompleted || completed.TargetAcknowledgedAt == nil {
		t.Fatalf("switch continuation failed: switch=%+v found=%v error=%v", completed, found, err)
	}
	f.targetChat.mu.Lock()
	defer f.targetChat.mu.Unlock()
	if len(f.targetChat.sent) != 1 || f.targetChat.sent[0].ClientMessageID != chatSwitchActivationMessageID(sw.ID) {
		t.Fatalf("switch continuation sends=%+v", f.targetChat.sent)
	}
}

func TestManagerOrdinaryChatSendHoldsLeaseThroughProviderDelivery(t *testing.T) {
	f := newChatSwitchIntegrationFixture(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	f.sourceChat.beforeSend = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- f.manager.Send(ctx, f.session.ID, "ordinary delivery", nil) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("ordinary send did not reach provider")
	}
	f.manager.agentOpMu.Lock()
	leases := f.manager.inputLeases[f.session.ID]
	f.manager.agentOpMu.Unlock()
	if leases != 1 {
		t.Errorf("provider delivery holds %d input leases, want exactly one", leases)
	}
	switchDone := make(chan error, 1)
	go func() { switchDone <- f.manager.beginAgentSwitch(ctx, f.session.ID) }()
	eventuallySessionInput(t, time.Second, func() bool { return f.manager.SessionMutationInProgress(f.session.ID) })
	select {
	case err := <-switchDone:
		t.Errorf("switch failed to drain provider delivery: %v", err)
		switchDone <- err
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-switchDone; err != nil {
		t.Fatal(err)
	}
	f.manager.endAgentSwitch(f.session.ID)
	f.manager.agentOpMu.Lock()
	leases = f.manager.inputLeases[f.session.ID]
	f.manager.agentOpMu.Unlock()
	if leases != 0 {
		t.Fatalf("ordinary send leaked %d leases", leases)
	}
	f.sourceChat.mu.Lock()
	defer f.sourceChat.mu.Unlock()
	if len(f.sourceChat.sent) != 1 || f.sourceChat.sent[0].Text != "ordinary delivery" || f.sourceChat.sent[0].Origin != domain.MessageOriginAutomation {
		t.Fatalf("ordinary provider delivery=%+v", f.sourceChat.sent)
	}
}

func TestManagerChatRelayRestoresBeforeAdmission(t *testing.T) {
	for _, automation := range []bool{false, true} {
		name := "ordinary"
		if automation {
			name = "automation"
		}
		t.Run(name, func(t *testing.T) {
			m, svc, st, driver, rec := lazyChatFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if automation {
				if ack, err := m.SendAutomationWithAcknowledgement(ctx, rec.ID, "resume relay", "automation-key"); err != nil || !ack {
					t.Fatalf("automation delivery: ack=%v err=%v", ack, err)
				}
			} else if err := m.Send(ctx, rec.ID, "resume relay", nil); err != nil {
				t.Fatal(err)
			}
			current, found, err := st.GetSession(ctx, rec.ID)
			if err != nil || !found || current.ID != rec.ID || current.Metadata.ProviderConversationID != rec.Metadata.ProviderConversationID || current.Metadata.ControllerGeneration == rec.Metadata.ControllerGeneration {
				t.Fatalf("restore lost native continuity or generation: current=%+v found=%v err=%v", current, found, err)
			}
			if driver.starts.Load() != 0 || driver.resumes.Load() != 1 || !svc.HasLiveChatController(rec.ID) || m.SessionMutationInProgress(rec.ID) {
				t.Fatalf("relay restore: starts=%d resumes=%d live=%v gated=%v", driver.starts.Load(), driver.resumes.Load(), svc.HasLiveChatController(rec.ID), m.SessionMutationInProgress(rec.ID))
			}
			provider := driver.conversations[0]
			provider.mu.Lock()
			defer provider.mu.Unlock()
			if len(provider.sent) != 1 || provider.sent[0].Text != "resume relay" || provider.sent[0].Origin != domain.MessageOriginAutomation {
				t.Fatalf("restored relay delivery=%+v", provider.sent)
			}
			m.agentOpMu.Lock()
			leases := m.inputLeases[rec.ID]
			m.agentOpMu.Unlock()
			if leases != 0 {
				t.Fatalf("relay leaked %d leases", leases)
			}
		})
	}
}
