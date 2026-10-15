package sessionmanager

import (
	"context"
	"errors"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type chatRestore struct {
	done   chan struct{}
	result RestoreResult
	err    error
}

// EnsureChatController shares one native restoration among callers. Delivery
// remains outside the attempt, so retries retain their own durable message ID.
func (m *Manager) EnsureChatController(ctx context.Context, id domain.SessionID) error {
	_, err := m.ensureChatController(ctx, id, false)
	if err != nil && !errors.Is(err, ports.ErrChatControllerRestore) {
		return fmt.Errorf("%w: %w", ports.ErrChatControllerRestore, err)
	}
	return err
}

func (m *Manager) ensureChatController(ctx context.Context, id domain.SessionID, explicit bool) (RestoreResult, error) {
	m.agentOpMu.Lock()
	if attempt := m.chatRestores[id]; attempt != nil {
		m.agentOpMu.Unlock()
		select {
		case <-attempt.done:
			return attempt.result, attempt.err
		case <-ctx.Done():
			return RestoreResult{}, ctx.Err()
		}
	}
	attempt := &chatRestore{done: make(chan struct{})}
	m.chatRestores[id] = attempt
	m.agentOpMu.Unlock()
	defer func() {
		m.agentOpMu.Lock()
		delete(m.chatRestores, id)
		close(attempt.done)
		m.agentOpMu.Unlock()
	}()
	restoreCtx, cancel := context.WithTimeout(ctx, m.statusVerificationLimit)
	defer cancel()
	attempt.result, attempt.err = m.resumeAgentWithMode(restoreCtx, id, explicit)
	if attempt.err != nil && !explicit {
		attempt.err = fmt.Errorf("%w: %w", ports.ErrChatControllerRestore, attempt.err)
	}
	return attempt.result, attempt.err
}

// chatNeedsStartupRestore uses only existing durable facts. Missing continuity
// reads and incomplete/unknown sessions never qualify as safely idle workers.
func (m *Manager) chatNeedsStartupRestore(ctx context.Context, rec domain.SessionRecord) (bool, error) {
	if rec.Kind != domain.KindWorker || rec.Metadata.WorkspacePath == "" || rec.Metadata.ProviderConversationID == "" {
		return true, nil
	}
	switch rec.Activity.State {
	case domain.ActivityIdle, domain.ActivityWaitingInput, domain.ActivityExited:
	default:
		return true, nil
	}
	if rec.Metadata.ConversationCheckpointUnsettled {
		return true, nil
	}
	if recovery, ok := m.chat.(interface {
		ChatNeedsController(context.Context, domain.SessionID) (bool, error)
	}); ok {
		return recovery.ChatNeedsController(ctx, rec.ID)
	}
	return true, nil
}
