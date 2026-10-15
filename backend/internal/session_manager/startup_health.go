package sessionmanager

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type startupRecoveryWorkReader interface {
	ConversationForSession(context.Context, domain.SessionID) (domain.ConversationRecord, error)
	HasUnsettledConversationTurns(context.Context, string) (bool, error)
	HasPendingConversationInteractions(context.Context, string) (bool, error)
}

// The accepted fork can resume a provider but has no reconnect-only port. A
// replacement process would settle old work/approvals. Preserve those facts
// instead of assuming a missing controller proves its native process died.
func (m *Manager) checkSessionHealth(ctx context.Context, rec domain.SessionRecord) error {
	if domain.NormalizeSessionMode(rec.Mode) == domain.SessionModeChat {
		if m.chat != nil && m.chat.HasLiveChatController(rec.ID) {
			return nil
		}
		if rec.Metadata.ConversationCheckpointUnsettled {
			return ports.ErrChatRecoveryInconclusive
		}
		if work, ok := m.store.(startupRecoveryWorkReader); ok {
			conversation, err := work.ConversationForSession(ctx, rec.ID)
			if err == nil {
				if conversation.ID == "" || conversation.SessionID != rec.ID {
					return ports.ErrChatRecoveryInconclusive
				}
				unfinished, err := work.HasUnsettledConversationTurns(ctx, conversation.ID)
				if err != nil || unfinished {
					return fmt.Errorf("startup unfinished Chat work: %w", ports.ErrChatRecoveryInconclusive)
				}
				pending, err := work.HasPendingConversationInteractions(ctx, conversation.ID)
				if err != nil || pending {
					return fmt.Errorf("startup pending Chat interaction: %w", ports.ErrChatRecoveryInconclusive)
				}
			} else if err != domain.ErrNoConversation {
				return err
			}
		}
	}
	return m.reconcileLive(ctx, rec)
}
