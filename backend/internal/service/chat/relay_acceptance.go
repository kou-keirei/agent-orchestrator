package chat

import (
	"context"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// RelayChatTurnAccepted is used only by the manager while it owns the switch
// operation. It must not acquire an ordinary input lease and wait on itself.
// Durable admission and a deferred local id cannot acknowledge the target.
func (s *Service) RelayChatTurnAccepted(ctx context.Context, id domain.SessionID, text, clientMessageID, generation string, validateOwner func() error) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	gate := s.inputGate(id)
	if err := gate.lock(ctx); err != nil {
		return "", err
	}
	defer gate.unlock()
	controller, release, err := s.workingController(ctx, id)
	if err != nil {
		return "", err
	}
	defer release()
	owner := func() error {
		if validateOwner == nil {
			return ErrControllerHandoff
		}
		if err := validateOwner(); err != nil {
			return err
		}
		rec, err := s.requireChatSession(ctx, id)
		if err != nil {
			return err
		}
		if rec.IsTerminated || controller.State() == ports.ChatControllerStopped || controller.handoffActive() ||
			generation == "" || controller.Generation() != generation ||
			rec.Metadata.ControllerGeneration != generation ||
			rec.Metadata.ProviderConversationID != controller.conv.ProviderConversationID() {
			return ErrControllerHandoff
		}
		_, err = s.acquireControllerInput(ctx, id, controller, func() (func(), error) {
			return func() {}, nil // The authenticated switch owns exclusion.
		})
		return err
	}
	if err := owner(); err != nil {
		return "", err
	}
	turn, err := controller.Send(ctx, ports.ChatUserMessage{
		Text: text, ClientMessageID: clientMessageID, Origin: domain.MessageOriginAutomation,
	})
	if err != nil {
		return turn.ID, err
	}
	if turn.ProviderTurnID == "" || turn.State == domain.TurnStateQueued {
		return turn.ID, domain.ErrConversationSendUncertain
	}
	if _, deferred := controller.conv.(ports.ChatDeferredTurnStarter); deferred {
		acceptance, ok := controller.conv.(ports.ChatTurnAcceptance)
		if !ok {
			return turn.ID, domain.ErrConversationSendUncertain
		}
		if err := acceptance.WaitTurnAccepted(ctx, turn.ProviderTurnID); err != nil {
			return turn.ID, fmt.Errorf("%w: %w", domain.ErrConversationSendUncertain, err)
		}
	}
	if err := owner(); err != nil {
		return turn.ID, err
	}
	return turn.ID, nil
}
