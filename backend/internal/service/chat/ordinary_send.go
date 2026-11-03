package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// OrdinarySendStore is the existing durable pre-provider reservation contract.
// A successful Reserve commits authority for one call using its returned prompt;
// no error or readback grants that authority. Resolution requires the same owner.
type OrdinarySendStore interface {
	OrdinarySendAttempt(context.Context, string) (domain.ConversationSendAttempt, error)
	ReserveOrdinarySend(context.Context, domain.ConversationSendOwner, time.Time) (domain.QueuedTurn, error)
	BindReservedOrdinarySend(context.Context, domain.ConversationSendOwner, string, time.Time) error
	ConfirmOrdinarySendNotDelivered(context.Context, domain.ConversationSendOwner, string, time.Time) error
}

func ordinarySendUncertain(err error) error {
	return fmt.Errorf("%w: %w", ports.ErrChatDeliveryUncertain, err)
}

// An uncommitted native refusal must not authorize branch restoration through
// the older ChatRefusal classification. Preserve it as a diagnostic while the
// durable uncertainty (and any store failure) remains the actionable result.
func ordinarySendProviderUncertain(providerErr, cause error) error {
	var refusal providerRefusal
	if errors.As(providerErr, &refusal) && refusal.ChatRefusal() {
		return ordinarySendUncertain(fmt.Errorf("native result %s: %w", fmt.Sprintf("%v", providerErr), cause))
	}
	return ordinarySendUncertain(errors.Join(providerErr, cause))
}

// confirmTurnDelivery distinguishes untouched queue admission from an attempt.
// The reservation, rather than a controller's volatile state, owns that fact.
func (c *Controller) confirmTurnDelivery(ctx context.Context, turn domain.ConversationTurn) error {
	if turn.State != domain.TurnStateQueued || turn.ProviderTurnID != "" {
		return nil
	}
	attempt, err := c.store.OrdinarySendAttempt(ctx, turn.ID)
	if err != nil {
		return ordinarySendUncertain(err)
	}
	if attempt.State != domain.ConversationSendPending {
		return ordinarySendUncertain(domain.ErrConversationSendUncertain)
	}
	return nil
}

// A surviving, unrelated native turn is not permission to steer across an
// unresolved ordinary send. NextQueuedTurn already owns this conversation gate.
func (c *Controller) guardOrdinarySend(ctx context.Context) error {
	_, err := c.store.NextQueuedTurn(ctx, c.conversation.ID)
	if err != nil && !errors.Is(err, domain.ErrNoQueuedTurn) {
		return ordinarySendUncertain(err)
	}
	return nil
}

func (c *Controller) reserveOrdinarySend(ctx context.Context, turnID string, msg ports.ChatUserMessage) (domain.ConversationSendOwner, ports.ChatUserMessage, error) {
	attempt, err := c.store.OrdinarySendAttempt(ctx, turnID)
	if err != nil {
		return domain.ConversationSendOwner{}, ports.ChatUserMessage{}, ordinarySendUncertain(err)
	}
	owner := attempt.ConversationSendOwner
	if owner.ConversationID != c.conversation.ID || owner.SessionID != c.sessionID ||
		owner.TurnID != turnID || owner.ClientMessageID != msg.ClientMessageID {
		return owner, ports.ChatUserMessage{}, domain.ErrConversationSendConflict
	}
	// Pending admissions may belong to an earlier controller generation. The
	// store atomically verifies the current session/generation and active branch.
	owner.ControllerGeneration = c.generation
	queued, err := c.store.ReserveOrdinarySend(ctx, owner, c.now())
	if err != nil {
		if errors.Is(err, domain.ErrConversationSendUncertain) {
			err = ordinarySendUncertain(err)
		}
		return owner, ports.ChatUserMessage{}, err
	}
	durable := ports.ChatUserMessage{
		Text: queued.Text, ClientMessageID: queued.ClientMessageID,
		Origin: queued.Origin, Settings: msg.Settings,
	}
	if queued.DeliveryContentJSON != "" {
		if err := json.Unmarshal([]byte(queued.DeliveryContentJSON), &durable.Content); err != nil {
			return owner, ports.ChatUserMessage{}, ordinarySendUncertain(err)
		}
	}
	return owner, durable, nil
}
