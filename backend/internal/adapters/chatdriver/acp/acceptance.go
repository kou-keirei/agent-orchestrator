package acp

import (
	"context"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// One prompt can be in flight. Keep its result until the next preparation so a
// fast native response cannot be lost before the switch starts waiting.
type promptAcceptance struct {
	id   string
	done chan struct{}
	err  error
	set  bool
}

var _ ports.ChatTurnAcceptance = (*conversation)(nil)

func (c *conversation) WaitTurnAccepted(ctx context.Context, id string) error {
	c.mu.Lock()
	result := c.promptAcceptance
	closed := c.closed || c.detaching
	c.mu.Unlock()
	if closed || result == nil || id == "" || result.id != id {
		return errors.New("ACP prompt acceptance is unknown")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-result.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed || c.detaching {
			return errConversationClosed
		}
		return result.err
	}
}

// Only the response to the actual native Prompt call can settle acceptance.
// Local turn-started events, settings updates and prepared ids cannot do so.
func (c *conversation) settlePromptAcceptanceLocked(id string, err error) {
	result := c.promptAcceptance
	if result == nil || result.id != id || result.set {
		return
	}
	result.err = err
	result.set = true
	close(result.done)
}
