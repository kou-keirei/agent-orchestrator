package chat

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const deltaBatchEvents = 32
const deltaBatchBytes = 32 * 1024

// No timer or reader goroutine: only drain deltas already waiting in the
// provider's bounded channel. Keep the first boundary event for the next call.
type deltaBatchReader struct {
	events  <-chan ports.ChatEvent
	enabled bool
	pending *ports.ChatEvent
}

func (r *deltaBatchReader) next() []ports.ChatEvent {
	var first ports.ChatEvent
	if r.pending != nil {
		first, r.pending = *r.pending, nil
	} else {
		var ok bool
		first, ok = <-r.events
		if !ok {
			return nil
		}
	}
	batch := []ports.ChatEvent{first}
	if !r.enabled || first.Kind != ports.ChatEventMessageDelta {
		return batch
	}
	bytes := len(first.Delta)
	for len(batch) < deltaBatchEvents && bytes < deltaBatchBytes {
		select {
		case event, ok := <-r.events:
			if !ok {
				return batch
			}
			if event.Kind != ports.ChatEventMessageDelta || bytes+len(event.Delta) > deltaBatchBytes {
				r.pending = &event
				return batch
			}
			batch = append(batch, event)
			bytes += len(event.Delta)
		default:
			return batch
		}
	}
	return batch
}

// Optional so alternate stores retain the singleton path. The callback receives
// the store's existing projection transaction context; no event is joined or
// discarded, and native history never enters this live-stream batching path.
type providerEventBatchStore interface {
	ProjectProviderEventBatch(context.Context, func(context.Context) error) error
}

func (c *Controller) projectEvents(ctx context.Context, events []ports.ChatEvent, persistent bool) (bool, bool, error) {
	batchStore, supported := c.store.(providerEventBatchStore)
	if len(events) == 1 || !supported {
		var projected, primary bool
		for _, event := range events {
			p, turn, err := c.projectEvent(ctx, event)
			if err != nil {
				return false, false, err
			}
			projected, primary = projected || p, primary || turn
		}
		return projected, primary, nil
	}
	err := batchStore.ProjectProviderEventBatch(ctx, func(txCtx context.Context) error {
		for _, event := range events {
			if _, _, err := c.projectEvent(txCtx, event); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && !persistent {
		// Match the old nonpersistent policy: one malformed event must not lose
		// later queued output. The failed batch has rolled back in its entirety.
		for _, event := range events {
			if _, _, eventErr := c.projectEvent(ctx, event); eventErr != nil {
				c.log.Error("failed to project chat event", "session", c.sessionID, "kind", event.Kind, "error", eventErr)
			}
		}
		return false, false, nil
	}
	// Message deltas have no volatile lifecycle or afterProject effects.
	return false, false, err
}
