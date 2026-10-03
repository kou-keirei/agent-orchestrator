package daemon

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
)

type automationSender interface {
	SendAutomation(context.Context, domain.SessionID, string, string) error
}

type automationAdmissionSender interface {
	SendAutomationWithAcknowledgement(context.Context, domain.SessionID, string, string) (bool, error)
}

type orchestrationTransport struct{ sender automationSender }

func (t orchestrationTransport) Submit(ctx context.Context, target domain.SessionRecord, batch orchestrationevents.Batch) (orchestrationevents.Submission, error) {
	var acknowledged bool
	var err error
	if sender, ok := t.sender.(automationAdmissionSender); ok {
		acknowledged, err = sender.SendAutomationWithAcknowledgement(ctx, target.ID, batch.Payload, batch.ID)
	} else {
		// Error-only senders cannot prove durable Chat admission. A pane write
		// remains submitted until the exact lifecycle hook acknowledges it.
		err = t.sender.SendAutomation(ctx, target.ID, batch.Payload, batch.ID)
	}
	if err != nil {
		return orchestrationevents.Submission{}, err
	}
	// Chat's durable client-message-id admission is the acknowledgement. TUI is
	// submitted only; its exact prompt-submit hook closes the leased batch.
	return orchestrationevents.Submission{Submitted: true, Acknowledged: acknowledged}, nil
}
