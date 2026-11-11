package daemon

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// Bind only accepted native boundaries. Semantic reconciliation remains the
// Chat adapter's typed producer-unbound HOLD; no public ACK is introduced.
func newExactParentReports(store *sqlite.Store, native ports.ExactReportNative) *orchestrationevents.ExactParentReports {
	return &orchestrationevents.ExactParentReports{Store: store, Source: store, Parents: store, Native: native}
}

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
