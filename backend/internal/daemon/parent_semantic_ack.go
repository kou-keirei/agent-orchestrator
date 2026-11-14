package daemon

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
)

// bindParentSemanticACK is opt-in owner wiring for a future native producer.
// No route, authored tool or current provider invokes it. The frozen outbox and
// immutable receipt select the callback's report; a native notification supplies
// only its durable event reference. Missing provider support stays on HOLD.
func bindParentSemanticACK(ctx context.Context, store ports.ExactReportStore, parents ports.ExactReportParentReader,
	chat *chatsvc.Service, project domain.ProjectID, reportID string,
) error {
	if store == nil || parents == nil || chat == nil {
		return &ports.ReportDeliveryHold{Reason: "report_native_binding_unbound"}
	}
	ctx, cancel := context.WithTimeout(ctx, orchestrationevents.AttemptTimeout)
	defer cancel()
	return store.WithExactParentReport(ctx, project, reportID, func(w ports.ExactReportWriter) error {
		receipt, event, err := w.Read(ctx)
		if err != nil {
			return err
		}
		m, err := ports.ParseExactReportMessage(event.LeaseToken)
		if err != nil {
			return err
		}
		if err := m.ValidateSource(receipt, event); err != nil {
			return err
		}
		if receipt.Report.ID != reportID || event.ProjectID != project || receipt.Report.Identity.ProjectID != project ||
			event.DestinationSessionID != m.Parent.SessionID || event.AttemptCount == 0 {
			return &ports.ReportDeliveryHold{Reason: "report_binding_mismatch"}
		}
		return chat.BindReportParentSemanticACK(ctx, m, func(callCtx context.Context, native ports.ExactReportNative, id string) error {
			engine := orchestrationevents.ExactParentReports{Store: store, Parents: parents, Native: native}
			return engine.Reconcile(callCtx, project, reportID, id)
		})
	})
}
