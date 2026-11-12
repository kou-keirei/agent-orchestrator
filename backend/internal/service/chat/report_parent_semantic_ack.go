package chat

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// ReportParentSemanticACKSource extends df398's opt-in adapter with committed
// turn readback. Existing production wiring remains producer-unbound. The
// existing ExactParentReports singleton writer owns durable once-only resolution.
type ReportParentSemanticACKSource struct {
	Chat       *Service
	controller *Controller
	binding    *ports.ParentSemanticACKBinding
}

var _ ports.ExactReportNative = ReportParentSemanticACKSource{}

func (s ReportParentSemanticACKSource) WithLiveReportParent(ctx context.Context, expected ports.ExactReportParent,
	fn func(ports.ExactReportParent, ports.ExactReportChat) error,
) error {
	if s.Chat == nil || fn == nil {
		return &ports.ReportDeliveryHold{Reason: "report_native_binding_unbound"}
	}
	return s.Chat.WithLiveReportParent(ctx, expected, func(observed ports.ExactReportParent, chat ports.ExactReportChat) error {
		parent, ok := chat.(reportParentChat)
		if !ok || (s.controller != nil && parent.controller != s.controller) {
			return &ports.ReportDeliveryHold{Reason: "report_live_parent_mismatch"}
		}
		return fn(observed, semanticACKParentChat{reportParentChat: parent, parent: observed, binding: s.binding})
	})
}

type semanticACKParentChat struct {
	reportParentChat
	parent  ports.ExactReportParent
	binding *ports.ParentSemanticACKBinding
}

func (p semanticACKParentChat) ReadReportReconciliation(ctx context.Context, nativeEventID string) (ports.ExactReportReconciliation, error) {
	hold := func(reason string, cause error) (ports.ExactReportReconciliation, error) {
		return ports.ExactReportReconciliation{}, errors.Join(&ports.ReportDeliveryHold{Reason: reason}, cause)
	}
	if err := p.scope.Err(); err != nil {
		return hold("report_parent_scope_expired", err)
	}
	if err := ctx.Err(); err != nil {
		return hold("report_parent_attempt_expired", err)
	}
	if !validSemanticReference(nativeEventID) {
		return hold("report_semantic_event_missing", nil)
	}
	// Origin comes from the selected native conversation, never from payload,
	// context values, model text or a worker's report acceptance tool.
	source, ok := p.controller.conv.(ports.CommittedReportParentSemanticACKReader)
	if !ok {
		return hold("report_parent_semantic_producer_unbound", nil)
	}
	event, err := source.ReadCommittedReportParentSemanticACK(ctx, nativeEventID)
	if err != nil {
		return hold("report_semantic_event_unavailable", err)
	}
	if event.NativeEventID != nativeEventID || event.Binding.Message.Parent != p.parent ||
		(p.binding != nil && event.Binding != *p.binding) {
		return hold("report_semantic_event_mismatch", nil)
	}
	observed, err := p.committedBinding(ctx, event.Binding.Message)
	if err != nil || observed != event.Binding {
		return hold("report_semantic_turn_mismatch", err)
	}
	if err := errors.Join(p.scope.Err(), ctx.Err()); err != nil {
		return hold("report_parent_attempt_expired", err)
	}
	m := event.Binding.Message
	return ports.ExactReportReconciliation{NativeEventID: nativeEventID, ClientMessageID: m.ClientMessageID(), Message: m}, nil
}

func validSemanticReference(id string) bool {
	return id != "" && len(id) <= 1024 && strings.TrimSpace(id) == id &&
		!strings.ContainsAny(id, "\x00\r\n") && utf8.ValidString(id)
}

// committedBinding reads the existing durable message/turn and the provider's
// primary native history. A dispatch ACK, queued/failed/interrupted/rolled-back
// turn or nested thread cannot supply the captured identity. Completion is only
// a prerequisite for this settled-turn boundary, never semantic success.
func (p semanticACKParentChat) committedBinding(ctx context.Context, m ports.ExactReportMessage) (ports.ParentSemanticACKBinding, error) {
	fail := func(err error) (ports.ParentSemanticACKBinding, error) {
		return ports.ParentSemanticACKBinding{}, errors.Join(&ports.ReportDeliveryHold{Reason: "report_committed_parent_turn_unavailable"}, err)
	}
	c := p.controller
	if m.Parent != p.parent {
		return fail(nil)
	}
	msg, found, err := c.store.ConversationMessageByClientID(ctx, c.conversation.ID, m.ClientMessageID())
	if err != nil || !found || msg.Text != m.Payload || msg.Origin != domain.MessageOriginAutomation ||
		msg.Role != domain.MessageRoleUser || msg.DeliveryContentJSON != "" || msg.ConversationID != c.conversation.ID {
		return fail(err)
	}
	turn, err := c.store.TurnByID(ctx, msg.TurnID)
	if err != nil || turn.ID == "" || turn.ConversationID != c.conversation.ID || turn.HandledBySessionID != c.sessionID ||
		turn.BranchID != c.conversation.ActiveBranchID || turn.State != domain.TurnStateCompleted ||
		turn.StartedAt == nil || turn.CompletedAt == nil || turn.RolledBackAt != nil || !validSemanticReference(turn.ProviderTurnID) {
		return fail(err)
	}
	history, ok := c.conv.(ports.ChatHistoryReader)
	if !ok {
		return fail(nil)
	}
	events, err := history.ReadHistory(ctx)
	if err != nil {
		return fail(err)
	}
	nativeTurn := ""
	for _, event := range events {
		if event.Kind != ports.ChatEventTurnStarted || event.ProviderTurnID != turn.ProviderTurnID ||
			event.ProviderConversationID != p.parent.ProviderConversationID {
			continue
		}
		if !validSemanticReference(event.NativeTurnID) || !validSemanticReference(event.ProviderEventID) ||
			(nativeTurn != "" && nativeTurn != event.NativeTurnID) {
			return fail(nil)
		}
		nativeTurn = event.NativeTurnID
	}
	if nativeTurn == "" {
		return fail(nil)
	}
	return ports.ParentSemanticACKBinding{Message: m, ConversationID: turn.ConversationID, BranchID: turn.BranchID,
		TurnID: turn.ID, ProviderTurnID: turn.ProviderTurnID, NativeTurnID: nativeTurn}, nil
}

// BindReportParentSemanticACK is trusted owner wiring, not a public ACK API.
// The daemon supplies the frozen outbox message and reconciliation closure.
// The installed callback cannot select a different report, parent or turn.
func (s *Service) BindReportParentSemanticACK(ctx context.Context, m ports.ExactReportMessage,
	reconcile func(context.Context, ports.ExactReportNative, string) error,
) error {
	if s == nil || reconcile == nil {
		return &ports.ReportDeliveryHold{Reason: "report_native_binding_unbound"}
	}
	var ready atomic.Bool
	err := s.WithLiveReportParent(ctx, m.Parent, func(parent ports.ExactReportParent, chat ports.ExactReportChat) error {
		p := semanticACKParentChat{reportParentChat: chat.(reportParentChat), parent: parent}
		source, ok := p.controller.conv.(ports.AuthenticatedParentSemanticACKSource)
		if !ok {
			return &ports.ReportDeliveryHold{Reason: "report_parent_semantic_producer_unbound"}
		}
		binding, err := p.committedBinding(ctx, m)
		if err != nil {
			return err
		}
		native := ReportParentSemanticACKSource{Chat: s, controller: p.controller, binding: &binding}
		return source.BindAuthenticatedParentSemanticACK(ctx, binding, func(callCtx context.Context, id string) error {
			if !ready.Load() {
				return &ports.ReportDeliveryHold{Reason: "report_semantic_callback_unbound"}
			}
			if err := callCtx.Err(); err != nil {
				return errors.Join(&ports.ReportDeliveryHold{Reason: "report_parent_attempt_expired"}, err)
			}
			return reconcile(callCtx, native, id)
		})
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(&ports.ReportDeliveryHold{Reason: "report_parent_attempt_expired"}, err)
	}
	ready.Store(true) // only after successful binding and release of owner gates
	return nil
}
