package chat

import (
	"context"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var _ ports.ExactReportNative = (*Service)(nil)
var _ ports.ExactReportResumer = (*Service)(nil)

// WithLiveReportParent adapts the existing Chat owner/input gates to the opt-in
// exact-report consumer. It never restores or discovers a controller. The
// immutable reader supplies work/attempt provenance; the live controller and
// active branch independently supply the observed destination.
func (s *Service) WithLiveReportParent(ctx context.Context, expected ports.ExactReportParent, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	hold := func(reason string, err error) error {
		return errors.Join(&ports.ReportDeliveryHold{Reason: reason}, err)
	}
	if expected.SessionID == "" || fn == nil || s.sessions == nil || s.reader == nil {
		return hold("report_native_binding_unbound", nil)
	}
	input := s.inputGate(expected.SessionID)
	if err := input.lock(ctx); err != nil {
		return hold("report_parent_input_unavailable", err)
	}
	defer input.unlock()
	gate := s.controllerGate(expected.SessionID)
	if err := gate.lock(ctx); err != nil {
		return hold("report_parent_owner_unavailable", err)
	}
	defer gate.unlock()
	c, err := s.Controller(expected.SessionID)
	if err != nil {
		// Absence is not a positive provider-exit observation and must not grant
		// the consumer's optional Resume permission.
		return hold("report_parent_unavailable", err)
	}
	release, err := s.acquireInput(ctx, c.sessionID)
	if err != nil {
		return hold("report_parent_input_unavailable", err)
	}
	defer release()
	// Refuse contention rather than block on a mutex beyond the attempt context.
	if !c.sendMu.TryLock() {
		return hold("report_parent_busy", nil)
	}
	defer c.sendMu.Unlock()
	c.mu.Lock()
	available := c.state == ports.ChatControllerReady && c.handoff == controllerHandoffNone && !c.preserveProviderOnStop
	c.mu.Unlock()
	if !available {
		return hold("report_parent_unavailable", nil)
	}
	select {
	case <-c.stopped:
		return hold("report_parent_unavailable", nil)
	default:
	}
	reader, ok := s.store.(ports.WorkItemParentReader)
	if !ok {
		return hold("report_original_parent_unavailable", nil)
	}
	target, err := reader.GetWorkItemOriginalParent(ctx, expected.WorkItemID, expected.AttemptID)
	if err != nil {
		return hold("report_original_parent_unavailable", err)
	}
	record, err := s.requireChatSession(ctx, c.sessionID)
	if err != nil {
		return hold("report_parent_unavailable", err)
	}
	conversation, err := s.store.ConversationForSession(ctx, c.sessionID)
	if err != nil {
		return hold("report_parent_history_unavailable", err)
	}
	branch, err := s.store.ConversationBranch(ctx, conversation.ID, conversation.ActiveBranchID)
	if err != nil {
		return hold("report_parent_history_unavailable", err)
	}
	nativeID := c.ProviderConversationID()
	observed := ports.ExactReportParent{
		WorkItemID: target.WorkItemID, AttemptID: target.AttemptID, SessionID: c.sessionID,
		NativeSessionID: nativeID, ControllerGeneration: c.generation,
		ProviderConversationID: nativeID, ProviderScopeID: branch.ProviderScopeID,
	}
	if observed != expected || record.ID != c.sessionID || record.IsTerminated ||
		record.Kind != domain.KindOrchestrator || record.Harness != domain.HarnessCodex || c.harness != domain.HarnessCodex ||
		record.Metadata.ControllerGeneration != c.generation || record.Metadata.ProviderConversationID != nativeID ||
		record.Activity.State == domain.ActivityBlocked || record.Activity.State == domain.ActivityWaitingInput || record.Activity.State == domain.ActivityExited ||
		conversation.ID != c.conversation.ID || conversation.SessionID != c.sessionID || conversation.ProjectID != record.ProjectID ||
		branch.SessionID != c.sessionID || branch.ProviderConversationID != nativeID ||
		record.Metadata.ConversationCheckpointUnsettled ||
		(record.Metadata.ConversationCheckpointState.Trusted() && record.Metadata.ConversationCheckpointNativeID != nativeID) ||
		target.OriginalParentSessionID != observed.SessionID || target.NativeSessionID != observed.NativeSessionID ||
		target.ControllerGeneration != observed.ControllerGeneration || target.ProviderConversationID != nativeID || target.ProviderScopeID != observed.ProviderScopeID {
		return hold("report_live_parent_mismatch", nil)
	}
	if c.busy() {
		return hold("report_parent_busy", nil)
	}
	rows, err := s.reader.LoadConversationSnapshot(ctx, conversation.ID)
	if err != nil {
		return hold("report_parent_history_unavailable", err)
	}
	checkpoint := nativeHistoryCheckpoint{}
	checkpoint.captureAOHighWater(c.sessionID, rows.Turns, rows.Messages, rows.Activities)
	if _, err := c.readNativeHistory(ctx, rows.Turns, rows.Messages, rows.Activities, true, checkpoint); err != nil {
		return hold("report_parent_history_mismatch", err)
	}
	if err := ctx.Err(); err != nil {
		return hold("report_parent_attempt_expired", err)
	}
	scope, cancel := context.WithCancel(ctx)
	defer cancel()
	return fn(observed, reportParentChat{controller: c, scope: scope})
}

// WithResumedReportParent deliberately refuses automatic replacement. Accepted
// Resume verifies the same provider thread, but a replacement controller has a
// new generation. No accepted contract translates issuance-time generation to
// that replacement. Do not reuse a generation or start a fresh conversation to
// manufacture continuity. Existing ordinary Resume remains unchanged.
func (s *Service) WithResumedReportParent(ctx context.Context, _ ports.ExactReportParent, _ func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	return errors.Join(&ports.ReportDeliveryHold{Reason: "report_resume_generation_continuity_unavailable"}, ctx.Err())
}

type reportParentChat struct {
	controller *Controller
	scope      context.Context
}

// Report admission must never fall back to a drain-eligible ordinary append.
// Only a successful new append-and-reserve COMMIT grants immediate dispatch.
type reportAdmissionStore interface {
	AppendReservedUserMessage(context.Context, domain.ConversationSendOwner, domain.ConversationMessage, time.Time) (bool, error)
}

// SendReport runs inside the owner/input/send gates. Refusing busy controllers
// and atomically reserving admission prevent later ordinary queue dispatch.
func (p reportParentChat) SendReport(ctx context.Context, clientID, payload string) (bool, error) {
	c := p.controller
	if err := p.scope.Err(); err != nil {
		return false, errors.Join(&ports.ReportDeliveryHold{Reason: "report_parent_scope_expired"}, err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// Ordinary Chat deduplicates by ID alone. Reports require the same durable
	// bytes and origin before accepting that existing row as delivery evidence.
	existing, found, err := c.store.ConversationMessageByClientID(ctx, c.conversation.ID, clientID)
	if err != nil {
		return false, err
	}
	if found && (existing.Text != payload || existing.Origin != domain.MessageOriginAutomation || existing.DeliveryContentJSON != "") {
		return false, domain.ErrConversationSendConflict
	}
	turn, err := c.sendLocked(ctx, ports.ChatUserMessage{
		ClientMessageID: clientID, Text: payload, Origin: domain.MessageOriginAutomation,
	}, false)
	if err != nil {
		return false, err
	}
	if turn.ID == "" {
		message, found, err := c.store.ConversationMessageByClientID(ctx, c.conversation.ID, clientID)
		if err != nil || !found {
			return false, ordinarySendUncertain(err)
		}
		turn.ID = message.TurnID
	}
	turn, err = c.store.TurnByID(ctx, turn.ID)
	if err != nil {
		return false, ordinarySendUncertain(err)
	}
	if turn.ConversationID != c.conversation.ID || turn.HandledBySessionID != c.sessionID ||
		turn.BranchID != c.conversation.ActiveBranchID || turn.ProviderTurnID == "" || turn.State == domain.TurnStateQueued || turn.RolledBackAt != nil {
		return false, domain.ErrConversationSendUncertain
	}
	if _, deferred := c.conv.(ports.ChatDeferredTurnStarter); deferred {
		acceptance, ok := c.conv.(ports.ChatTurnAcceptance)
		if !ok {
			return false, domain.ErrConversationSendUncertain
		}
		if err := acceptance.WaitTurnAccepted(ctx, turn.ProviderTurnID); err != nil {
			return false, err
		}
	}
	return true, nil // provider delivery only; never semantic ACK
}

// No trusted parent-native semantic producer is bound by the accepted Codex
// protocol. Text, tool echoes, idle and turn completion cannot fill this port.
// W584 preregistration remains a separate product decision; no tool is added.
func (p reportParentChat) ReadReportReconciliation(context.Context, string) (ports.ExactReportReconciliation, error) {
	return ports.ExactReportReconciliation{}, &ports.ReportDeliveryHold{Reason: "report_parent_semantic_producer_unbound"}
}
