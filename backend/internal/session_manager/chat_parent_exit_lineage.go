package sessionmanager

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// ChatParentExitBinding is frozen by the native owner before reading exit
// evidence. Message retains the original parent generation and lease revision;
// it is never rewritten to name the restored destination.
type ChatParentExitBinding struct {
	Message              ports.ExactReportMessage
	Owner                domain.SessionControllerOwner
	ConversationID       string
	BranchID             string
	RuntimeGeneration    string
	OwnerID              string
	AttachmentGeneration uint64
}

// ChatParentExitLineage separates source evidence from destination authority.
// It is valid only during the owner's destination callback, not a durable
// generation translation or a semantic acknowledgement.
type ChatParentExitLineage struct {
	Source      ChatParentExitBinding
	Exit        ports.ChatProviderExitEvidence
	Destination ports.ExactReportParent
}

// ChatParentExitLease delegates authority to the existing native owner. Validate
// authenticates the complete immutable original-parent/report/lease binding and
// checks that it remains current. The reader belongs to the frozen attachment,
// not a lookup of whatever controller is current after exit.
type ChatParentExitLease interface {
	ports.ChatPositiveExitEvidenceReader
	Validate(context.Context) error
	// WithDestination independently authenticates the live destination and holds
	// its input/controller gates through fn. It verifies native history, scope,
	// and the supplied durable owner; it must not copy expected into observed.
	// The chat capability expires with fn. It must not reacquire Session Manager
	// input admission, which is already closed by this operation's resume fence.
	WithDestination(context.Context, domain.SessionControllerOwner, func(ports.ExactReportParent, ports.ExactReportChat) error) error
}

// ChatParentExitOwner is optional on ChatLauncher. It owns lease admission and
// source attachment retention, using AO's existing ownership gates. Callbacks
// are synchronous, once, and cannot escape. Its fence must permit StartChat's
// normal controller gate while excluding competing ownership/lease changes.
// No production adapter is supplied by this slice; absence is a typed HOLD.
type ChatParentExitOwner interface {
	WithChatParentExitLineage(context.Context, ports.ExactReportMessage, func(ChatParentExitBinding, ChatParentExitLease) error) error
}

func chatParentExitHold(reason string, cause error) error {
	return errors.Join(&ports.ReportDeliveryHold{Reason: reason}, cause)
}

// WithResumedChatParentAfterExit makes one bounded native restore under the
// session operation fence. It never discovers another parent, sends a report,
// edits an issuance binding, or treats Stopped/EOF/absence as process exit.
// The caller consumes explicit lineage; this deliberately does not implement
// ExactReportResumer, whose observed identity still requires the old generation.
func (m *Manager) WithResumedChatParentAfterExit(ctx context.Context, message ports.ExactReportMessage, fn func(ChatParentExitLineage, ports.ExactReportChat) error) error {
	owner, ok := m.chat.(ChatParentExitOwner)
	if !ok {
		return chatParentExitHold("report_resume_owner_adapter_unbound", nil)
	}
	parent := message.Parent
	if fn == nil || message.ReportID == "" || message.EventID == "" || message.SourceRevision == "" ||
		message.LeaseID == "" || message.LeaseRevision <= 0 || parent.WorkItemID == "" || parent.AttemptID == "" ||
		parent.SessionID == "" || parent.NativeSessionID == "" || parent.NativeSessionID != parent.ProviderConversationID ||
		parent.ControllerGeneration == "" || parent.ProviderScopeID == "" {
		return chatParentExitHold("report_resume_binding_incomplete", nil)
	}
	ctx, cancel := context.WithTimeout(ctx, m.statusVerificationLimit)
	defer cancel()
	if err := m.beginAgentResume(ctx, parent.SessionID); err != nil {
		return chatParentExitHold("report_resume_operation_unavailable", err)
	}
	defer m.endAgentResume(parent.SessionID)
	var calls atomic.Int32
	var consumed error
	err := owner.WithChatParentExitLineage(ctx, message, func(binding ChatParentExitBinding, lease ChatParentExitLease) error {
		if calls.Add(1) != 1 {
			return chatParentExitHold("report_resume_source_callback_replayed", nil)
		}
		if err := ctx.Err(); err != nil {
			consumed = chatParentExitHold("report_resume_expired", err)
			return consumed
		}
		consumed = m.resumeChatParentExitLineage(ctx, message, binding, lease, fn)
		return consumed
	})
	if calls.Load() != 1 {
		return chatParentExitHold("report_resume_source_callback_invalid", err)
	}
	return errors.Join(err, consumed)
}

func (m *Manager) resumeChatParentExitLineage(ctx context.Context, message ports.ExactReportMessage, binding ChatParentExitBinding, lease ChatParentExitLease, fn func(ChatParentExitLineage, ports.ExactReportChat) error) error {
	if binding.Message != message || lease == nil || binding.ConversationID == "" || binding.BranchID == "" || binding.OwnerID == "" || binding.AttachmentGeneration == 0 || binding.RuntimeGeneration == "" {
		return chatParentExitHold("report_resume_source_binding_mismatch", nil)
	}
	if err := lease.Validate(ctx); err != nil {
		return chatParentExitHold("report_resume_lease_unavailable", err)
	}
	rec, err := m.chatParentExitRecord(ctx, message.Parent, binding)
	if err != nil {
		return err
	}
	if rec.ControllerOwner() != binding.Owner {
		return chatParentExitHold("report_resume_source_owner_mismatch", nil)
	}
	if active, err := m.hasActiveInterfaceTransition(ctx, rec.ID); err != nil || active {
		return chatParentExitHold("report_resume_interface_transition", err)
	}
	releaseHarness, err := m.beginHarnessUse(rec.Harness)
	if err != nil {
		return chatParentExitHold("report_resume_harness_unavailable", err)
	}
	defer releaseHarness()
	exit, err := lease.PositiveExitEvidence(ctx)
	if err != nil {
		return chatParentExitHold("report_resume_exit_unproven", err)
	}
	if exit.SessionID != rec.ID || exit.ProviderConversationID != message.Parent.ProviderConversationID ||
		exit.ProviderScopeID != message.Parent.ProviderScopeID || exit.ControllerGeneration != binding.Owner.ControllerGeneration ||
		exit.RuntimeGeneration != binding.RuntimeGeneration || exit.OwnerID != binding.OwnerID ||
		exit.AttachmentGeneration != binding.AttachmentGeneration || exit.ObservedAt.IsZero() {
		return chatParentExitHold("report_resume_exit_mismatch", nil)
	}
	// Recheck after native I/O; the old observation cannot authorize a different
	// owner or a renewed/replaced lease. The owner keeps this fence through launch.
	if err := lease.Validate(ctx); err != nil {
		return chatParentExitHold("report_resume_lease_changed", err)
	}
	current, err := m.chatParentExitRecord(ctx, message.Parent, binding)
	if err != nil {
		return err
	}
	if current.ControllerOwner() != binding.Owner || m.chat.HasLiveChatController(rec.ID) {
		return chatParentExitHold("report_resume_source_owner_changed", nil)
	}
	if err := ctx.Err(); err != nil {
		return chatParentExitHold("report_resume_expired", err)
	}
	generation := m.newLaunchID()
	if generation == "" || generation == binding.Owner.ControllerGeneration {
		return chatParentExitHold("report_resume_generation_unavailable", nil)
	}
	result, err := m.resumeChatParentRecord(ctx, current, generation)
	if err != nil {
		return chatParentExitHold("report_resume_failed", err)
	}
	destination := message.Parent
	destination.ControllerGeneration = generation
	restored, err := m.chatParentExitRecord(ctx, destination, binding)
	if err != nil {
		return err
	}
	if result.Mode != RestoreModeNative || result.Session.ControllerOwner() != restored.ControllerOwner() {
		return chatParentExitHold("report_resume_destination_mismatch", nil)
	}
	var calls atomic.Int32
	var consumed error
	err = lease.WithDestination(ctx, restored.ControllerOwner(), func(observed ports.ExactReportParent, chat ports.ExactReportChat) error {
		if calls.Add(1) != 1 {
			return chatParentExitHold("report_resume_destination_callback_replayed", nil)
		}
		if err := ctx.Err(); err != nil {
			consumed = chatParentExitHold("report_resume_expired", err)
		} else if observed != destination || chat == nil {
			consumed = chatParentExitHold("report_resume_destination_unauthenticated", nil)
		} else if err := lease.Validate(ctx); err != nil {
			consumed = chatParentExitHold("report_resume_lease_changed", err)
		} else {
			current, err := m.chatParentExitRecord(ctx, destination, binding)
			if err != nil {
				consumed = err
			} else if current.ControllerOwner() != restored.ControllerOwner() {
				consumed = chatParentExitHold("report_resume_destination_mismatch", nil)
			} else {
				consumed = fn(ChatParentExitLineage{Source: binding, Exit: exit, Destination: observed}, chat)
			}
		}
		return consumed
	})
	if calls.Load() != 1 {
		return chatParentExitHold("report_resume_destination_callback_invalid", err)
	}
	return errors.Join(err, consumed)
}

// Durable history corroborates the adapter; metadata alone never authenticates
// a native destination. Never adopt a project conversation from another parent.
func (m *Manager) chatParentExitRecord(ctx context.Context, parent ports.ExactReportParent, binding ChatParentExitBinding) (domain.SessionRecord, error) {
	rec, found, err := m.store.GetSession(ctx, parent.SessionID)
	if err != nil || !found {
		return rec, chatParentExitHold("report_resume_parent_unavailable", err)
	}
	if rec.ID != parent.SessionID || rec.IsTerminated || rec.Kind != domain.KindOrchestrator || rec.Harness != domain.HarnessCodex ||
		domain.NormalizeSessionMode(rec.Mode) != domain.SessionModeChat || rec.Metadata.ControllerGeneration != parent.ControllerGeneration ||
		rec.Metadata.ProviderConversationID != parent.ProviderConversationID || rec.Metadata.ConversationCheckpointUnsettled ||
		rec.Activity.State == domain.ActivityBlocked || rec.Activity.State == domain.ActivityWaitingInput ||
		(rec.Metadata.ConversationCheckpointState.Trusted() && rec.Metadata.ConversationCheckpointNativeID != parent.NativeSessionID) {
		return rec, chatParentExitHold("report_resume_parent_mismatch", nil)
	}
	store, ok := m.store.(chatProviderOwnershipStore)
	if !ok {
		return rec, chatParentExitHold("report_resume_history_unavailable", nil)
	}
	conversation, err := store.ConversationForSession(ctx, rec.ID)
	if err != nil {
		return rec, chatParentExitHold("report_resume_history_unavailable", err)
	}
	branch, err := store.ConversationBranch(ctx, conversation.ID, conversation.ActiveBranchID)
	if err != nil || conversation.ID != binding.ConversationID || conversation.ActiveBranchID != binding.BranchID ||
		conversation.SessionID != rec.ID || conversation.ProjectID != rec.ProjectID ||
		branch.ConversationID != conversation.ID || branch.SessionID != rec.ID ||
		branch.ProviderConversationID != parent.ProviderConversationID || branch.ProviderScopeID != parent.ProviderScopeID {
		return rec, chatParentExitHold("report_resume_history_mismatch", err)
	}
	return rec, nil
}
