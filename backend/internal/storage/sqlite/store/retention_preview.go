package store

import (
	"sort"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// RetentionFact represents evidence states, not booleans. The preview explicitly
// initializes absent evidence to UNKNOWN; absent evidence never grants eligibility.
type RetentionFact string

const (
	RetentionUnknown RetentionFact = "UNKNOWN"
	RetentionYes     RetentionFact = "YES"
	RetentionNo      RetentionFact = "NO"
)

// RetentionPreview is a single-row fixture result, not a snapshot census or a
// deletion plan. No storage reader, runtime, clock, or caller-provided positive
// assertions participate in this version.
type RetentionPreview struct {
	PolicyVersion                string        `json:"policy_version"`
	EventID                      int64         `json:"event_id"`
	Class                        string        `json:"class"`
	EventConversationBinding     RetentionFact `json:"event_conversation_binding"`
	EventBranchBinding           RetentionFact `json:"event_branch_binding"`
	EventProviderTurnItemBinding RetentionFact `json:"event_provider_turn_item_binding"`
	TurnSettled                  RetentionFact `json:"turn_settled"`
	CompleteProjection           RetentionFact `json:"complete_projection"`
	CoveringCheckpoint           RetentionFact `json:"covering_checkpoint"`
	NativeReplayNeeded           RetentionFact `json:"native_replay_needed"`
	NativeAcknowledged           RetentionFact `json:"native_acknowledged"`
	BranchReplayNeeded           RetentionFact `json:"branch_replay_needed"`
	HostReplayNeeded             RetentionFact `json:"host_replay_needed"`
	ReviewerObligation           RetentionFact `json:"reviewer_obligation"`
	ProvenanceObligation         RetentionFact `json:"provenance_obligation"`
	RecoveryObligation           RetentionFact `json:"recovery_obligation"`
	WithinExplicitPolicyWindow   RetentionFact `json:"within_explicit_policy_window"`
	Identified                   RetentionFact `json:"identified"`
	Reasons                      []string      `json:"reasons"`
	CoverageComplete             bool          `json:"coverage_complete"`
	StopReason                   string        `json:"stop_reason"`
	DeletionAuthority            string        `json:"deletion_authority"`
	FilesystemReclaimEstimate    string        `json:"filesystem_reclaim_estimate"`
}

// PreviewRetentionEvent implements the conservative source-supported subset of
// ao16.retention-preview.v1. It examines fixed scalar metadata only; it does not
// parse, copy, or emit payloads or variable-size identifiers. Work and output are
// bounded independently of payload size. The supplied row is never modified.
//
// Evidence mapping (source at 76c1daea92619fb4249ee0fdd587f5f4e0c53c65):
//   - gen.ConversationProviderEvent / migration 0066: nonempty ProviderEventID
//     positively identifies a deduped event; empty means anonymous, not redundant.
//     Method supplies the exact method protection below.
//   - ConversationID and BranchID (0087) are stored associations. A lone fixture
//     row cannot validate their referenced rows, provider ownership epoch, or
//     event-to-provider-turn/item identity. All binding facts remain UNKNOWN.
//   - conversation_turns.state/completed_at describe a turn, but this row has no
//     typed turn FK. Age, received_at and terminal state cannot prove settlement
//     for this event, nor an explicit retention policy window.
//   - ProjectProviderEvent atomically archives and projects, but records no
//     per-event completeness/covering checkpoint. In particular, later message
//     completion can replace projected text. Both coverage facts stay UNKNOWN.
//   - Branch replay_cutoff_sequence/replay_truncated describe replay selection,
//     not proof that raw events are dispensable. No consumed archive fact proves
//     absence of native, branch or host replay needs or reviewer, provenance or
//     recovery obligations. Each remains independently UNKNOWN.
//
// SOURCE_GAP: no current mapping can prove every positive independent condition
// for an anonymous message.delta candidate. Consequently this kernel emits only
// UNKNOWN_REVIEW_REQUIRED, preserving explicit protection reasons. Snapshot
// traversal and eligibility integration require a separately reviewed source
// contract; this function supplies neither high-water nor census evidence.
//
// Minimum additional contract (not implemented here): an AO-owned durable
// checkpoint must bind a bounded event-ID window and exact conversation, branch,
// provider ownership epoch, turn and item to committed projection coverage and
// provider-native replay/checkpoint/ack evidence. Branch reconstruction and each
// reviewer/provenance/recovery hold must be resolved independently at the same
// snapshot. Native provider semantics remain owned by the provider; AO consumes
// its evidence rather than manufacturing an acknowledgement from turn state.
// Donor e30d685b's replay checkpoint is absent at this base and cannot be inferred
// from payload claims or imported history. A lossless fixture archive proves
// transport only, not this contract or production restore/Resume admission.
func PreviewRetentionEvent(row gen.ConversationProviderEvent) RetentionPreview {
	p := RetentionPreview{
		PolicyVersion:                "ao16.retention-preview.v1",
		EventID:                      row.ID,
		Class:                        "UNKNOWN_REVIEW_REQUIRED",
		EventConversationBinding:     RetentionUnknown,
		EventBranchBinding:           RetentionUnknown,
		EventProviderTurnItemBinding: RetentionUnknown,
		TurnSettled:                  RetentionUnknown,
		CompleteProjection:           RetentionUnknown,
		CoveringCheckpoint:           RetentionUnknown,
		NativeReplayNeeded:           RetentionUnknown,
		NativeAcknowledged:           RetentionUnknown,
		BranchReplayNeeded:           RetentionUnknown,
		HostReplayNeeded:             RetentionUnknown,
		ReviewerObligation:           RetentionUnknown,
		ProvenanceObligation:         RetentionUnknown,
		RecoveryObligation:           RetentionUnknown,
		WithinExplicitPolicyWindow:   RetentionUnknown,
		Identified:                   RetentionNo,
		Reasons: []string{
			"BRANCH_BINDING_UNKNOWN", "BRANCH_REPLAY_UNKNOWN",
			"CONVERSATION_BINDING_UNKNOWN", "COVERING_CHECKPOINT_UNKNOWN",
			"EXPLICIT_POLICY_WINDOW_UNKNOWN", "HOST_REPLAY_UNKNOWN",
			"NATIVE_ACK_UNKNOWN", "NATIVE_REPLAY_UNKNOWN", "PROJECTION_COMPLETENESS_UNKNOWN",
			"PROVENANCE_OBLIGATION_UNKNOWN", "PROVIDER_TURN_ITEM_BINDING_UNKNOWN",
			"RECOVERY_OBLIGATION_UNKNOWN", "REVIEWER_OBLIGATION_UNKNOWN",
			"TURN_SETTLEMENT_UNKNOWN",
		},
		StopReason:                "SOURCE_GAP",
		DeletionAuthority:         "NONE",
		FilesystemReclaimEstimate: "UNKNOWN",
	}
	if row.ProviderEventID != "" {
		p.Identified = RetentionYes
		p.Reasons = append(p.Reasons, "IDENTIFIED_EVENT_RETAIN")
	}
	switch row.Method {
	case "message.delta":
		p.Reasons = append(p.Reasons, "MESSAGE_DELTA_COVERAGE_UNPROVEN")
	case "command.output.delta":
		p.Reasons = append(p.Reasons, "COMMAND_OUTPUT_RETAIN")
	case "reasoning.delta":
		p.Reasons = append(p.Reasons, "REASONING_RECOVERABILITY_UNPROVEN")
	default:
		p.Reasons = append(p.Reasons, "METHOD_NOT_ADMITTED_RETAIN")
	}
	sort.Strings(p.Reasons)
	return p
}
