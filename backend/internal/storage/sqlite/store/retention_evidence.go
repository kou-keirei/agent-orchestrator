package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"unicode/utf8"
)

// RetentionEvidenceReason names a protection or an unresolved native contract.
type RetentionEvidenceReason string

const (
	// RetentionEvidenceIdentity means stored associations cannot be validated.
	RetentionEvidenceIdentity RetentionEvidenceReason = "IDENTITY_LINK_INVALID_OR_MISSING"
	// RetentionEvidenceMetadata means a scalar is absent, oversized or malformed.
	RetentionEvidenceMetadata RetentionEvidenceReason = "METADATA_UNKNOWN"
	// RetentionEvidenceTurn means no event-specific native turn/item/epoch exists.
	RetentionEvidenceTurn RetentionEvidenceReason = "EVENT_TURN_ITEM_PROVIDER_EPOCH_UNKNOWN"
	// RetentionEvidenceACK means no event-specific provider-native replay ACK exists.
	RetentionEvidenceACK RetentionEvidenceReason = "NATIVE_REPLAY_ACK_UNKNOWN"
	// RetentionEvidenceCheckpoint means session observations do not cover events.
	RetentionEvidenceCheckpoint RetentionEvidenceReason = "EVENT_WINDOW_CHECKPOINT_COMPLETENESS_UNKNOWN"
	// RetentionEvidenceReview means review-run records do not release event holds.
	RetentionEvidenceReview RetentionEvidenceReason = "INDEPENDENT_REVIEW_OBLIGATION_UNKNOWN"
	// RetentionEvidenceBranchReplay means branch selection is not replay coverage.
	RetentionEvidenceBranchReplay RetentionEvidenceReason = "BRANCH_REPLAY_COVERAGE_UNKNOWN"
	// RetentionEvidenceProjection means archive/projection atomicity is not coverage.
	RetentionEvidenceProjection RetentionEvidenceReason = "EVENT_PROJECTION_COMPLETENESS_UNKNOWN"
	// RetentionEvidenceRecovery includes unresolved host/provenance/recovery holds.
	RetentionEvidenceRecovery RetentionEvidenceReason = "HOST_PROVENANCE_RECOVERY_POLICY_UNKNOWN"
	// RetentionEvidenceIdentified protects provider deduplication evidence.
	RetentionEvidenceIdentified RetentionEvidenceReason = "IDENTIFIED_EVENT_RETAIN"
	// RetentionEvidenceMethod protects every method except anonymous message deltas.
	RetentionEvidenceMethod RetentionEvidenceReason = "METHOD_EVIDENCE_RETAIN"
	// RetentionEvidenceEdited preserves evidence on a stored edit/replacement branch.
	RetentionEvidenceEdited RetentionEvidenceReason = "EDIT_BRANCH_EVIDENCE_RETAIN"
	// RetentionEvidenceUnsettled preserves unresolved session checkpoint observations.
	RetentionEvidenceUnsettled RetentionEvidenceReason = "UNSETTLED_CHECKPOINT_RETAIN"
)

// RetentionEvidenceItem separates validated stored associations from native
// ownership. Empty metadata plus METADATA_UNKNOWN is never an inferred identity.
// Branch and checkpoint fields are snapshot observations, NOT event turn links,
// historical provider attestations, replay ACKs, or completeness certificates.
type RetentionEvidenceItem struct {
	EventID                     int64                     `json:"event_id"`
	ConversationID              string                    `json:"conversation_id"`
	SessionID                   string                    `json:"session_id"`
	BranchID                    string                    `json:"branch_id"`
	ProviderEventID             string                    `json:"provider_event_id"`
	Method                      string                    `json:"method"`
	ConversationBinding         RetentionFact             `json:"conversation_binding"`
	SessionBinding              RetentionFact             `json:"session_binding"`
	BranchBinding               RetentionFact             `json:"branch_binding"`
	EventNativeTurnItemBinding  RetentionFact             `json:"event_native_turn_item_binding"`
	ObservedBranchNativeID      string                    `json:"observed_branch_native_id"`
	ObservedBranchProviderScope string                    `json:"observed_branch_provider_scope"`
	ObservedParentBranchID      string                    `json:"observed_parent_branch_id"`
	ObservedReplacedTurnID      string                    `json:"observed_replaced_turn_id"`
	ObservedReplacementTurnID   string                    `json:"observed_replacement_turn_id"`
	ObservedReplacementState    string                    `json:"observed_replacement_state"`
	ObservedReplayStrategy      string                    `json:"observed_replay_strategy"`
	ObservedCheckpointState     string                    `json:"observed_checkpoint_state"`
	ObservedCheckpointNativeID  string                    `json:"observed_checkpoint_native_id"`
	ObservedCheckpointTurnID    string                    `json:"observed_checkpoint_turn_id"`
	LogicalPayloadBytes         *int64                    `json:"logical_payload_bytes"`
	Class                       string                    `json:"class"`
	Reasons                     []RetentionEvidenceReason `json:"reasons"`
}

// RetentionEvidencePage is semantic source mapping for a bounded event window.
// WindowComplete refers only to traversal. CoverageComplete is always false.
type RetentionEvidencePage struct {
	Version                   string                  `json:"version"`
	SnapshotID                string                  `json:"snapshot_id"`
	CallerSnapshotID          string                  `json:"caller_snapshot_id"`
	SnapshotBinding           string                  `json:"snapshot_binding"`
	SourceMaxID               int64                   `json:"source_max_id"`
	HighWaterID               int64                   `json:"high_water_id"`
	Page                      int                     `json:"page"`
	TotalScannedRows          int64                   `json:"total_scanned_rows"`
	Items                     []RetentionEvidenceItem `json:"items"`
	WindowComplete            bool                    `json:"window_complete"`
	CoverageComplete          bool                    `json:"coverage_complete"`
	StopReason                string                  `json:"stop_reason"`
	TraversalStopReason       string                  `json:"traversal_stop_reason"`
	DeletionAuthority         string                  `json:"deletion_authority"`
	FilesystemReclaimEstimate string                  `json:"filesystem_reclaim_estimate"`
	Next                      *RetentionCensusCursor  `json:"next"`
}

// RetentionEvidence reuses the census's bounded native SQLite read-transaction
// lifetime and cursor identity, without changing or running the original census.
//
// Source map at d3d6b8ae: 0066/0087/0101 and InsertConversationProviderEvent
// persist conversation/session/branch associations and dedupe IDs. They persist
// no event turn/item or controller generation. Branch native handles are mutable
// observations, including repeated ownership epochs; current session provider
// identity and active_branch_id must never replace historical event associations.
// 0110/0147 replay metadata controls selection, not reconstruction completeness.
// 0137 edit deliveries bind requests/turns/branches, not raw archive IDs. Branch
// replacement turn refs can be observed without attributing an event to a turn.
// 0141-0145 checkpoints are session observations (checkpoint_turn_id is native,
// not an AO turn FK). NativeCheckpointEvidence JSON is deliberately not loaded.
// Review-run evidence likewise has no event-window release contract.
//
// SOURCE_GAP: eligibility needs an existing native owner to bind exact event IDs
// to conversation, branch, provider/epoch, native turn/item, projection coverage
// and a covering checkpoint/replay ACK. Branch/host reconstruction and independent
// review/provenance/recovery/policy holds must be resolved separately at that
// snapshot. This adapter supplies no such proof, ACK, deletion or provider calls.
type RetentionEvidence struct {
	census *RetentionCensus
}

// BeginRetentionEvidence pins all association reads to the same snapshot as the
// event high-water. The existing census resource limits apply without defaults.
func (s *Store) BeginRetentionEvidence(ctx context.Context, options RetentionCensusOptions) (*RetentionEvidence, error) {
	c, err := s.BeginRetentionCensus(ctx, options)
	if err != nil {
		return nil, err
	}
	return &RetentionEvidence{census: c}, nil
}

// Close abandons the read transaction; it never commits.
func (e *RetentionEvidence) Close() error { return e.census.Close() }

// Page accepts only the last cursor issued by this still-open transaction.
// Both contexts cancel reads; failures return no partial evidence and close it.
func (e *RetentionEvidence) Page(ctx context.Context, cursor *RetentionCensusCursor) (RetentionEvidencePage, error) {
	c := e.census
	c.mu.Lock()
	defer c.mu.Unlock()
	var empty RetentionEvidencePage
	if c.closed {
		return empty, ErrRetentionCensusClosed
	}
	if err := c.ctx.Err(); err != nil {
		_ = c.closeLocked()
		return empty, err
	}
	if (cursor == nil) != (c.next == nil) || (cursor != nil && *cursor != *c.next) {
		return empty, ErrRetentionCensusCursor
	}
	pageCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	p, err := e.pageLocked(pageCtx)
	if err != nil {
		_ = c.closeLocked()
		return empty, err
	}
	return p, nil
}

// Scalars are bounded in SQL before transfer. LEFT JOINs are primary-key probes,
// never timestamp/text/provider-ID guesses or unbounded lineage/turn scans.
func retentionEvidenceScalar(column string) string {
	return "CASE WHEN typeof(" + column + ") = 'text' AND octet_length(" + column + ") <= 256 THEN " + column + " ELSE NULL END"
}

func (e *RetentionEvidence) pageLocked(ctx context.Context) (RetentionEvidencePage, error) {
	c := e.census
	p := RetentionEvidencePage{
		Version: "ao16.retention-evidence.v1", SnapshotID: c.id,
		CallerSnapshotID: c.options.CallerSnapshotID, SnapshotBinding: "SQLITE_READ_TRANSACTION",
		SourceMaxID: c.sourceMax, HighWaterID: c.highWater, Page: c.pages + 1,
		Items: []RetentionEvidenceItem{}, StopReason: "SOURCE_GAP", TraversalStopReason: "PAGE_ROWS",
		DeletionAuthority: "NONE", FilesystemReclaimEstimate: "UNKNOWN",
	}
	after, comparison := int64(math.MinInt64), ">="
	if c.next != nil {
		after, comparison = c.next.AfterID, ">"
	}
	columns := []string{"e.conversation_id", "e.session_id", "e.branch_id", "e.provider_event_id", "e.method",
		"b.provider_conversation_id", "b.provider_scope_id", "b.parent_branch_id", "b.replaced_turn_id",
		"b.replacement_turn_id", "replacement.state", "b.strategy", "s.conversation_checkpoint_state",
		"s.conversation_checkpoint_native_id", "s.conversation_checkpoint_turn_id"}
	query := "SELECT e.id"
	for i, column := range columns {
		if i >= 7 && i <= 10 { // Nullable lineage refs: distinguish absent from invalid.
			query += ", CASE WHEN " + column + " IS NULL THEN '' ELSE " + retentionEvidenceScalar(column) + " END"
		} else {
			query += ", " + retentionEvidenceScalar(column)
		}
	}
	query += `,
 CASE typeof(e.payload_json) WHEN 'text' THEN octet_length(e.payload_json) WHEN 'blob' THEN length(e.payload_json) ELSE NULL END,
 c.id IS NOT NULL,
 s.id IS NOT NULL AND c.id IS NOT NULL AND s.project_id = c.project_id,
 b.id IS NOT NULL AND b.conversation_id = e.conversation_id,
 (b.parent_branch_id IS NULL OR (parent.id IS NOT NULL AND parent.conversation_id = e.conversation_id))
 AND (b.replaced_turn_id IS NULL OR (replaced.id IS NOT NULL AND replaced.conversation_id = e.conversation_id))
 AND (b.replacement_turn_id IS NULL OR (replacement.id IS NOT NULL AND replacement.conversation_id = e.conversation_id)),
 COALESCE(s.conversation_checkpoint_unsettled, 0),
 typeof(e.provider_event_id) = 'text' AND octet_length(e.provider_event_id) > 0
 FROM conversation_provider_events e
 LEFT JOIN conversations c ON c.id = e.conversation_id
 LEFT JOIN sessions s ON s.id = e.session_id AND s.project_id = c.project_id
 LEFT JOIN conversation_branches b ON b.id = e.branch_id AND b.conversation_id = e.conversation_id
 LEFT JOIN conversation_branches parent ON parent.id = b.parent_branch_id AND parent.conversation_id = e.conversation_id
 LEFT JOIN conversation_turns replaced ON replaced.id = b.replaced_turn_id AND replaced.conversation_id = e.conversation_id
 LEFT JOIN conversation_turns replacement ON replacement.id = b.replacement_turn_id AND replacement.conversation_id = e.conversation_id
 WHERE e.id ` + comparison + ` ? AND e.id <= ? ORDER BY e.id LIMIT ?`
	rows, err := c.tx.QueryContext(ctx, query, after, c.highWater, c.options.PageRows)
	if err != nil {
		return p, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item RetentionEvidenceItem
		var values [15]sql.NullString
		var size sql.NullInt64
		var conversation, session, branch, refs, identified bool
		var unsettled int64
		args := []any{&item.EventID}
		for i := range values {
			args = append(args, &values[i])
		}
		args = append(args, &size, &conversation, &session, &branch, &refs, &unsettled, &identified)
		if err := rows.Scan(args...); err != nil {
			return p, err
		}
		classifyRetentionEvidence(&item, values, size, conversation, session, branch, refs, unsettled != 0, identified)
		p.Items = append(p.Items, item)
		after = item.EventID
	}
	if err := rows.Err(); err != nil {
		return p, err
	}
	if err := rows.Close(); err != nil {
		return p, err
	}
	var more bool
	if err := c.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM conversation_provider_events WHERE id > ? AND id <= ?)`, after, c.highWater).Scan(&more); err != nil {
		return p, err
	}
	p.TotalScannedRows = c.scanned + int64(len(p.Items))
	p.WindowComplete = !more
	if !more {
		p.TraversalStopReason = "WINDOW_COMPLETE"
	} else if p.Page == c.options.MaxPages {
		p.TraversalStopReason = "PAGE_LIMIT"
	} else {
		p.Next = &RetentionCensusCursor{SnapshotID: c.id, AfterID: after, Page: p.Page}
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	if len(encoded) > c.options.MaxReportBytes {
		return p, errors.New("retention evidence report byte limit exceeded")
	}
	if err := ctx.Err(); err != nil {
		return p, err
	}
	c.pages++
	c.scanned = p.TotalScannedRows
	if p.Next == nil {
		if err := c.closeLocked(); err != nil {
			return p, err
		}
	} else {
		next := *p.Next
		c.next = &next
	}
	return p, nil
}

func classifyRetentionEvidence(item *RetentionEvidenceItem, values [15]sql.NullString, size sql.NullInt64,
	conversation, session, branch, refs, unsettled, identified bool,
) {
	item.Class = "UNKNOWN_REVIEW_REQUIRED"
	item.EventNativeTurnItemBinding = RetentionUnknown
	item.ConversationBinding, item.SessionBinding, item.BranchBinding = RetentionUnknown, RetentionUnknown, RetentionUnknown
	item.Reasons = []RetentionEvidenceReason{RetentionEvidenceTurn, RetentionEvidenceACK,
		RetentionEvidenceCheckpoint, RetentionEvidenceReview, RetentionEvidenceBranchReplay,
		RetentionEvidenceProjection, RetentionEvidenceRecovery}
	dest := []*string{&item.ConversationID, &item.SessionID, &item.BranchID, &item.ProviderEventID, &item.Method,
		&item.ObservedBranchNativeID, &item.ObservedBranchProviderScope, &item.ObservedParentBranchID,
		&item.ObservedReplacedTurnID, &item.ObservedReplacementTurnID, &item.ObservedReplacementState,
		&item.ObservedReplayStrategy, &item.ObservedCheckpointState, &item.ObservedCheckpointNativeID, &item.ObservedCheckpointTurnID}
	metadataUnknown := !size.Valid
	for i, value := range values {
		if !value.Valid || !utf8.ValidString(value.String) {
			metadataUnknown = true
			continue
		}
		*dest[i] = value.String
	}
	if conversation && item.ConversationID != "" {
		item.ConversationBinding = RetentionYes
	}
	if session && item.SessionID != "" && item.ConversationBinding == RetentionYes {
		item.SessionBinding = RetentionYes
	}
	if branch && item.BranchID != "" && item.ConversationBinding == RetentionYes {
		item.BranchBinding = RetentionYes
	}
	if item.ConversationBinding != RetentionYes || item.SessionBinding != RetentionYes || item.BranchBinding != RetentionYes || !refs {
		item.Reasons = append(item.Reasons, RetentionEvidenceIdentity)
	}
	if metadataUnknown {
		item.Reasons = append(item.Reasons, RetentionEvidenceMetadata)
	}
	if size.Valid {
		n := size.Int64
		item.LogicalPayloadBytes = &n
	}
	if identified {
		item.Reasons = append(item.Reasons, RetentionEvidenceIdentified)
		item.Class = "KEEP"
	}
	if item.Method != "message.delta" {
		item.Reasons = append(item.Reasons, RetentionEvidenceMethod)
		item.Class = "KEEP"
	}
	if item.ObservedReplacedTurnID != "" || item.ObservedReplacementTurnID != "" {
		item.Reasons = append(item.Reasons, RetentionEvidenceEdited)
		item.Class = "KEEP"
	}
	if unsettled {
		item.Reasons = append(item.Reasons, RetentionEvidenceUnsettled)
		item.Class = "KEEP"
	}
	sort.Slice(item.Reasons, func(i, j int) bool { return item.Reasons[i] < item.Reasons[j] })
}
