package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// OrdinarySendEvidenceRequest is an exact comparison target, not trusted origin
// or send authority. Provider identity must come from AO's current owner.
type OrdinarySendEvidenceRequest struct {
	domain.ConversationSendOwner
	Harness                domain.AgentHarness
	ProviderConversationID string
	ProviderScopeID        string
	ProviderTurnID         string // empty is compared exactly; never a wildcard
}

// SendEvidenceHoldReason describes why a snapshot cannot qualify delivery.
type SendEvidenceHoldReason string

const (
	// SendEvidenceInvalidRequest rejects an incomplete evidence comparison target.
	SendEvidenceInvalidRequest  SendEvidenceHoldReason = "invalid_request"
	// SendEvidenceTransaction rejects a nested evidence snapshot transaction.
	SendEvidenceTransaction     SendEvidenceHoldReason = "nested_transaction"
	// SendEvidenceConcurrentWrite holds when a stable snapshot is unavailable.
	SendEvidenceConcurrentWrite SendEvidenceHoldReason = "concurrent_mutation"
	// SendEvidenceMissing indicates required durable source rows are absent.
	SendEvidenceMissing         SendEvidenceHoldReason = "missing_evidence"
	// SendEvidenceAmbiguous indicates conflicting or incomplete durable evidence.
	SendEvidenceAmbiguous       SendEvidenceHoldReason = "ambiguous_evidence"
	// SendEvidenceIdentity rejects stale or foreign owner and provider identities.
	SendEvidenceIdentity        SendEvidenceHoldReason = "stale_or_foreign_identity"
	// SendEvidenceAdmissionGap marks the missing immutable admission producer.
	SendEvidenceAdmissionGap    SendEvidenceHoldReason = "missing_admission_producer"
	// SendEvidenceNativeGap marks the missing correlated native result producer.
	SendEvidenceNativeGap       SendEvidenceHoldReason = "missing_native_result_producer"
)

// OrdinarySendProducerContract is deliberately unmet by migration 0217. Pending
// rows do not retain immutable admission/request/provider identity. Bound rows
// can precede dispatch; the opaque non-delivery string is not a native ACK.
// A service-owned producer must persist these facts atomically at their real
// boundaries before a store reader can distinguish never-attempted or accepted.
const OrdinarySendProducerContract = "service producer must atomically persist immutable admission (conversation, branch, session, controller generation, turn, client-message/request identity, harness, provider conversation and scope), pre-dispatch reservation bound to that admission, and a correlated native result/ACK (same owner/request/provider identity and provider turn); absence, queued/pending, bound reference, archive payload, or opaque non-delivery text alone cannot prove never-attempted or accepted"

// SendEvidenceHold is a typed UNKNOWN/HOLD, including the missing producer
// contract when applicable. It never permits a send, retry, resume or release.
type SendEvidenceHold struct {
	Reason                  SendEvidenceHoldReason
	MissingProducerContract string
}

func (h *SendEvidenceHold) Error() string {
	return "ordinary send evidence UNKNOWN/HOLD: " + string(h.Reason)
}

// OrdinarySendEvidence contains journal facts only. Even a fully matched bound
// row is UNKNOWN: no current producer durably proves native dispatch acceptance.
// The snapshot expires when the accessor returns and confers no authority.
type OrdinarySendEvidence struct {
	Owner                  domain.ConversationSendOwner
	AdmissionGeneration    string
	Harness                domain.AgentHarness
	ProviderConversationID string
	ProviderScopeID        string
	ProviderTurnID         string
	AttemptState           domain.ConversationSendState
	ReservedAt             *time.Time
	NonDeliveryEvidence    string
	TurnState              domain.TurnState
}

// ReadOrdinarySendEvidence is an opt-in, bounded, data-read-only accessor. It
// rejects a busy local writer or SQLite read-to-write upgrade rather than
// combining stale independent projections. The empty UPDATE reserves SQLite's
// writer without modifying rows or firing CDC triggers. All facts are read in
// that transaction and it is always rolled back. No public port/API opts in.
//
// Every current outcome returns an error; matched facts accompany a typed HOLD
// naming the missing service producer. In particular, missing turns and pending
// attempts are never positive evidence that a prompt was not sent.
func (s *Store) ReadOrdinarySendEvidence(ctx context.Context, want OrdinarySendEvidenceRequest) (OrdinarySendEvidence, error) {
	var empty OrdinarySendEvidence
	hold := func(reason SendEvidenceHoldReason) error { return &SendEvidenceHold{Reason: reason} }
	if want.ConversationID == "" || want.BranchID == "" || want.SessionID == "" ||
		want.ControllerGeneration == "" || want.TurnID == "" || want.ClientMessageID == "" ||
		want.Harness == "" || want.ProviderConversationID == "" || want.ProviderScopeID == "" {
		return empty, hold(SendEvidenceInvalidRequest)
	}
	if q, ok := ctx.Value(conversationProjectionTxKey{}).(*gen.Queries); ok && q != nil {
		return empty, hold(SendEvidenceTransaction)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	select {
	case <-s.writeMu.token:
		defer s.writeMu.Unlock()
	default:
		return empty, hold(SendEvidenceConcurrentWrite)
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return empty, fmt.Errorf("begin send evidence: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fenceOrdinarySendEvidence(ctx, tx, want.TurnID); err != nil {
		var coded interface{ Code() int }
		if errors.As(err, &coded) && coded.Code()&255 == 5 { // SQLITE_BUSY, including BUSY_SNAPSHOT
			return empty, hold(SendEvidenceConcurrentWrite)
		}
		return empty, fmt.Errorf("fence send evidence: %w", err)
	}
	evidence, err := readOrdinarySendEvidence(ctx, tx, want)
	if err != nil {
		return empty, err
	}
	if err := tx.Rollback(); err != nil {
		return empty, fmt.Errorf("close send evidence snapshot: %w", err)
	}
	reason := SendEvidenceNativeGap
	if evidence.AttemptState == domain.ConversationSendPending || evidence.AttemptState == domain.ConversationSendLegacy {
		reason = SendEvidenceAdmissionGap
	}
	return evidence, &SendEvidenceHold{Reason: reason, MissingProducerContract: OrdinarySendProducerContract}
}

func fenceOrdinarySendEvidence(ctx context.Context, tx *sql.Tx, turnID string) error {
	// Pin a read snapshot before upgrading. An existing writer, or a commit
	// between this read and the upgrade, must fail the upgrade immediately.
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conversation_send_attempts WHERE turn_id=?`, turnID).Scan(&count); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE conversation_send_attempts SET state=state WHERE 0`)
	return err
}

func readOrdinarySendEvidence(ctx context.Context, tx *sql.Tx, want OrdinarySendEvidenceRequest) (OrdinarySendEvidence, error) {
	var empty OrdinarySendEvidence
	// LIMIT 2 detects ambiguous user-message joins without loading history or
	// parsing untrusted provider payloads. Join every persisted owning identity;
	// compare the current owner, admission generation and reservation separately.
	rows, err := tx.QueryContext(ctx, `SELECT t.id, t.conversation_id, t.branch_id, t.handled_by_session_id,
		m.client_message_id, t.controller_generation, t.provider_turn_id, t.state,
		a.state, a.controller_generation, a.reserved_at, a.non_delivery_evidence,
		c.active_branch_id, COALESCE(c.current_session_id,''), COALESCE(b.session_id,''),
		b.provider_conversation_id, b.provider_scope_id, b.provider_ids_scoped,
		s.harness, s.provider_conversation_id, s.controller_generation, s.session_mode, s.is_terminated,
		t.rolled_back_at IS NOT NULL OR t.promotion_started_at IS NOT NULL OR t.promoted_to_turn_id IS NOT NULL
		FROM conversation_turns t
		JOIN conversation_send_attempts a ON a.turn_id=t.id AND a.conversation_id=t.conversation_id
		JOIN conversations c ON c.id=t.conversation_id
		JOIN conversation_branches b ON b.id=t.branch_id AND b.conversation_id=c.id
		JOIN sessions s ON s.id=t.handled_by_session_id
		JOIN conversation_messages m ON m.turn_id=t.id AND m.conversation_id=c.id AND m.branch_id=t.branch_id AND m.role='user'
		WHERE t.id=? LIMIT 2`, want.TurnID)
	if err != nil {
		return empty, fmt.Errorf("read send evidence: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var got OrdinarySendEvidence
	var activeBranch, currentSession, branchSession, sessionProvider, currentGeneration, reservationGeneration string
	var mode domain.SessionMode
	var scoped, terminated, unavailable bool
	var reserved sql.NullTime
	count := 0
	for rows.Next() {
		count++
		if count > 1 {
			return empty, &SendEvidenceHold{Reason: SendEvidenceAmbiguous}
		}
		if err := rows.Scan(&got.Owner.TurnID, &got.Owner.ConversationID, &got.Owner.BranchID, &got.Owner.SessionID,
			&got.Owner.ClientMessageID, &got.AdmissionGeneration, &got.ProviderTurnID, &got.TurnState,
			&got.AttemptState, &reservationGeneration, &reserved, &got.NonDeliveryEvidence,
			&activeBranch, &currentSession, &branchSession, &got.ProviderConversationID, &got.ProviderScopeID, &scoped,
			&got.Harness, &sessionProvider, &currentGeneration, &mode, &terminated, &unavailable); err != nil {
			return empty, fmt.Errorf("scan send evidence: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return empty, fmt.Errorf("read send evidence rows: %w", err)
	}
	if count == 0 {
		return empty, &SendEvidenceHold{Reason: SendEvidenceMissing}
	}
	got.Owner.ControllerGeneration = currentGeneration
	if got.Owner != want.ConversationSendOwner || activeBranch != want.BranchID ||
		currentSession != string(want.SessionID) || branchSession != string(want.SessionID) ||
		got.AdmissionGeneration != want.ControllerGeneration ||
		got.Harness != want.Harness || got.ProviderConversationID != want.ProviderConversationID ||
		sessionProvider != want.ProviderConversationID || got.ProviderScopeID != want.ProviderScopeID ||
		got.ProviderTurnID != want.ProviderTurnID || !scoped || mode != domain.SessionModeChat || terminated || unavailable ||
		(reservationGeneration != "" && reservationGeneration != want.ControllerGeneration) {
		return empty, &SendEvidenceHold{Reason: SendEvidenceIdentity}
	}
	if reserved.Valid {
		at := reserved.Time
		got.ReservedAt = &at
	}
	return got, nil
}
