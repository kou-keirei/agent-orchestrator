package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// Both append-and-reserve and ordinary queue drain use this boundary in the
// transaction which grants dispatch. No provider call is authorized until COMMIT.
func admitOrdinarySendBoundary(ctx context.Context, db gen.DBTX, turnID string, now time.Time) error {
	if db == nil || now.IsZero() {
		return domain.ErrConversationSendConflict
	}
	result, err := db.ExecContext(ctx, `INSERT INTO conversation_send_dispatch_boundaries
		(turn_id, conversation_id, branch_id, session_id, client_message_id, admission_generation, admitted_at)
		SELECT t.id, t.conversation_id, t.branch_id, t.handled_by_session_id,
		       m.client_message_id, t.controller_generation, ?
		FROM conversation_turns t JOIN conversation_messages m ON m.turn_id = t.id AND m.role = 'user'
		WHERE t.id = ?`, now, turnID)
	return ordinarySendBoundaryWrite(result, err)
}

func commitOrdinarySendBoundary(ctx context.Context, db gen.DBTX, owner domain.ConversationSendOwner, now time.Time) error {
	if db == nil || now.IsZero() {
		return domain.ErrConversationSendConflict
	}
	// An older untouched pending row may still be dispatched normally. Capture
	// its exact call boundary without inventing immutable admission history.
	inserted, err := db.ExecContext(ctx, `INSERT INTO conversation_send_dispatch_boundaries
		(turn_id, conversation_id, branch_id, session_id, client_message_id,
		 admission_generation, call_generation, call_committed_at)
		SELECT t.id, t.conversation_id, t.branch_id, t.handled_by_session_id,
		       m.client_message_id, t.controller_generation, ?, ?
		FROM conversation_turns t JOIN conversation_messages m ON m.turn_id = t.id AND m.role = 'user'
		WHERE t.id = ? AND NOT EXISTS
		    (SELECT 1 FROM conversation_send_dispatch_boundaries b WHERE b.turn_id = t.id)`,
		owner.ControllerGeneration, now, owner.TurnID)
	if err != nil {
		return err
	}
	insertedRows, err := inserted.RowsAffected()
	if err != nil {
		return err
	}
	// The pending->reserved CAS in the SAME transaction already checked current
	// session/generation, active branch, client key and unresolved/running work.
	// Check immutable identity too, so later mutable rows cannot substitute it.
	var matches bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM conversation_send_dispatch_boundaries b
		JOIN conversation_turns t ON t.id = b.turn_id
		WHERE b.turn_id = ? AND b.conversation_id = ? AND b.branch_id = ?
		  AND b.session_id = ? AND b.client_message_id = ?
		  AND b.admission_generation = t.controller_generation
		  AND (b.call_committed_at IS NULL OR ? = 1))`,
		owner.TurnID, owner.ConversationID, owner.BranchID, owner.SessionID, owner.ClientMessageID,
		insertedRows).Scan(&matches)
	if err != nil {
		return err
	}
	if !matches {
		return domain.ErrConversationSendConflict
	}
	if insertedRows == 1 {
		return nil
	}
	result, err := db.ExecContext(ctx, `UPDATE conversation_send_dispatch_boundaries
		SET call_generation = ?, call_committed_at = ? WHERE turn_id = ? AND call_committed_at IS NULL`,
		owner.ControllerGeneration, now, owner.TurnID)
	return ordinarySendBoundaryWrite(result, err)
}

func ordinarySendBoundaryWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return domain.ErrConversationSendConflict
	}
	return nil
}

// OrdinarySendDispatchBoundary reads facts only. It never grants a call or
// replay. A negative is available only for a new, exact, still-pending admission
// in its live original generation. Missing/old/mismatched rows remain UNKNOWN.
// CallCommitted remains uncertain even after provider binding or deferred setup.
func (s *Store) OrdinarySendDispatchBoundary(ctx context.Context, owner domain.ConversationSendOwner) (domain.ConversationSendDispatchBoundary, error) {
	b := domain.ConversationSendDispatchBoundary{State: domain.ConversationSendDispatchUnknown}
	// Never read an uncommitted projection as write-ahead evidence.
	if q, ok := ctx.Value(conversationProjectionTxKey{}).(*gen.Queries); ok && q != nil {
		return b, domain.ErrConversationSendConflict
	}
	var admitted, called sql.NullTime
	var callGeneration string
	var untouched bool
	err := s.readDB.QueryRowContext(ctx, `SELECT b.conversation_id, b.turn_id, b.client_message_id,
		b.branch_id, b.session_id, b.admission_generation, b.admitted_at, b.call_generation, b.call_committed_at,
		(a.state = 'pending' AND t.state = 'queued' AND t.provider_turn_id = ''
		 AND t.rolled_back_at IS NULL AND t.promotion_started_at IS NULL
		 AND t.conversation_id = b.conversation_id AND t.branch_id = b.branch_id
		 AND t.handled_by_session_id = b.session_id AND t.controller_generation = b.admission_generation
		 AND m.client_message_id = b.client_message_id AND c.current_session_id = b.session_id
		 AND c.active_branch_id = b.branch_id AND s.controller_generation = b.admission_generation
		 AND s.session_mode = 'chat' AND s.is_terminated = 0)
		FROM conversation_send_dispatch_boundaries b
		JOIN conversation_turns t ON t.id = b.turn_id
		JOIN conversation_messages m ON m.turn_id = t.id AND m.role = 'user'
		JOIN conversation_send_attempts a ON a.turn_id = t.id
		JOIN conversations c ON c.id = t.conversation_id
		JOIN sessions s ON s.id = b.session_id WHERE b.turn_id = ?`, owner.TurnID).Scan(
		&b.ConversationID, &b.TurnID, &b.ClientMessageID, &b.BranchID, &b.SessionID,
		&b.AdmissionGeneration, &admitted, &callGeneration, &called, &untouched)
	if errors.Is(err, sql.ErrNoRows) {
		return b, nil
	}
	if err != nil {
		return b, err
	}
	b.ControllerGeneration = b.AdmissionGeneration
	if admitted.Valid {
		b.AdmittedAt = &admitted.Time
	}
	if called.Valid {
		b.CallCommittedAt = &called.Time
		b.ControllerGeneration = callGeneration
	}
	if b.ConversationSendOwner != owner || owner.ClientMessageID == "" || owner.ControllerGeneration == "" {
		return b, nil
	}
	if called.Valid {
		b.State = domain.ConversationSendDispatchCallCommitted
	} else if admitted.Valid && untouched {
		b.State = domain.ConversationSendDispatchNeverCalled
	}
	return b, nil
}
