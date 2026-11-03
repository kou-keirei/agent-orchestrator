package sqlite

import (
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// A pre-contract unbound row has no evidence that it was never sent. In
// particular, its queued state must not authorize replay after an upgrade.
func TestOrdinarySendLegacyMigrationDoesNotReplayOrSettle(t *testing.T) {
	db := openTestDB(t)
	upTo(t, db, 156)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	mustExec(t, db, `INSERT INTO projects (id, path, registered_at) VALUES ('p', '/p', ?)`, now)
	mustExec(t, db, `INSERT INTO sessions
		(id, project_id, num, kind, harness, activity_state, activity_last_at, session_mode,
		controller_generation, created_at, updated_at)
		VALUES ('p-1', 'p', 1, 'worker', 'codex', 'idle', ?, 'chat', 'g1', ?, ?)`, now, now, now)
	mustExec(t, db, `INSERT INTO conversations
		(id, scope, project_id, session_id, current_session_id, created_at, updated_at)
		VALUES ('c', 'session', 'p', 'p-1', 'p-1', ?, ?)`, now, now)
	mustExec(t, db, `INSERT INTO conversation_turns
		(id, conversation_id, handled_by_session_id, controller_generation, state, requested_at)
		VALUES ('a', 'c', 'p-1', 'g1', 'queued', ?)`, now)
	mustExec(t, db, `INSERT INTO conversation_messages
		(id, conversation_id, turn_id, sequence, role, origin, text, client_message_id, created_at, updated_at)
		VALUES ('m', 'c', 'a', 1, 'user', 'human', 'possibly delivered A', 'client-a', ?, ?)`, now, now)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	s := sqlitestore.NewStore(db, db)
	if queued, err := s.NextQueuedTurn(t.Context(), "c"); err == nil {
		t.Errorf("legacy unknown attempt authorized provider replay: %+v", queued)
	}
	if err := s.SettleTurnByID(t.Context(), "a", domain.TurnStateFailed, "controller replaced", now); err == nil {
		t.Error("legacy unknown attempt falsely accepted generic terminal settlement")
	}
	if err := s.SettleOrphanedTurns(t.Context(), "p-1", now); err != nil {
		t.Fatal(err)
	}
	turn, err := s.TurnByID(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if turn.State != domain.TurnStateQueued || turn.ProviderTurnID != "" || turn.CompletedAt != nil {
		t.Errorf("legacy uncertainty destroyed by cleanup: %+v", turn)
	}
	attempt, err := s.OrdinarySendAttempt(t.Context(), "a")
	if err != nil || attempt.State != domain.ConversationSendLegacy || attempt.ClientMessageID != "client-a" || attempt.ReservedAt != nil {
		t.Fatalf("legacy quarantine readback: %+v %v", attempt, err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	again, err := s.OrdinarySendAttempt(t.Context(), "a")
	if err != nil || again != attempt {
		t.Fatalf("repeat migration changed quarantine: %+v %v", again, err)
	}
}
