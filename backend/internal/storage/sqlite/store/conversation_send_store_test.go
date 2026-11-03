package store_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type ordinarySendFixture struct {
	s     *sqlite.Store
	db    *sql.DB
	dir   string
	owner domain.ConversationSendOwner
}

func newOrdinarySendFixture(t *testing.T) *ordinarySendFixture {
	t.Helper()
	f := &ordinarySendFixture{dir: t.TempDir()}
	var err error
	f.s, err = sqlitetest.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.s.Close(); err != nil {
			t.Error(err)
		}
	})
	seedProject(t, f.s, "send")
	rec := sampleRecord("send")
	rec.Mode = domain.SessionModeChat
	rec.Metadata.ControllerGeneration = "g1"
	rec.Metadata.ProviderConversationID = "native-thread"
	session, err := f.s.CreateSession(t.Context(), rec)
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.s.CreateConversation(t.Context(), "send-conversation", domain.ConversationScopeSession, "send", session.ID, testNow)
	if err != nil {
		t.Fatal(err)
	}
	f.owner = domain.ConversationSendOwner{ConversationID: c.ID, BranchID: c.ActiveBranchID, SessionID: session.ID, ControllerGeneration: "g1"}
	f.db, err = sql.Open("sqlite", filepath.Join(f.dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.db.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *ordinarySendFixture) append(t *testing.T, id string) domain.ConversationSendOwner {
	t.Helper()
	owner := f.owner
	owner.TurnID = id
	owner.ClientMessageID = "client-" + id
	created, err := f.s.AppendUserMessage(t.Context(), owner.ConversationID, owner.SessionID, owner.ControllerGeneration,
		domain.ConversationMessage{ID: "message-" + id, ClientMessageID: owner.ClientMessageID, Origin: domain.MessageOriginHuman, Text: "prompt " + id}, id, testNow)
	if err != nil || !created {
		t.Fatalf("append: %v %v", created, err)
	}
	return owner
}

func assertOrdinarySendQueued(t *testing.T, f *ordinarySendFixture, owner domain.ConversationSendOwner, state domain.ConversationSendState) {
	t.Helper()
	turn, err := f.s.TurnByID(t.Context(), owner.TurnID)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := f.s.OrdinarySendAttempt(t.Context(), owner.TurnID)
	if err != nil {
		t.Fatal(err)
	}
	if turn.State != domain.TurnStateQueued || turn.ProviderTurnID != "" || turn.StartedAt != nil || turn.CompletedAt != nil || attempt.State != state || attempt.ClientMessageID != owner.ClientMessageID || attempt.BranchID != owner.BranchID || attempt.SessionID != owner.SessionID {
		t.Fatalf("identity or lifecycle changed: turn=%+v attempt=%+v", turn, attempt)
	}
}

func TestOrdinarySendCrashWindowsAndReplacement(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendPending)
	// A crash before reservation leaves genuine pending work. Replacement owns
	// a fresh generation, but the immutable admission generation stays g1.
	if err := f.s.ClaimChatControllerGeneration(t.Context(), a.SessionID, "g2"); err != nil {
		t.Fatal(err)
	}
	a.ControllerGeneration = "g2"
	message, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow)
	if err != nil || message.TurnID != a.TurnID || message.Text != "prompt a" {
		t.Fatalf("reserve fresh: %+v %v", message, err)
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.s, err = sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.ClaimChatControllerGeneration(t.Context(), a.SessionID, "g3"); err != nil {
		t.Fatal(err)
	}
	// Crash after commit and before SendTurn is conservatively uncertain. The
	// same state also covers unknown/no-ref results and post-send bind failures.
	attempt, err := f.s.OrdinarySendAttempt(t.Context(), a.TurnID)
	if err != nil || attempt.State != domain.ConversationSendReserved || attempt.ControllerGeneration != "g2" || attempt.ReservedAt == nil {
		t.Fatalf("restart readback: %+v %v", attempt, err)
	}
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("same-ID replay: %v", err)
	}
	b := f.append(t, "b")
	b.ControllerGeneration = "g3"
	if _, err := f.s.ReserveOrdinarySend(t.Context(), b, testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("B crossed unresolved A: %v", err)
	}
	if _, err := f.s.NextQueuedTurn(t.Context(), a.ConversationID); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("drain crossed A: %v", err)
	}
	if err := f.s.BindReservedOrdinarySend(t.Context(), a, "native-a", testNow); !errors.Is(err, domain.ErrConversationSendConflict) {
		t.Fatalf("stale binding: %v", err)
	}
	a.ControllerGeneration = "g3"
	if err := f.s.ConfirmOrdinarySendNotDelivered(t.Context(), a, "foreign-generation-proof", testNow); !errors.Is(err, domain.ErrConversationSendConflict) {
		t.Fatalf("successor discarded prior attempt: %v", err)
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
}

func TestOrdinarySendReservationFailureForbidsNativeCall(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	if _, err := f.db.Exec(`CREATE TRIGGER reject_send_reservation BEFORE UPDATE ON conversation_send_attempts
		WHEN NEW.state = 'reserved' BEGIN SELECT RAISE(ABORT, 'injected durable write failure'); END`); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	// Exact caller boundary contract for the separately owned controller.
	send := func() error {
		message, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow)
		if err != nil {
			return err
		}
		if message.ClientMessageID != a.ClientMessageID {
			t.Fatal("substituted prompt")
		}
		attempt, err := f.s.OrdinarySendAttempt(t.Context(), a.TurnID)
		if err != nil || attempt.State != domain.ConversationSendReserved || attempt.ReservedAt == nil {
			t.Fatalf("native call before committed reservation: %+v %v", attempt, err)
		}
		calls.Add(1) // fake native SendTurn
		return nil
	}
	if err := send(); err == nil || calls.Load() != 0 {
		t.Fatalf("failed persistence escaped: calls=%d err=%v", calls.Load(), err)
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendPending)
	if _, err := f.db.Exec(`DROP TRIGGER reject_send_reservation`); err != nil {
		t.Fatal(err)
	}
	if err := send(); err != nil {
		t.Fatal(err)
	}
	if err := send(); !errors.Is(err, domain.ErrConversationSendUncertain) || calls.Load() != 1 {
		t.Fatalf("retry delivered twice: calls=%d err=%v", calls.Load(), err)
	}
}

func TestOrdinarySendBindingFailureAndGenericLifecycleStayHeld(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
		t.Fatal(err)
	}
	if err := f.s.BindReservedOrdinarySend(t.Context(), a, "", testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("empty reference: %v", err)
	}
	if _, err := f.db.Exec(`CREATE TRIGGER reject_send_binding BEFORE UPDATE OF provider_turn_id ON conversation_turns
		WHEN NEW.provider_turn_id <> '' BEGIN SELECT RAISE(ABORT, 'injected bind failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.s.BindReservedOrdinarySend(t.Context(), a, "native-a", testNow); err == nil {
		t.Fatal("bind failure accepted")
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
	if err := f.s.BindTurnToProvider(t.Context(), a.TurnID, "unfenced-native", testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("unfenced binding: %v", err)
	}
	if err := f.s.SettleTurnByID(t.Context(), a.TurnID, domain.TurnStateFailed, "timeout", testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("false terminal: %v", err)
	}
	if err := f.s.SettleTurn(t.Context(), a.ConversationID, "", domain.TurnStateCompleted, "", testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("uncorrelated terminal: %v", err)
	}
	if err := f.s.CancelAllQueuedTurns(t.Context(), a.ConversationID, testNow); err != nil {
		t.Fatal(err)
	}
	if err := f.s.CancelQueuedTurns(t.Context(), a.ConversationID, testNow, testNow); err != nil {
		t.Fatal(err)
	}
	if err := f.s.CancelQueuedTurnByID(t.Context(), a.ConversationID, a.TurnID, testNow); err == nil {
		t.Fatal("cancelled unresolved send")
	}
	if err := f.s.SettleOrphanedTurns(t.Context(), a.SessionID, testNow); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.ReserveQueuedTurnForPromotion(t.Context(), a.ConversationID, a.TurnID, testNow); err == nil {
		t.Fatal("promoted unresolved send")
	}
	if _, err := f.s.QueuedTurnMessage(t.Context(), a.ConversationID, a.TurnID); err == nil {
		t.Fatal("offered dispatched prompt for editing")
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
}

func TestOrdinarySendSQLConstraintsAndAtomicAdmission(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	for _, statement := range []string{
		`INSERT INTO conversation_send_attempts(turn_id, conversation_id, state) VALUES ('a', 'send-conversation', 'pending')`,
		`UPDATE conversation_send_attempts SET state = 'reserved' WHERE turn_id = 'a'`,
		`UPDATE conversation_send_attempts SET state = 'accepted' WHERE turn_id = 'a'`,
	} {
		if _, err := f.db.Exec(statement); err == nil {
			t.Fatalf("invalid SQL state accepted: %s", statement)
		}
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendPending)
	if _, err := f.db.Exec(`CREATE TRIGGER reject_send_admission BEFORE INSERT ON conversation_send_attempts
		BEGIN SELECT RAISE(ABORT, 'injected pending insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	created, err := f.s.AppendUserMessage(t.Context(), f.owner.ConversationID, f.owner.SessionID, "g1",
		domain.ConversationMessage{ID: "message-b", ClientMessageID: "client-b", Origin: domain.MessageOriginHuman, Text: "b"}, "b", testNow)
	if err == nil || created {
		t.Fatalf("partial admission: %v %v", created, err)
	}
	if _, err := f.s.TurnByID(t.Context(), "b"); !errors.Is(err, domain.ErrNoConversationTurn) {
		t.Fatalf("turn escaped rollback: %v", err)
	}
	if _, found, err := f.s.ConversationMessageByClientID(t.Context(), f.owner.ConversationID, "client-b"); err != nil || found {
		t.Fatalf("message escaped rollback: %v %v", found, err)
	}
}

func TestOrdinarySendUnknownCallerBoundaryAcrossReplacement(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	var calls int
	send := func(owner domain.ConversationSendOwner) error {
		if _, err := f.s.ReserveOrdinarySend(t.Context(), owner, testNow); err != nil {
			return err
		}
		calls++ // native SendTurn delivered, but lost the result/reference
		return f.s.BindReservedOrdinarySend(t.Context(), owner, "", testNow)
	}
	if err := send(a); !errors.Is(err, domain.ErrConversationSendUncertain) || calls != 1 {
		t.Fatalf("unknown result: calls=%d %v", calls, err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.s, err = sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.s.ClaimChatControllerGeneration(t.Context(), a.SessionID, "g2"); err != nil {
		t.Fatal(err)
	}
	a.ControllerGeneration = "g2"
	// Both ordinary and G1 same-ID callers must consume typed uncertainty, not
	// turn the durable queued row into a positive duplicate acknowledgement.
	for _, caller := range []string{"ordinary", "G1"} {
		if err := send(a); !errors.Is(err, domain.ErrConversationSendUncertain) {
			t.Fatalf("%s same-ID recovery: %v", caller, err)
		}
	}
	b := f.append(t, "b")
	b.ControllerGeneration = "g2"
	if err := send(b); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("new-ID B: %v", err)
	}
	if _, err := f.s.NextQueuedTurn(t.Context(), a.ConversationID); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("queue drain A: %v", err)
	}
	if calls != 1 {
		t.Fatalf("replayed unknown delivery: calls=%d", calls)
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
}

func TestOrdinarySendRaceAndExactOwnerFence(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	for _, mutate := range []func(*domain.ConversationSendOwner){
		func(o *domain.ConversationSendOwner) { o.ConversationID = "foreign" },
		func(o *domain.ConversationSendOwner) { o.TurnID = "foreign" },
		func(o *domain.ConversationSendOwner) { o.ClientMessageID = "foreign" },
		func(o *domain.ConversationSendOwner) { o.BranchID = "foreign" },
		func(o *domain.ConversationSendOwner) { o.SessionID = "foreign" },
		func(o *domain.ConversationSendOwner) { o.ControllerGeneration = "foreign" },
	} {
		bad := a
		mutate(&bad)
		if _, err := f.s.ReserveOrdinarySend(t.Context(), bad, testNow); !errors.Is(err, domain.ErrConversationSendConflict) {
			t.Fatalf("foreign owner reserved: %+v %v", bad, err)
		}
	}
	// Two independent Store objects share only SQLite, not the Go write mutex.
	other, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := other.Close(); err != nil {
			t.Error(err)
		}
	}()
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, s := range []*sqlite.Store{f.s, other} {
		wg.Add(1)
		go func(s *sqlite.Store) {
			defer wg.Done()
			<-start
			if _, err := s.ReserveOrdinarySend(context.Background(), a, testNow); err == nil {
				wins.Add(1)
			}
		}(s)
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("reservation winners=%d", wins.Load())
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
}

func TestOrdinarySendHealthyBindingAndUntouchedQueue(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	b := f.append(t, "b")
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
		t.Fatal(err)
	}
	if err := f.s.BindReservedOrdinarySend(t.Context(), a, "native-a", testNow); err != nil {
		t.Fatal(err)
	}
	turn, err := f.s.TurnByID(t.Context(), a.TurnID)
	if err != nil || turn.State != domain.TurnStateRunning || turn.ProviderTurnID != "native-a" || turn.StartedAt == nil {
		t.Fatalf("binding: %+v %v", turn, err)
	}
	if _, err := f.s.ReserveOrdinarySend(t.Context(), b, testNow); !errors.Is(err, domain.ErrConversationSendConflict) {
		t.Fatalf("sent queue while running: %v", err)
	}
	if err := f.s.SettleTurn(t.Context(), a.ConversationID, "native-a", domain.TurnStateCompleted, "", testNow.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	queued, err := f.s.NextQueuedTurn(t.Context(), a.ConversationID)
	if err != nil || queued.TurnID != b.TurnID {
		t.Fatalf("fresh queue blocked: %+v %v", queued, err)
	}
	if _, err := f.s.ReserveOrdinarySend(t.Context(), b, testNow); err != nil {
		t.Fatal(err)
	}
	if err := f.s.ConfirmOrdinarySendNotDelivered(t.Context(), b, "", testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("unproven non-delivery: %v", err)
	}
	if err := f.s.ConfirmOrdinarySendNotDelivered(t.Context(), b, "native-refusal-for-client-b", testNow); err != nil {
		t.Fatal(err)
	}
	attempt, err := f.s.OrdinarySendAttempt(t.Context(), b.TurnID)
	if err != nil || attempt.State != domain.ConversationSendNotDelivered {
		t.Fatalf("non-delivery: %+v %v", attempt, err)
	}
	if _, err := f.s.NextQueuedTurn(t.Context(), a.ConversationID); !errors.Is(err, domain.ErrNoQueuedTurn) {
		t.Fatalf("non-delivery replay: %v", err)
	}
}

func TestOrdinarySendProjectionContextCannotAuthorizeOrDeadlock(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	result := make(chan error, 1)
	var nativeCalls atomic.Int32
	rollback := errors.New("deliberate projection rollback")
	consumed := false
	_, err := f.s.ProjectProviderEvent(t.Context(), a.ConversationID, a.SessionID, "g1",
		"nested-reserve", "turn.updated", `{}`, testNow, func(txCtx context.Context) error {
			go func() {
				_, err := f.s.ReserveOrdinarySend(txCtx, a, testNow)
				if err == nil {
					nativeCalls.Add(1)
				}
				result <- err
			}()
			select {
			case err := <-result:
				consumed = true
				if !errors.Is(err, domain.ErrConversationSendConflict) {
					t.Errorf("projection admission must reject before commit: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("reservation waited on its projection's already-owned write mutex")
			}
			return rollback
		})
	if !errors.Is(err, rollback) {
		t.Fatalf("outer rollback: %v", err)
	}
	// Release the outer transaction before joining the broken implementation's
	// waiter, so behavioral RED does not strand a goroutine or test DB handle.
	if !consumed {
		select {
		case <-result:
		case <-time.After(3 * time.Second):
			t.Fatal("reservation waiter leaked after rollback")
		}
	}
	if nativeCalls.Load() != 0 {
		t.Fatalf("authorized native I/O before commit: %d", nativeCalls.Load())
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendPending)
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
		t.Fatalf("ordinary admission lost mutex/transaction resource: %v", err)
	}
}

func TestOrdinarySendCommitFailureForbidsNativeCall(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "a")
	// The reservation UPDATE succeeds. The deferred FK rejects only COMMIT on
	// the real FK-enabled Store connection, without a production injection hook.
	if _, err := f.db.Exec(`CREATE TABLE ordinary_send_commit_parent(id INTEGER PRIMARY KEY);
		CREATE TABLE ordinary_send_commit_child(id INTEGER REFERENCES ordinary_send_commit_parent(id) DEFERRABLE INITIALLY DEFERRED);
		CREATE TRIGGER reject_send_commit AFTER UPDATE ON conversation_send_attempts
		WHEN NEW.state = 'reserved' BEGIN INSERT INTO ordinary_send_commit_child VALUES (1); END`); err != nil {
		t.Fatal(err)
	}
	message, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow)
	var nativeCalls int
	if err == nil {
		nativeCalls++
	}
	if err == nil || nativeCalls != 0 || message.TurnID != "" {
		t.Fatalf("commit failure authorized native I/O: %+v calls=%d err=%v", message, nativeCalls, err)
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY") || strings.Contains(err.Error(), "reserve ordinary send:") {
		t.Fatalf("expected deferred COMMIT failure, not a statement failure: %v", err)
	}
	assertOrdinarySendQueued(t, f, a, domain.ConversationSendPending)
	var pendingSideEffects int
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM ordinary_send_commit_child`).Scan(&pendingSideEffects); err != nil || pendingSideEffects != 0 {
		t.Fatalf("commit failure escaped rollback: %d %v", pendingSideEffects, err)
	}
	if _, err := f.db.Exec(`DROP TRIGGER reject_send_commit`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
		t.Fatalf("retry after conclusive transaction rollback: %v", err)
	}
}

func TestOrdinarySendProjectionResolutionRejectsBeforeMutex(t *testing.T) {
	for _, kind := range []string{"binding", "non_delivery"} {
		t.Run(kind, func(t *testing.T) {
			f := newOrdinarySendFixture(t)
			a := f.append(t, "a")
			if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
				t.Fatal(err)
			}
			rollback := errors.New("projection resolution rollback")
			result := make(chan error, 1)
			consumed := false
			_, err := f.s.ProjectProviderEvent(t.Context(), a.ConversationID, a.SessionID, "g1", "nested-resolve", "turn.updated", `{}`, testNow, func(txCtx context.Context) error {
				go func() {
					if kind == "binding" {
						result <- f.s.BindReservedOrdinarySend(txCtx, a, "native-a", testNow)
					} else {
						result <- f.s.ConfirmOrdinarySendNotDelivered(txCtx, a, "native-refusal", testNow)
					}
				}()
				select {
				case err := <-result:
					consumed = true
					if !errors.Is(err, domain.ErrConversationSendConflict) {
						t.Errorf("projection resolution admitted: %v", err)
					}
				case <-time.After(time.Second):
					t.Error("resolution waited on projection-owned write mutex")
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatalf("projection rollback: %v", err)
			}
			if !consumed {
				select {
				case <-result:
				case <-time.After(3 * time.Second):
					t.Fatal("resolution waiter leaked")
				}
			}
			assertOrdinarySendQueued(t, f, a, domain.ConversationSendReserved)
			var archived int
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM conversation_provider_events WHERE conversation_id=?`, a.ConversationID).Scan(&archived); err != nil || archived != 0 {
				t.Fatalf("projection rollback retained archive without resolution: count=%d err=%v", archived, err)
			}
			if err := f.s.BindReservedOrdinarySend(t.Context(), a, "native-a", testNow); err != nil {
				t.Fatalf("ordinary resolution lost resources: %v", err)
			}
		})
	}
}
