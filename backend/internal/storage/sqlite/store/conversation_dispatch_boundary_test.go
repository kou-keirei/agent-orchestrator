package store_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func assertDispatchBoundary(t *testing.T, s *sqlite.Store, owner domain.ConversationSendOwner, want domain.ConversationSendDispatchState) domain.ConversationSendDispatchBoundary {
	t.Helper()
	b, err := s.OrdinarySendDispatchBoundary(t.Context(), owner)
	if err != nil || b.State != want {
		t.Fatalf("boundary: %+v err=%v want=%s", b, err, want)
	}
	return b
}

func TestDispatchBoundaryCrashWindows(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "boundary-crash")
	before := assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchNeverCalled)
	if before.AdmittedAt == nil || before.CallCommittedAt != nil || before.ConversationSendOwner != a {
		t.Fatalf("admission: %+v", before)
	}
	reopen := func() {
		t.Helper()
		if err := f.s.Close(); err != nil {
			t.Fatal(err)
		}
		var err error
		f.s, err = sqlite.Open(f.dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	reopen() // process loss before write-ahead call marker
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchNeverCalled)
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
		t.Fatal(err)
	}
	reopen() // process loss after marker, whether or not the native call ran
	after := assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchCallCommitted)
	if after.CallCommittedAt == nil || after.AdmissionGeneration != "g1" {
		t.Fatalf("call: %+v", after)
	}
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); !errors.Is(err, domain.ErrConversationSendUncertain) {
		t.Fatalf("replay: %v", err)
	}
	// A bound reference (including deferred request preparation) is not an ACK.
	if err := f.s.BindReservedOrdinarySend(t.Context(), a, "prepared-native", testNow); err != nil {
		t.Fatal(err)
	}
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchCallCommitted)
}

func TestDispatchBoundaryWrongOwnerAndReplacement(t *testing.T) {
	for _, field := range []string{"session", "branch", "conversation", "client", "turn", "generation"} {
		t.Run(field, func(t *testing.T) {
			f := newOrdinarySendFixture(t)
			a := f.append(t, "boundary-owner")
			wrong := a
			switch field {
			case "session":
				wrong.SessionID = "foreign"
			case "branch":
				wrong.BranchID = "foreign"
			case "conversation":
				wrong.ConversationID = "foreign"
			case "client":
				wrong.ClientMessageID = "foreign"
			case "turn":
				wrong.TurnID = "foreign"
			case "generation":
				wrong.ControllerGeneration = "foreign"
			}
			assertDispatchBoundary(t, f.s, wrong, domain.ConversationSendDispatchUnknown)
			if _, err := f.s.ReserveOrdinarySend(t.Context(), wrong, testNow); err == nil {
				t.Fatal("foreign grant")
			}
			assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchNeverCalled)
		})
	}
	f := newOrdinarySendFixture(t)
	a := f.append(t, "boundary-generation")
	if err := f.s.ClaimChatControllerGeneration(t.Context(), a.SessionID, "g2"); err != nil {
		t.Fatal(err)
	}
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchUnknown)
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err == nil {
		t.Fatal("stale grant")
	}
	a.ControllerGeneration = "g2"
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchUnknown)
	// Untouched queued work still dispatches in the replacement generation.
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
		t.Fatal(err)
	}
	b := assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchCallCommitted)
	if b.AdmissionGeneration != "g1" || b.ControllerGeneration != "g2" {
		t.Fatalf("generations: %+v", b)
	}
}

func TestDispatchBoundaryOldRowsStayUnknown(t *testing.T) {
	for _, state := range []string{"pending", "legacy", "reserved", "bound"} {
		t.Run(state, func(t *testing.T) {
			f := newOrdinarySendFixture(t)
			a := f.append(t, "boundary-old")
			if state == "reserved" || state == "bound" {
				if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
					t.Fatal(err)
				}
				if state == "bound" {
					if err := f.s.BindReservedOrdinarySend(t.Context(), a, "old-native", testNow); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := f.db.Exec(`DELETE FROM conversation_send_dispatch_boundaries WHERE turn_id = ?`, a.TurnID); err != nil {
				t.Fatal(err)
			}
			if state == "legacy" {
				if _, err := f.db.Exec(`UPDATE conversation_send_attempts SET state='legacy' WHERE turn_id=?`, a.TurnID); err != nil {
					t.Fatal(err)
				}
			}
			assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchUnknown)
			if state == "pending" {
				if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err != nil {
					t.Fatal(err)
				}
				b := assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchCallCommitted)
				if b.AdmittedAt != nil {
					t.Fatal("invented old admission history")
				}
			} else if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err == nil {
				t.Fatal("old row replayed")
			}
		})
	}
}

func TestDispatchBoundaryConcurrentStoresOneShot(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.append(t, "boundary-race")
	other, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	var grants atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, s := range []*sqlite.Store{f.s, other} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := s.ReserveOrdinarySend(t.Context(), a, testNow); err == nil {
				grants.Add(1)
				assertDispatchBoundary(t, other, a, domain.ConversationSendDispatchCallCommitted)
			} else if !errors.Is(err, domain.ErrConversationSendUncertain) {
				// SQLite may reject a competing read->write upgrade with BUSY.
				// That error grants no call; the committed winner is still unique.
				var coded interface{ Code() int }
				if !errors.As(err, &coded) || coded.Code()&0xff != 5 {
					t.Errorf("race: %v", err)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if grants.Load() != 1 {
		t.Fatalf("grants=%d", grants.Load())
	}
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchCallCommitted)
}

func TestDispatchBoundaryRollbackAndImmutableIdentity(t *testing.T) {
	for _, phase := range []string{"marker", "commit"} {
		t.Run(phase, func(t *testing.T) {
			f := newOrdinarySendFixture(t)
			a := f.append(t, "boundary-rollback")
			statement := `CREATE TRIGGER reject_boundary BEFORE UPDATE ON conversation_send_dispatch_boundaries
				BEGIN SELECT RAISE(ABORT, 'marker failure'); END`
			if phase == "commit" {
				statement = `CREATE TABLE boundary_parent(id INTEGER PRIMARY KEY);
				CREATE TABLE boundary_child(id INTEGER REFERENCES boundary_parent(id) DEFERRABLE INITIALLY DEFERRED);
				CREATE TRIGGER reject_boundary AFTER UPDATE ON conversation_send_dispatch_boundaries
				BEGIN INSERT INTO boundary_child VALUES(1); END`
			}
			if _, err := f.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			if message, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); err == nil || message.TurnID != "" {
				t.Fatalf("failed barrier granted: %+v %v", message, err)
			}
			assertOrdinarySendQueued(t, f, a, domain.ConversationSendPending)
			assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchNeverCalled)
		})
	}
	f := newOrdinarySendFixture(t)
	a := f.append(t, "boundary-immutable")
	if _, err := f.db.Exec(`UPDATE conversation_send_dispatch_boundaries SET client_message_id='foreign' WHERE turn_id=?`, a.TurnID); err == nil {
		t.Fatal("rewrote immutable admission")
	}
	// Mutable source rows cannot counterfeit an exact original admission.
	if _, err := f.db.Exec(`UPDATE conversation_messages SET client_message_id='substituted' WHERE turn_id=?`, a.TurnID); err != nil {
		t.Fatal(err)
	}
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchUnknown)
	a.ClientMessageID = "substituted"
	if _, err := f.s.ReserveOrdinarySend(t.Context(), a, testNow); !errors.Is(err, domain.ErrConversationSendConflict) {
		t.Fatalf("substituted grant: %v", err)
	}
}

func TestDispatchBoundaryAtomicReservedAdmission(t *testing.T) {
	f := newOrdinarySendFixture(t)
	a := f.owner
	a.TurnID, a.ClientMessageID = "boundary-atomic", "boundary-atomic-client"
	msg := domain.ConversationMessage{ID: "boundary-atomic-message", ClientMessageID: a.ClientMessageID, Text: "atomic", Origin: domain.MessageOriginAutomation}
	created, err := f.s.AppendReservedUserMessage(t.Context(), a, msg, testNow)
	if err != nil || !created {
		t.Fatalf("append reserved: %v %v", created, err)
	}
	assertDispatchBoundary(t, f.s, a, domain.ConversationSendDispatchCallCommitted)
	created, err = f.s.AppendReservedUserMessage(t.Context(), a, msg, testNow)
	if err != nil || created {
		t.Fatalf("duplicate grants: %v %v", created, err)
	}
	if _, err := f.db.Exec(`UPDATE conversation_send_dispatch_boundaries SET call_generation='g2' WHERE turn_id=?`, a.TurnID); err == nil {
		t.Fatal("rewrote one-shot call marker")
	}
}
