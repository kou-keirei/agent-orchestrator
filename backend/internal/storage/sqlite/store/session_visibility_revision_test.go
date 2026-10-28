package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func visibilityRevisionFixture(t *testing.T) (*sqlite.Store, *sqlite.Store, *sql.DB, domain.SessionID) {
	t.Helper()
	dir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dir)
	other, err := sqlite.OpenPreMigrated(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	u := url.URL{Path: filepath.Join(dir, "ao.db")}
	db, err := sql.Open("sqlite", "file:"+u.EscapedPath()+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	seedProject(t, s, "visibility")
	rec := sampleRecord("visibility")
	rec.Mode = domain.SessionModeChat
	rec.Metadata.RuntimeLaunchID = "runtime-launch"
	rec.Metadata.ProviderConversationID = "provider-thread"
	rec.Metadata.AgentSessionID = "native-session"
	session, err := s.CreateSession(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	return s, other, db, session.ID
}

func visibilitySession(t *testing.T, s *sqlite.Store, id domain.SessionID) domain.SessionRecord {
	t.Helper()
	rec, ok, err := s.GetSession(context.Background(), id)
	if err != nil || !ok {
		t.Fatalf("read session: ok=%v err=%v", ok, err)
	}
	return rec
}

func requireVisibilityStale(t *testing.T, err error, id domain.SessionID, revision int64) {
	t.Helper()
	var stale *store.SessionVisibilityStaleWriteError
	if !errors.As(err, &stale) || stale.SessionID != id || stale.ExpectedRevision != revision {
		t.Fatalf("error = %v, want typed stale write for %s revision %d", err, id, revision)
	}
}

// Snapshot every persisted table except CDC, omitting only the two facts a
// visibility CAS may change. This also checks raw session columns not exposed
// through SessionRecord, plus history, cleanup, workspaces and provider archive.
func visibilityUnchangedFacts(t *testing.T, db *sql.DB) map[string][][]any {
	t.Helper()
	names, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name <> 'change_log' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for names.Next() {
		var table string
		if err := names.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := names.Err(); err != nil {
		t.Fatal(err)
	}
	_ = names.Close()
	snapshot := make(map[string][][]any)
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		snapshot[table] = nil
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			for i, column := range columns {
				if table == "sessions" && (column == "hidden_at" || column == "revision") {
					values[i] = nil
				} else if bytes, ok := values[i].([]byte); ok {
					values[i] = string(bytes)
				}
			}
			snapshot[table] = append(snapshot[table], values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
	}
	return snapshot
}

func TestSessionHiddenRevisionPreservesHistoryAndLifecycle(t *testing.T) {
	for _, terminated := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminated=%v", terminated), func(t *testing.T) {
			s, _, db, id := visibilityRevisionFixture(t)
			ctx := context.Background()
			conversation, err := s.CreateConversation(ctx, "visibility-conversation", domain.ConversationScopeSession, "visibility", id, histClock)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.ClaimChatControllerGeneration(ctx, id, "generation"); err != nil {
				t.Fatal(err)
			}
			if created, err := s.AppendUserMessage(ctx, conversation.ID, id, "generation", domain.ConversationMessage{
				ID: "history-message", Origin: domain.MessageOriginHuman, Text: "preserve history",
			}, "history-turn", histClock); err != nil || !created {
				t.Fatalf("seed history: created=%v err=%v", created, err)
			}
			if applied, err := s.ProjectProviderEvent(ctx, conversation.ID, id, "generation", "provider-event", "message.delta", `{}`, histClock, func(context.Context) error { return nil }); err != nil || !applied {
				t.Fatalf("seed archive: applied=%v err=%v", applied, err)
			}
			rec := visibilitySession(t, s, id)
			rec.IsTerminated = terminated
			if err := s.UpdateSession(ctx, rec); err != nil {
				t.Fatal(err)
			}
			facts := visibilityUnchangedFacts(t, db)
			for i, hidden := range []bool{true, false, true, false} {
				before := visibilitySession(t, s, id)
				seq, err := s.LatestSeq(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var hiddenAt *time.Time
				if hidden {
					at := histClock.Add(time.Duration(i+1) * time.Minute)
					hiddenAt = &at
				}
				if err := s.SetSessionHiddenIfRevision(ctx, id, before.Revision, hiddenAt); err != nil {
					t.Fatal(err)
				}
				after := visibilitySession(t, s, id)
				if after.Revision != before.Revision+1 || !reflect.DeepEqual(after.HiddenAt, hiddenAt) {
					t.Fatalf("visibility/revision = %v/%d, want %v/%d", after.HiddenAt, after.Revision, hiddenAt, before.Revision+1)
				}
				if got := visibilityUnchangedFacts(t, db); !reflect.DeepEqual(got, facts) {
					t.Fatal("visibility CAS changed unrelated persisted facts")
				}
				events, err := s.EventsAfter(ctx, seq, 100)
				if err != nil || len(events) != 1 || string(events[0].Type) != "session_updated" {
					t.Fatalf("visibility CDC = %+v, err=%v; want one session_updated", events, err)
				}
			}
		})
	}
}

func TestSessionHiddenRevisionDuplicateDoesNotWrite(t *testing.T) {
	s, _, _, id := visibilityRevisionFixture(t)
	ctx := context.Background()
	for _, hiddenAt := range []*time.Time{&histClock, nil} {
		before := visibilitySession(t, s, id)
		if err := s.SetSessionHiddenIfRevision(ctx, id, before.Revision, hiddenAt); err != nil {
			t.Fatal(err)
		}
		after := visibilitySession(t, s, id)
		seq, err := s.LatestSeq(ctx)
		if err != nil {
			t.Fatal(err)
		}
		requireVisibilityStale(t, s.SetSessionHiddenIfRevision(ctx, id, before.Revision, hiddenAt), id, before.Revision)
		if got := visibilitySession(t, s, id); !reflect.DeepEqual(got, after) {
			t.Fatal("duplicate request changed session")
		}
		if got, err := s.LatestSeq(ctx); err != nil || got != seq {
			t.Fatalf("duplicate emitted CDC: seq=%d want=%d err=%v", got, seq, err)
		}
	}
}

func TestSessionHiddenRevisionRejectsIndependentWriter(t *testing.T) {
	for _, writer := range []string{"raw SQL same timestamp", "legacy hide", "activation clear"} {
		t.Run(writer, func(t *testing.T) {
			s, other, db, id := visibilityRevisionFixture(t)
			ctx := context.Background()
			if ok, err := s.SetSessionHidden(ctx, id, &histClock); err != nil || !ok {
				t.Fatalf("seed hide: ok=%v err=%v", ok, err)
			}
			before := visibilitySession(t, s, id)
			switch writer {
			case "raw SQL same timestamp":
				if _, err := db.Exec(`UPDATE sessions SET latest_user_prompt = 'other writer' WHERE id = ?`, id); err != nil {
					t.Fatal(err)
				}
			case "legacy hide":
				at := histClock.Add(time.Hour)
				if ok, err := other.SetSessionHidden(ctx, id, &at); err != nil || !ok {
					t.Fatalf("legacy hide: ok=%v err=%v", ok, err)
				}
			case "activation clear":
				if ok, err := other.ClearSessionHiddenIfUnchanged(ctx, id, &histClock); err != nil || !ok {
					t.Fatalf("activation clear: ok=%v err=%v", ok, err)
				}
			}
			after := visibilitySession(t, s, id)
			seq, err := s.LatestSeq(ctx)
			if err != nil || after.Revision != before.Revision+1 || !after.UpdatedAt.Equal(before.UpdatedAt) {
				t.Fatalf("independent writer revision/recency: before=%+v after=%+v err=%v", before, after, err)
			}
			requireVisibilityStale(t, s.SetSessionHiddenIfRevision(ctx, id, before.Revision, nil), id, before.Revision)
			if got := visibilitySession(t, s, id); !reflect.DeepEqual(got, after) {
				t.Fatal("stale CAS overwrote independent writer")
			}
			if got, err := s.LatestSeq(ctx); err != nil || got != seq {
				t.Fatalf("stale CAS emitted CDC: seq=%d want=%d err=%v", got, seq, err)
			}
		})
	}
}

func TestSessionHiddenRevisionConcurrentWritersHaveOneWinner(t *testing.T) {
	s, other, _, id := visibilityRevisionFixture(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		before := visibilitySession(t, s, id)
		start := make(chan struct{})
		results := make(chan error, 2)
		first := histClock.Add(time.Duration(2*i) * time.Minute)
		second := first.Add(time.Minute)
		for j, writer := range []*sqlite.Store{s, other} {
			at := []time.Time{first, second}[j]
			go func() {
				<-start
				results <- writer.SetSessionHiddenIfRevision(ctx, id, before.Revision, &at)
			}()
		}
		close(start)
		wins := 0
		for j := 0; j < 2; j++ {
			if err := <-results; err == nil {
				wins++
			} else {
				requireVisibilityStale(t, err, id, before.Revision)
			}
		}
		after := visibilitySession(t, s, id)
		if wins != 1 || after.Revision != before.Revision+1 || after.HiddenAt == nil || (!after.HiddenAt.Equal(first) && !after.HiddenAt.Equal(second)) {
			t.Fatalf("race: wins=%d revision=%d hidden=%v", wins, after.Revision, after.HiddenAt)
		}
	}
}

func TestSessionHiddenRevisionStatementRowCounts(t *testing.T) {
	s, _, db, id := visibilityRevisionFixture(t)
	ctx := context.Background()
	before := visibilitySession(t, s, id)
	q := gen.New(db)
	params := gen.SetSessionHiddenIfRevisionParams{ID: id, ExpectedRevision: before.Revision}
	// Matching unchanged NULL still consumes one revision, as legacy writes do.
	if rows, err := q.SetSessionHiddenIfRevision(ctx, params); err != nil || rows != 1 {
		t.Fatalf("matched statement rows=%d err=%v", rows, err)
	}
	if got := visibilitySession(t, s, id); got.Revision != before.Revision+1 || got.HiddenAt != nil {
		t.Fatalf("unchanged write revision=%d hidden=%v", got.Revision, got.HiddenAt)
	}
	if rows, err := q.SetSessionHiddenIfRevision(ctx, params); err != nil || rows != 0 {
		t.Fatalf("stale statement rows=%d err=%v", rows, err)
	}
	params.ID = "missing"
	if rows, err := q.SetSessionHiddenIfRevision(ctx, params); err != nil || rows != 0 {
		t.Fatalf("missing statement rows=%d err=%v", rows, err)
	}
}

func TestSessionHiddenRevisionMissingAndDatabaseErrors(t *testing.T) {
	s, _, _, id := visibilityRevisionFixture(t)
	ctx := context.Background()
	requireVisibilityStale(t, s.SetSessionHiddenIfRevision(ctx, "missing", 0, nil), "missing", 0)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err := s.SetSessionHiddenIfRevision(canceled, id, 0, &histClock)
	var stale *store.SessionVisibilityStaleWriteError
	if !errors.Is(err, context.Canceled) || errors.As(err, &stale) {
		t.Fatalf("canceled statement error = %v, want cancellation, not stale write", err)
	}
	if got := visibilitySession(t, s, id); got.Revision != 0 || got.HiddenAt != nil {
		t.Fatalf("failed statement mutated session: %+v", got)
	}
}
