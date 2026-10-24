package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	"github.com/pressly/goose/v3"
)

// Frozen pre-port statement: also exercises migration 0087's legacy-writer fallback.
const legacyProviderEventInsert = `INSERT OR IGNORE INTO conversation_provider_events
(conversation_id, session_id, provider_event_id, method, payload_json, received_at)
VALUES (?, ?, ?, ?, ?, ?)`

type providerBranchFixture struct {
	s            *sqlite.Store
	db           *sql.DB
	path         string
	session      domain.SessionID
	conversation string
}

func newProviderBranchFixture(t *testing.T) providerBranchFixture {
	t.Helper()
	dir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dir)
	seedProject(t, s, "archive")
	rec := sampleRecord("archive")
	rec.Mode = domain.SessionModeChat
	rec.Metadata.ControllerGeneration = "gen-archive"
	session, err := s.CreateSession(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := s.CreateConversation(context.Background(), "archive-conversation", domain.ConversationScopeSession, "archive", session.ID, testNow)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ao.db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return providerBranchFixture{s, db, path, session.ID, conversation.ID}
}

func archiveParams(f providerBranchFixture, id string) gen.InsertConversationProviderEventParams {
	return gen.InsertConversationProviderEventParams{ConversationID: f.conversation, SessionID: f.session, ProviderEventID: id, Method: "message.delta", PayloadJson: `{"delta":"x"}`, ReceivedAt: testNow}
}

func archiveExec(t *testing.T, db gen.DBTX, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func archiveInt(t *testing.T, db gen.DBTX, query string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestProviderEventBranchInsertAndFallback(t *testing.T) {
	f := newProviderBranchFixture(t)
	ctx := context.Background()
	// Observe actual branch UPDATEs, including no-op rewrites on an empty head.
	archiveExec(t, f.db, `CREATE TABLE branch_rewrites (id INTEGER);
CREATE TRIGGER observe_branch_rewrite AFTER UPDATE OF branch_id ON conversation_provider_events
BEGIN INSERT INTO branch_rewrites VALUES (NEW.id); END;`)
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	q := gen.New(tx)
	for i, branch := range []string{f.conversation + ":root", "switched", "nested", ""} {
		// The head changes on this same transaction/connection before the INSERT.
		archiveExec(t, tx, `UPDATE conversations SET active_branch_id = ? WHERE id = ?`, branch, f.conversation)
		p := archiveParams(f, fmt.Sprintf("event-%d", i))
		if n, err := q.InsertConversationProviderEvent(ctx, p); err != nil || n != 1 {
			t.Fatalf("insert %q: %d, %v", branch, n, err)
		}
		var got string
		if err := tx.QueryRowContext(ctx, `SELECT branch_id FROM conversation_provider_events WHERE provider_event_id = ?`, p.ProviderEventID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != branch {
			t.Fatalf("branch = %q, want %q", got, branch)
		}
		want := int64(0)
		if branch == "" {
			want = 1
		}
		if n := archiveInt(t, tx, `SELECT count(*) FROM branch_rewrites`); n != want {
			t.Fatalf("branch %q: rewrites = %d, want %d", branch, n, want)
		}
	}
	archiveExec(t, tx, `UPDATE conversations SET active_branch_id = 'legacy-head' WHERE id = ?`, f.conversation)
	p := archiveParams(f, "legacy")
	archiveExec(t, tx, legacyProviderEventInsert, p.ConversationID, p.SessionID, p.ProviderEventID, p.Method, p.PayloadJson, p.ReceivedAt)
	var got string
	if err := tx.QueryRowContext(ctx, `SELECT branch_id FROM conversation_provider_events WHERE provider_event_id = 'legacy'`).Scan(&got); err != nil || got != "legacy-head" {
		t.Fatalf("fallback branch = %q, %v", got, err)
	}
	if n := archiveInt(t, tx, `SELECT count(*) FROM branch_rewrites`); n != 2 {
		t.Fatalf("fallback rewrites = %d", n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestProviderEventBranchProjectionBoundaries(t *testing.T) {
	f := newProviderBranchFixture(t)
	ctx := context.Background()
	projected := 0
	project := func(txCtx context.Context) error {
		projected++
		return f.s.AppendAssistantDelta(txCtx, f.conversation, "item", "", "x", "message", testNow)
	}
	apply := func(conversation, generation, id string, fn func(context.Context) error) (bool, error) {
		return f.s.ProjectProviderEvent(ctx, conversation, f.session, generation, id, "message.delta", `{"delta":"x"}`, testNow, fn)
	}
	if ok, err := apply("missing", "gen-archive", "missing", project); err == nil || ok || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("missing conversation: %v, %v", ok, err)
	}
	if ok, err := apply(f.conversation, "stale", "stale", project); err != nil || ok {
		t.Fatalf("stale generation: %v, %v", ok, err)
	}
	if projected != 0 {
		t.Fatal("rejected event ran projection")
	}
	for i := 0; i < 2; i++ {
		ok, err := apply(f.conversation, "gen-archive", "identified", project)
		if err != nil || ok != (i == 0) {
			t.Fatalf("identified replay %d: %v, %v", i, ok, err)
		}
	}
	for range 2 {
		if ok, err := apply(f.conversation, "gen-archive", "", project); err != nil || !ok {
			t.Fatalf("anonymous event: %v, %v", ok, err)
		}
	}
	injected := errors.New("rollback projection")
	if ok, err := apply(f.conversation, "gen-archive", "rolled-back", func(txCtx context.Context) error {
		if err := project(txCtx); err != nil {
			return err
		}
		// Nested archive must share the outer rollback as well.
		_, err := f.s.ProjectProviderEvent(txCtx, f.conversation, f.session, "gen-archive", "nested", "message.delta", `{}`, testNow, func(context.Context) error { return nil })
		if err != nil {
			return err
		}
		return injected
	}); ok || !errors.Is(err, injected) {
		t.Fatalf("rollback: %v, %v", ok, err)
	}
	if n := archiveInt(t, f.db, `SELECT count(*) FROM conversation_provider_events`); n != 3 {
		t.Fatalf("archive rows = %d, want 3", n)
	}
	var text string
	if err := f.db.QueryRowContext(ctx, `SELECT text FROM conversation_messages WHERE id = 'message'`).Scan(&text); err != nil || text != "xxx" {
		t.Fatalf("projection = %q, %v", text, err)
	}
	if projected != 4 {
		t.Fatalf("projection calls = %d, want 4 including rolled-back callback", projected)
	}
}

func TestProviderEventBranchNestedArchiveSeesTransactionHead(t *testing.T) {
	f := newProviderBranchFixture(t)
	ctx := context.Background()
	// Use the real lineage and activation APIs before entering projection.
	for _, branch := range []domain.ConversationBranch{
		{ID: "child", ConversationID: f.conversation, ParentBranchID: f.conversation + ":root", ProviderConversationID: "thread-child"},
		{ID: "grandchild", ConversationID: f.conversation, ParentBranchID: "child", ProviderConversationID: "thread-grandchild"},
	} {
		if err := f.s.CreateConversationBranch(ctx, branch, testNow); err != nil {
			t.Fatal(err)
		}
		if err := f.s.ActivateConversationBranch(ctx, f.session, f.conversation, branch.ID, branch.ProviderConversationID, "gen-archive", testNow); err != nil {
			t.Fatal(err)
		}
		ok, err := f.s.ProjectProviderEvent(ctx, f.conversation, f.session, "gen-archive", branch.ID, "message.delta", `{}`, testNow, func(txCtx context.Context) error {
			_, err := f.s.ProjectProviderEvent(txCtx, f.conversation, f.session, "gen-archive", branch.ID+"-nested", "message.delta", `{}`, testNow, func(context.Context) error { return nil })
			return err
		})
		if err != nil || !ok {
			t.Fatalf("nested archive: %v, %v", ok, err)
		}
	}
	rows, err := f.db.QueryContext(ctx, `SELECT branch_id FROM conversation_provider_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var branch string
		if err := rows.Scan(&branch); err != nil {
			t.Fatal(err)
		}
		got = append(got, branch)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"child", "child", "grandchild", "grandchild"}) {
		t.Fatalf("branches = %v", got)
	}
}

func TestProviderEventBranchMigrationCompatibility(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "upgrade.db"))+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, os.DirFS("../migrations"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 86); err != nil {
		t.Fatal(err)
	}
	// Seed using the old schema, without calling current generated session SQL.
	archiveExec(t, db, `INSERT INTO projects (id, path, registered_at) VALUES ('upgrade', '/fixture', ?)`, testNow)
	archiveExec(t, db, `INSERT INTO sessions (id, project_id, num, activity_last_at, created_at, updated_at) VALUES ('upgrade-1', 'upgrade', 1, ?, ?, ?)`, testNow, testNow, testNow)
	archiveExec(t, db, `INSERT INTO conversations (id, scope, project_id, created_at, updated_at) VALUES ('upgrade', 'project', 'upgrade', ?, ?)`, testNow, testNow)
	archiveExec(t, db, legacyProviderEventInsert, "upgrade", "upgrade-1", "old", "message.delta", `{"delta":"old"}`, testNow)
	if _, err := provider.Up(ctx); err != nil {
		t.Fatal(err)
	}
	p := gen.InsertConversationProviderEventParams{ConversationID: "upgrade", SessionID: "upgrade-1", ProviderEventID: "new", Method: "message.delta", PayloadJson: `{"delta":"new"}`, ReceivedAt: testNow}
	if n, err := gen.New(db).InsertConversationProviderEvent(ctx, p); err != nil || n != 1 {
		t.Fatalf("post-upgrade insert: %d, %v", n, err)
	}
	if n := archiveInt(t, db, `SELECT count(*) FROM conversation_provider_events WHERE branch_id = 'upgrade:root'`); n != 2 {
		t.Fatalf("old/new root archive rows = %d, want 2", n)
	}
}

// SQL-boundary stream comparison. Both variants use the same migrated schema,
// singleton transactions, payloads, projection UPDATE, branch changes and pragmas.
// No observer trigger is installed: total_changes measures the fallback UPDATE
// without adding writes that would perturb WAL measurements.
func TestProviderEventBranchPairedStream(t *testing.T) {
	type result struct {
		rows                    []string
		text                    string
		rewrites, frames, bytes int64
		elapsed                 time.Duration
	}
	run := func(t *testing.T, legacy bool) result {
		f := newProviderBranchFixture(t)
		ctx := context.Background()
		if err := f.s.AppendAssistantDelta(ctx, f.conversation, "stream-item", "", "", "stream-message", testNow); err != nil {
			t.Fatal(err)
		}
		archiveExec(t, f.db, `PRAGMA wal_autocheckpoint=0`)
		archiveExec(t, f.db, `PRAGMA wal_checkpoint(TRUNCATE)`)
		var rewrites int64
		start := time.Now()
		const events = 256
		for i := range events {
			tx, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			branch := fmt.Sprintf("stream-branch-%d", i/64)
			if i%64 == 0 {
				archiveExec(t, tx, `UPDATE conversations SET active_branch_id = ? WHERE id = ?`, branch, f.conversation)
			}
			p := archiveParams(f, "")
			p.PayloadJson = fmt.Sprintf(`{"delta":"%04d"}`, i)
			before := archiveInt(t, tx, `SELECT total_changes()`)
			if legacy {
				archiveExec(t, tx, legacyProviderEventInsert, p.ConversationID, p.SessionID, p.ProviderEventID, p.Method, p.PayloadJson, p.ReceivedAt)
			} else {
				if n, err := gen.New(tx).InsertConversationProviderEvent(ctx, p); err != nil || n != 1 {
					t.Fatalf("insert: %d, %v", n, err)
				}
			}
			rewrites += archiveInt(t, tx, `SELECT total_changes()`) - before - 1
			if err := gen.New(tx).AppendConversationMessageDelta(ctx, gen.AppendConversationMessageDeltaParams{Text: fmt.Sprintf("%04d", i), UpdatedAt: testNow, ConversationID: f.conversation, ProviderItemID: "stream-item"}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		r := result{elapsed: time.Since(start), rewrites: rewrites}
		info, err := os.Stat(f.path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		r.bytes = info.Size()
		pageSize := archiveInt(t, f.db, `PRAGMA page_size`)
		r.frames = (r.bytes - 32) / (pageSize + 24)
		rows, err := f.db.QueryContext(ctx, `SELECT id, branch_id, provider_event_id, method, payload_json, received_at FROM conversation_provider_events ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id int64
			var branch, event, method, payload, received string
			if err := rows.Scan(&id, &branch, &event, &method, &payload, &received); err != nil {
				t.Fatal(err)
			}
			r.rows = append(r.rows, fmt.Sprintf("%d|%s|%s|%s|%s|%s", id, branch, event, method, payload, received))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if err := f.db.QueryRowContext(ctx, `SELECT text FROM conversation_messages WHERE id = 'stream-message'`).Scan(&r.text); err != nil {
			t.Fatal(err)
		}
		t.Logf("legacy=%v events=%d branch_updates=%d wal_frames=%d wal_bytes=%d latency=%s", legacy, events, r.rewrites, r.frames, r.bytes, r.elapsed)
		return r
	}
	var before, after result
	t.Run("before", func(t *testing.T) { before = run(t, true) })
	t.Run("after", func(t *testing.T) { after = run(t, false) })
	if t.Failed() {
		return
	}
	if before.rewrites != 256 || after.rewrites != 0 {
		t.Fatalf("branch updates before/after = %d/%d", before.rewrites, after.rewrites)
	}
	if len(before.rows) != 256 || !reflect.DeepEqual(before.rows, after.rows) || before.text != after.text || len(after.text) != 1024 {
		t.Fatal("archive order/identity/payload or streamed projection differs")
	}
}
