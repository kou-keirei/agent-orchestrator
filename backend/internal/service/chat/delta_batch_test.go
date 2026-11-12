package chat

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type batchTestStore struct {
	*sqlite.Store
	commits int
	fail    bool
}

func (s *batchTestStore) ProjectProviderEventBatch(ctx context.Context, project func(context.Context) error) error {
	err := s.Store.ProjectProviderEventBatch(ctx, func(txCtx context.Context) error {
		if err := project(txCtx); err != nil {
			return err
		}
		if s.fail {
			return errors.New("injected batch failure")
		}
		return nil
	})
	if err == nil {
		s.commits++
	}
	return err
}

type batchFixture struct {
	c    *Controller
	s    *batchTestStore
	db   *sql.DB
	path string
}

func newBatchFixture(t *testing.T) batchFixture {
	t.Helper()
	dir := t.TempDir()
	st := sqlitetest.MustOpenAt(t, dir)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(st.UpsertProject(ctx, domain.ProjectRecord{ID: "batch", Path: dir, RegisteredAt: now}))
	_, err := st.CreateSession(ctx, domain.SessionRecord{ID: "batch-1", ProjectID: "batch", Kind: domain.KindWorker, Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, CreatedAt: now, UpdatedAt: now, Metadata: domain.SessionMetadata{ControllerGeneration: "gen"}})
	must(err)
	conv, err := st.CreateConversation(ctx, "batch", domain.ConversationScopeSession, "batch", "batch-1", now)
	must(err)
	path := filepath.Join(dir, "ao.db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	must(err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	s := &batchTestStore{Store: st}
	id := 0
	c := &Controller{store: s, conversation: conv, sessionID: "batch-1", generation: "gen", now: func() time.Time { return now }, newID: func() string { id++; return fmt.Sprintf("id-%d", id) }, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return batchFixture{c, s, db, path}
}
func (f batchFixture) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.db.QueryRow(`SELECT count(*) FROM conversation_provider_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func (f batchFixture) fingerprint(t *testing.T) []string {
	t.Helper()
	var result []string
	for _, query := range []string{
		`SELECT json_array(id,branch_id,provider_event_id,method,payload_json) FROM conversation_provider_events ORDER BY id`,
		`SELECT json_array(branch_id,provider_item_id,text,sequence) FROM conversation_messages ORDER BY sequence`,
	} {
		rows, err := f.db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				t.Fatal(err)
			}
			result = append(result, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
	}
	return result
}
func deltaEvents(n int) []ports.ChatEvent {
	events := make([]ports.ChatEvent, n)
	for i := range events {
		events[i] = ports.ChatEvent{Kind: ports.ChatEventMessageDelta, ProviderEventID: fmt.Sprintf("event-%d", i), ProviderTurnID: "turn", ProviderItemID: fmt.Sprintf("item-%d", i%2), Delta: fmt.Sprintf("%d\x00z", i)}
	}
	return events
}
func TestDeltaBatchReaderBoundsAndOrder(t *testing.T) {
	for _, size := range []int{0, 1, deltaBatchBytes / 2, deltaBatchBytes, deltaBatchBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			events := deltaEvents(70)
			for i := range events {
				events[i].Delta = strings.Repeat("x", size)
			}
			events = append(events[:35], append([]ports.ChatEvent{{Kind: ports.ChatEventTurnCompleted}}, events[35:]...)...)
			ch := make(chan ports.ChatEvent, len(events))
			for _, e := range events {
				ch <- e
			}
			close(ch)
			reader := deltaBatchReader{events: ch, enabled: true}
			var got []ports.ChatEvent
			for batch := reader.next(); len(batch) > 0; batch = reader.next() {
				bytes := 0
				for _, e := range batch {
					bytes += len(e.Delta)
				}
				if len(batch) > deltaBatchEvents || (len(batch) > 1 && bytes > deltaBatchBytes) {
					t.Fatalf("unbounded batch: %d/%d", len(batch), bytes)
				}
				if batch[0].Kind != ports.ChatEventMessageDelta && len(batch) != 1 {
					t.Fatal("boundary joined")
				}
				got = append(got, batch...)
			}
			if !reflect.DeepEqual(got, events) {
				t.Fatal("event order/content differs")
			}
		})
	}
	// An open channel with one queued delta must return immediately, without a timer.
	ch := make(chan ports.ChatEvent, 1)
	ch <- deltaEvents(1)[0]
	r := deltaBatchReader{events: ch, enabled: true}
	if len(r.next()) != 1 {
		t.Fatal("idle writer did not flush")
	}
	close(ch)
}
func TestDeltaBatchSQLiteEquivalence(t *testing.T) {
	run := func(batched bool) ([]string, int, int64) {
		f := newBatchFixture(t)
		ctx := context.Background()
		for _, branch := range []string{"child", "sibling"} {
			if err := f.s.CreateConversationBranch(ctx, domain.ConversationBranch{ID: branch, ConversationID: "batch", ParentBranchID: "batch:root", ProviderConversationID: branch}, f.c.now()); err != nil {
				t.Fatal(err)
			}
		}
		for _, q := range []string{`PRAGMA wal_autocheckpoint=0`, `PRAGMA wal_checkpoint(TRUNCATE)`} {
			if _, err := f.db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
		commits := 0
		start := time.Now()
		for _, branch := range []string{"batch:root", "child", "sibling", "child"} {
			providerID := branch
			if branch == "batch:root" {
				providerID = ""
			}
			if err := f.s.ActivateConversationBranch(ctx, "batch-1", "batch", branch, providerID, "gen", f.c.now()); err != nil {
				t.Fatal(err)
			}
			events := deltaEvents(65)
			for i := range events {
				events[i].ProviderEventID = branch + ":" + events[i].ProviderEventID
			}
			events[0].Detail = json.RawMessage(`{"false":false,"zero":0}`)
			events[0].Input = &ports.ChatInputRequest{}
			events[0].Err = errors.New("")
			events = append(events, events[0]) // native replay must not append text twice
			events = append(events, ports.ChatEvent{Kind: ports.ChatEventMessageDelta, ProviderItemID: "anonymous", Delta: "same"}, ports.ChatEvent{Kind: ports.ChatEventMessageDelta, ProviderItemID: "anonymous", Delta: "same"})
			ch := make(chan ports.ChatEvent, len(events))
			for _, e := range events {
				ch <- e
			}
			close(ch)
			reader := deltaBatchReader{events: ch, enabled: batched}
			for batch := reader.next(); len(batch) > 0; batch = reader.next() {
				if _, _, err := f.c.projectEvents(ctx, batch, true); err != nil {
					t.Fatal(err)
				}
				commits++
			}
		}
		var payload string
		if err := f.db.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE provider_event_id='batch:root:event-0'`).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(payload), &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["detail"]) != `{"false":false,"zero":0}` || fields["input"] == nil || string(fields["error"]) != `""` {
			t.Fatalf("meaningful zero values lost: %s", payload)
		}
		info, err := os.Stat(f.path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("batched=%v transactions=%d archive_rows=%d WAL_bytes=%d elapsed=%s", batched, commits, f.count(t), info.Size(), time.Since(start))
		return f.fingerprint(t), commits, info.Size()
	}
	before, bc, bw := run(false)
	after, ac, aw := run(true)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("archive, branch replay, identity, payload or projection differs")
	}
	if ac >= bc || aw >= bw {
		t.Fatalf("no write improvement: commits %d/%d WAL %d/%d", bc, ac, bw, aw)
	}
}
func TestDeltaBatchRollbackCancellationAndResume(t *testing.T) {
	f := newBatchFixture(t)
	events := deltaEvents(12)
	ctx := context.Background()
	f.s.fail = true
	if _, _, err := f.c.projectEvents(ctx, events, true); err == nil {
		t.Fatal("missing injected failure")
	}
	if f.count(t) != 0 || len(f.fingerprint(t)) != 0 {
		t.Fatal("partial archive/projection escaped rollback")
	}
	f.s.fail = false
	canceled, cancel := context.WithCancel(ctx)
	err := f.s.ProjectProviderEventBatch(canceled, func(txCtx context.Context) error {
		if _, _, err := f.c.projectEvent(txCtx, events[0]); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if err == nil || f.count(t) != 0 {
		t.Fatalf("cancellation committed: %v", err)
	}
	for range 2 {
		if _, _, err := f.c.projectEvents(ctx, events, true); err != nil {
			t.Fatal(err)
		}
	}
	if f.count(t) != len(events) {
		t.Fatal("resume duplicated/lost native IDs")
	}
	before := f.fingerprint(t)
	f.c.generation = "stale"
	more := deltaEvents(1)
	more[0].ProviderEventID = "stale"
	if _, _, err := f.c.projectEvents(ctx, append(more, more...), true); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, f.fingerprint(t)) {
		t.Fatal("stale generation mutated history")
	}
}

type batchConversation struct {
	ports.ChatConversation
	events chan ports.ChatEvent
	ack    func(string) error
}

func (c *batchConversation) Events() <-chan ports.ChatEvent { return c.events }
func (c *batchConversation) AcknowledgeProviderEvent(_ context.Context, id string) error {
	return c.ack(id)
}
func (c *batchConversation) Close() error { return nil }
func TestDeltaBatchLiveAcknowledgesAfterCommit(t *testing.T) {
	f := newBatchFixture(t)
	events := deltaEvents(65)
	ch := make(chan ports.ChatEvent, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	var acks []string
	f.c.conv = &batchConversation{events: ch, ack: func(id string) error {
		// Every member of the current batch must already be durable at first ack.
		want := 32
		if len(acks) >= 32 {
			want = 64
		}
		if len(acks) >= 64 {
			want = 65
		}
		if f.count(t) != want {
			t.Fatalf("ack before full commit: %s rows=%d want=%d", id, f.count(t), want)
		}
		acks = append(acks, id)
		return nil
	}}
	f.c.preserveProviderOnStop = true
	f.c.stopped = make(chan struct{})
	f.c.project()
	if len(acks) != 65 || f.s.commits != 2 {
		t.Fatalf("acks=%d batch commits=%d", len(acks), f.s.commits)
	}
	for i, id := range acks {
		if id != events[i].ProviderEventID {
			t.Fatal("ack order differs")
		}
	}
}

func TestDeltaBatchMalformedEventFallback(t *testing.T) {
	f := newBatchFixture(t)
	events := deltaEvents(4)
	events[1].Detail = json.RawMessage("{")
	if _, _, err := f.c.projectEvents(context.Background(), events, false); err != nil {
		t.Fatal(err)
	}
	if f.count(t) != 3 {
		t.Fatal("malformed event discarded adjacent output")
	}
	var count int
	if err := f.db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE provider_event_id='event-1'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("malformed event archived: %d %v", count, err)
	}
}
func TestDeltaBatchLiveFailureAndReplay(t *testing.T) {
	for _, ackFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(ackFailure), func(t *testing.T) {
			f := newBatchFixture(t)
			events := deltaEvents(12)
			run := func(fail bool) []string {
				ch := make(chan ports.ChatEvent, len(events))
				for _, e := range events {
					ch <- e
				}
				close(ch)
				var acks []string
				f.s.fail = fail && !ackFailure
				f.c.conv = &batchConversation{events: ch, ack: func(id string) error {
					if fail && ackFailure && id == events[3].ProviderEventID {
						return errors.New("ack failed")
					}
					acks = append(acks, id)
					return nil
				}}
				f.c.stopped = make(chan struct{})
				f.c.preserveProviderOnStop = true
				f.c.project()
				return acks
			}
			acks := run(true)
			if ackFailure {
				if len(acks) != 3 || f.count(t) != 12 {
					t.Fatal("ack failure lost committed rows or advanced cursor")
				}
			} else if len(acks) != 0 || f.count(t) != 0 {
				t.Fatal("failed transaction acknowledged or partly committed")
			}
			if len(run(false)) != 12 || f.count(t) != 12 {
				t.Fatal("replay did not recover exactly once")
			}
			// Native history uses its unchanged singleton importer and must dedupe too.
			before := f.fingerprint(t)
			if err := f.c.projectNativeHistory(context.Background(), events); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, f.fingerprint(t)) {
				t.Fatal("native history replay changed output")
			}
		})
	}
}
