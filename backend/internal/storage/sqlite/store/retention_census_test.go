package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func censusOptions() store.RetentionCensusOptions {
	return store.RetentionCensusOptions{CallerSnapshotID: "fixture-window", PageRows: 3,
		MaxPages: 10, MaxReportBytes: 8192, MaxDuration: 10 * time.Second}
}

func beginCensus(t *testing.T, s *sqlite.Store, options store.RetentionCensusOptions) *store.RetentionCensus {
	t.Helper()
	c, err := s.BeginRetentionCensus(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func censusPage(t *testing.T, c *store.RetentionCensus, cursor *store.RetentionCensusCursor) store.RetentionCensusPage {
	t.Helper()
	p, err := c.Page(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if p.Version != "ao16.retention-census.v1" || p.UnknownRows != p.ScannedRows || p.CoverageComplete ||
		p.DeletionAuthority != "NONE" || p.FilesystemReclaimEstimate != "UNKNOWN" ||
		p.SnapshotBinding != "SQLITE_READ_TRANSACTION" || p.SnapshotID == "" || p.Revision != p.SnapshotID {
		t.Fatalf("unsafe census contract: %+v", p)
	}
	for _, gap := range []store.RetentionSourceGap{store.RetentionGapOwnership, store.RetentionGapProjection,
		store.RetentionGapCheckpoint, store.RetentionGapReplay, store.RetentionGapNativeACK,
		store.RetentionGapHolds, store.RetentionGapExternalSnapshot, store.RetentionGapSettlement} {
		if !slices.Contains(p.SourceGaps, gap) {
			t.Fatalf("missing source gap %s: %+v", gap, p)
		}
	}
	var rows int64
	for _, count := range p.Counts {
		if count.Class != "UNKNOWN_REVIEW_REQUIRED" || count.Rows != count.PayloadBytesKnownRows+count.PayloadBytesUnknownRows {
			t.Fatalf("unsafe class/byte accounting: %+v", count)
		}
		rows += count.Rows
	}
	if rows != p.ScannedRows {
		t.Fatalf("count sum = %d, scanned = %d", rows, p.ScannedRows)
	}
	return p
}

func insertCensusEvent(t *testing.T, f providerBranchFixture, branch, providerID, method string, payload any) {
	t.Helper()
	archiveExec(t, f.db, `INSERT OR IGNORE INTO conversation_provider_events
 (conversation_id, session_id, branch_id, provider_event_id, method, payload_json, received_at)
 VALUES (?, ?, ?, ?, ?, ?, ?)`, f.conversation, f.session, branch, providerID, method, payload, testNow)
}

func TestRetentionCensusMixedStreamsAndDeterministicPages(t *testing.T) {
	f := newProviderBranchFixture(t)
	// Repeated ownership of a native provider handle does not prove an event's
	// epoch, nor do terminal/interrupted turns establish checkpoint coverage.
	for i, state := range []string{"completed", "interrupted", "cancelled"} {
		branch := fmt.Sprintf("epoch-%d", i)
		archiveExec(t, f.db, `INSERT INTO conversation_branches
 (id, conversation_id, session_id, provider_conversation_id, created_at) VALUES (?, ?, ?, ?, ?)`,
			branch, f.conversation, f.session, "same-native-provider", testNow)
		turnState := state
		if turnState == "cancelled" {
			turnState = "interrupted" // AO's typed representation of provider cancel.
		}
		archiveExec(t, f.db, `INSERT INTO conversation_turns
 (id, conversation_id, handled_by_session_id, branch_id, provider_turn_id, state, requested_at, completed_at)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("turn-%d", i), f.conversation, f.session,
			branch, fmt.Sprintf("native-turn-%d", i), turnState, testNow, testNow)
		payload := fmt.Sprintf(`{"turnId":"turn-%d","state":%q,"ack":true,"checkpoint":true,"replayNeeded":false}`, i, state)
		insertCensusEvent(t, f, branch, "", "message.delta", payload)
	}
	insertCensusEvent(t, f, "epoch-0", "event-1", "message.delta", "identified")
	insertCensusEvent(t, f, "epoch-1", "event-1", "message.delta", "duplicate ignored")
	for _, method := range []string{"error", "command.output.delta", "reasoning.delta", "turn.completed", "turn.cancelled", "interrupt", "unknown.method", ""} {
		insertCensusEvent(t, f, "missing-branch", "", method, "no trusted provenance")
	}
	// A second conversation can use the same provider event ID independently.
	otherSession, err := f.s.CreateSession(context.Background(), sampleRecord("archive"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := f.s.CreateConversation(context.Background(), "other-census-conversation", domain.ConversationScopeSession, "archive", otherSession.ID, testNow)
	if err != nil {
		t.Fatal(err)
	}
	g := f
	g.conversation = other.ID
	g.session = otherSession.ID
	insertCensusEvent(t, g, other.ID+":root", "event-1", "message.delta", "other conversation")
	before := retentionRows(t, f.db)
	readAll := func() []store.RetentionCensusPage {
		c := beginCensus(t, f.s, censusOptions())
		var pages []store.RetentionCensusPage
		var cursor *store.RetentionCensusCursor
		var total int64
		for {
			p := censusPage(t, c, cursor)
			total += p.ScannedRows
			if p.TotalScannedRows != total || p.ScannedRows > 3 || p.CallerSnapshotID != "fixture-window" {
				t.Fatalf("wrong bounded page: %+v", p)
			}
			if len(pages) > 0 && p.SnapshotID != pages[0].SnapshotID {
				t.Fatal("snapshot changed across pages")
			}
			pages = append(pages, p)
			if p.Next == nil {
				if !p.WindowComplete || p.UnscannedRows != "ZERO_IN_WINDOW" || total != int64(len(before)) {
					t.Fatalf("lost rows or claimed incorrect coverage: %+v", p)
				}
				break
			}
			cursor = p.Next
		}
		return pages
	}
	a, b := readAll(), readAll()
	if len(before) != 13 || a[0].SnapshotID == b[0].SnapshotID {
		t.Fatalf("wrong dedupe count or reused snapshot: rows=%d", len(before))
	}
	for i := range a {
		// Transaction identities deliberately differ; page contents/order do not.
		a[i].SnapshotID, a[i].Revision = "", ""
		b[i].SnapshotID, b[i].Revision = "", ""
		if a[i].Next != nil {
			a[i].Next.SnapshotID, b[i].Next.SnapshotID = "", ""
		}
	}
	if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(before, retentionRows(t, f.db)) {
		t.Fatal("nondeterministic scan or mutated archive")
	}
}

func TestRetentionCensusNativeSnapshotWithConcurrentWriter(t *testing.T) {
	f := newProviderBranchFixture(t)
	for range 3 {
		insertCensusEvent(t, f, "missing", "", "message.delta", "old")
	}
	opts := censusOptions()
	opts.PageRows = 1
	c := beginCensus(t, f.s, opts)
	first := censusPage(t, c, nil)
	done := make(chan error, 1)
	go func() {
		_, err := f.db.Exec(`UPDATE conversation_provider_events SET method='writer.changed', payload_json='new-longer-payload' WHERE id=2`)
		if err == nil {
			_, err = f.db.Exec(legacyProviderEventInsert, f.conversation, f.session, "new-event", "new.method", "new", testNow)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read snapshot blocked WAL writer")
	}
	second := censusPage(t, c, first.Next)
	third := censusPage(t, c, second.Next)
	if second.Counts[0].Method != "message.delta" || second.Counts[0].LogicalPayloadBytes != 3 ||
		third.TotalScannedRows != 3 || !third.WindowComplete || third.HighWaterID != 3 || third.Revision != first.Revision {
		t.Fatalf("writer changed pinned snapshot: second=%+v third=%+v", second, third)
	}
	freshOpts := censusOptions()
	freshOpts.PageRows = 10
	fresh := censusPage(t, beginCensus(t, f.s, freshOpts), nil)
	if fresh.ScannedRows != 4 || fresh.SourceMaxID != 4 || fresh.Revision == first.Revision {
		t.Fatalf("new snapshot failed to observe writer: %+v", fresh)
	}
}

func TestRetentionCensusCursorsAndHighWater(t *testing.T) {
	f := newProviderBranchFixture(t)
	for range 4 {
		insertCensusEvent(t, f, "", "", "message.delta", "x")
	}
	opts := censusOptions()
	ceiling := int64(2)
	opts.HighWaterID, opts.PageRows = &ceiling, 1
	c := beginCensus(t, f.s, opts)
	ceiling = 4 // The caller cannot mutate an already bound ceiling.
	p := censusPage(t, c, nil)
	if p.HighWaterID != 2 || p.SourceMaxID != 4 {
		t.Fatalf("wrong caller ceiling: %+v", p)
	}
	for _, cursor := range []*store.RetentionCensusCursor{nil,
		{SnapshotID: p.SnapshotID, AfterID: 100, Page: 1},
		{SnapshotID: "foreign", AfterID: 1, Page: 1},
		{SnapshotID: p.SnapshotID, AfterID: 1, Page: 2}} {
		if _, err := c.Page(context.Background(), cursor); !errors.Is(err, store.ErrRetentionCensusCursor) {
			t.Fatalf("accepted invalid cursor: %v", err)
		}
	}
	other := beginCensus(t, f.s, censusOptions())
	if _, err := other.Page(context.Background(), p.Next); !errors.Is(err, store.ErrRetentionCensusCursor) {
		t.Fatalf("accepted foreign snapshot cursor: %v", err)
	}
	last := censusPage(t, c, p.Next)
	if !last.WindowComplete || last.TotalScannedRows != 2 {
		t.Fatalf("ceiling not honored: %+v", last)
	}
	if _, err := c.Page(context.Background(), p.Next); !errors.Is(err, store.ErrRetentionCensusClosed) {
		t.Fatalf("reused completed snapshot: %v", err)
	}
	tooHigh := int64(5)
	opts.HighWaterID = &tooHigh
	if _, err := f.s.BeginRetentionCensus(context.Background(), opts); err == nil {
		t.Fatal("accepted high-water beyond pinned source")
	}
	opts.HighWaterID, opts.MaxPages = nil, 1
	limited := censusPage(t, beginCensus(t, f.s, opts), nil)
	if limited.WindowComplete || limited.StopReason != "PAGE_LIMIT" || limited.Next != nil || limited.UnscannedRows != "UNKNOWN" {
		t.Fatalf("page exhaustion claimed coverage: %+v", limited)
	}
}

func TestRetentionCensusPayloadBytesAndBoundedMetadata(t *testing.T) {
	f := newProviderBranchFixture(t)
	large := strings.Repeat("payload-secret", 1<<17)
	payloads := []any{"", "é界😀", "a\x00界", []byte{0, 255, 1}, large}
	var expected int64
	for _, payload := range payloads {
		switch v := payload.(type) {
		case string:
			expected += int64(len(v))
		case []byte:
			expected += int64(len(v))
		}
		insertCensusEvent(t, f, large, large, "message.delta", payload)
		// Use anonymous IDs after the first row to avoid the unique dedupe index.
		archiveExec(t, f.db, `UPDATE conversation_provider_events SET provider_event_id=''`)
	}
	insertCensusEvent(t, f, "", "", large, "x")
	insertCensusEvent(t, f, "", "", "", "x")
	insertCensusEvent(t, f, "", "", string([]byte{0xff}), "x")
	opts := censusOptions()
	opts.PageRows = 10
	p := censusPage(t, beginCensus(t, f.s, opts), nil)
	var total int64
	for _, count := range p.Counts {
		total += count.LogicalPayloadBytes
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if p.ScannedRows != 8 || p.UnknownMethodRows != 2 || total != expected+3 ||
		len(b) > opts.MaxReportBytes || strings.Contains(string(b), "payload-secret") ||
		!slices.Contains(p.SourceGaps, store.RetentionGapMethod) {
		t.Fatalf("wrong bounded byte accounting: bytes=%d expected=%d page=%+v", total, expected+3, p)
	}
}

func TestRetentionCensusEmptyReadOnlyAndCancellation(t *testing.T) {
	f := newProviderBranchFixture(t)
	ro, err := sqlite.OpenReadOnly(context.Background(), strings.TrimSuffix(f.path, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ro.Close() })
	p := censusPage(t, beginCensus(t, ro, censusOptions()), nil)
	if !p.WindowComplete || p.ScannedRows != 0 || p.HighWaterID != 0 || len(p.Counts) != 0 {
		t.Fatalf("wrong empty snapshot: %+v", p)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c, err := f.s.BeginRetentionCensus(ctx, censusOptions())
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := c.Page(context.Background(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation ignored: %v", err)
	}
	c = beginCensus(t, f.s, censusOptions())
	pageCtx, pageCancel := context.WithCancel(context.Background())
	pageCancel()
	if _, err := c.Page(pageCtx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("page cancellation ignored: %v", err)
	}
	if _, err := c.Page(context.Background(), nil); !errors.Is(err, store.ErrRetentionCensusClosed) {
		t.Fatalf("canceled page snapshot reused: %v", err)
	}
}

func TestRetentionCensusDeadline(t *testing.T) {
	// MaxDuration includes opening and pinning the snapshot. A 50ms wall-clock
	// budget can expire during that setup under -race, before Page is tested.
	// Keep a real SQLite transaction, but advance its clock only at test barriers.
	synctest.Test(t, func(t *testing.T) {
		db, err := sql.Open("sqlite", filepath.ToSlash(filepath.Join(t.TempDir(), "census.db")))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		db.SetMaxOpenConns(1)
		archiveExec(t, db, `CREATE TABLE conversation_provider_events (id INTEGER PRIMARY KEY, method TEXT, provider_event_id TEXT, payload_json TEXT)`)
		archiveExec(t, db, `INSERT INTO conversation_provider_events VALUES (1, 'message.delta', '', 'a'), (2, 'message.delta', '', 'b')`)
		s := store.NewStore(db, db)
		opts := censusOptions()
		opts.MaxDuration, opts.PageRows = 50*time.Millisecond, 1

		// A held reader deterministically consumes the setup budget. This must
		// fail at Begin rather than masquerading as a Page cancellation failure.
		held, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Close() }()
		started := time.Now()
		if c, err := s.BeginRetentionCensus(context.Background(), opts); !errors.Is(err, context.DeadlineExceeded) || c != nil {
			t.Fatalf("reader wait ignored setup deadline: census=%v err=%v", c, err)
		}
		if time.Since(started) != opts.MaxDuration || db.Stats().WaitCount != 1 {
			t.Fatalf("setup did not exhaust the reader wait budget: elapsed=%v stats=%+v", time.Since(started), db.Stats())
		}
		if err := held.Close(); err != nil {
			t.Fatal(err)
		}

		c := beginCensus(t, s, opts)
		time.Sleep(opts.MaxDuration - time.Nanosecond)
		first := censusPage(t, c, nil)
		if first.ScannedRows != 1 || first.Next == nil {
			t.Fatalf("snapshot expired before deadline: %+v", first)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if db.Stats().InUse != 0 {
			t.Fatal("deadline did not release snapshot reader")
		}
		if p, err := c.Page(context.Background(), first.Next); !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(p, store.RetentionCensusPage{}) {
			t.Fatalf("snapshot deadline returned data or wrong error: page=%+v err=%v", p, err)
		}
		if _, err := c.Page(context.Background(), first.Next); !errors.Is(err, store.ErrRetentionCensusClosed) {
			t.Fatalf("expired snapshot reused: %v", err)
		}
		opts.PageRows = 3
		fresh := censusPage(t, beginCensus(t, s, opts), nil)
		if fresh.ScannedRows != 2 || !fresh.WindowComplete {
			t.Fatalf("deadline changed source rows or prevented a fresh snapshot: %+v", fresh)
		}
	})
}

func TestRetentionCensusRejectsBoundsAndOversizeReport(t *testing.T) {
	f := newProviderBranchFixture(t)
	for _, change := range []func(*store.RetentionCensusOptions){
		func(o *store.RetentionCensusOptions) { o.PageRows = 0 },
		func(o *store.RetentionCensusOptions) { o.PageRows = 1001 },
		func(o *store.RetentionCensusOptions) { o.MaxPages = 0 },
		func(o *store.RetentionCensusOptions) { o.MaxPages = 1001 },
		func(o *store.RetentionCensusOptions) { o.MaxReportBytes = 8191 },
		func(o *store.RetentionCensusOptions) { o.MaxReportBytes = 1<<20 + 1 },
		func(o *store.RetentionCensusOptions) { o.MaxDuration = 0 },
		func(o *store.RetentionCensusOptions) { o.MaxDuration = time.Minute + 1 },
		func(o *store.RetentionCensusOptions) { o.CallerSnapshotID = strings.Repeat("x", 129) },
		func(o *store.RetentionCensusOptions) { n := int64(-1); o.HighWaterID = &n },
	} {
		opts := censusOptions()
		change(&opts)
		if c, err := f.s.BeginRetentionCensus(context.Background(), opts); err == nil {
			_ = c.Close()
			t.Fatalf("accepted invalid bounds: %+v", opts)
		}
	}
	for i := range 50 {
		insertCensusEvent(t, f, "", "", fmt.Sprintf("%03d%s", i, strings.Repeat("m", 120)), "x")
	}
	opts := censusOptions()
	opts.PageRows = 100
	c := beginCensus(t, f.s, opts)
	p, err := c.Page(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "byte limit") || p.ScannedRows != 0 || p.SnapshotID != "" {
		t.Fatalf("oversize report returned success/partial data: %+v %v", p, err)
	}
	if _, err := c.Page(context.Background(), nil); !errors.Is(err, store.ErrRetentionCensusClosed) {
		t.Fatalf("oversize snapshot reused: %v", err)
	}
}

func TestRetentionCensusUnknownStorageTypesAndIDEdges(t *testing.T) {
	// A disposable minimal source allows storage classes excluded by today's
	// NOT NULL TEXT schema. Unknown/imported provenance must remain conservative.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	archiveExec(t, db, `CREATE TABLE conversation_provider_events (id INTEGER PRIMARY KEY, method, provider_event_id, payload_json)`)
	archiveExec(t, db, `INSERT INTO conversation_provider_events VALUES (?, NULL, NULL, NULL), (0, ?, X'61', 42), (5, '', '', 1.5)`, math.MinInt64, []byte("blob-method"))
	s := store.NewStore(db, db)
	opts := censusOptions()
	opts.PageRows = 1
	c := beginCensus(t, s, opts)
	var cursor *store.RetentionCensusCursor
	for i := range 3 {
		p := censusPage(t, c, cursor)
		if p.ScannedRows != 1 || p.Counts[0].PayloadBytesUnknownRows != 1 || p.Counts[0].IdentifiedRows != 0 ||
			!slices.Contains(p.SourceGaps, store.RetentionGapPayload) {
			t.Fatalf("unsupported storage class gained certainty: %+v", p)
		}
		if i == 2 && (!p.WindowComplete || p.TotalScannedRows != 3) {
			t.Fatalf("lost ID edge rows: %+v", p)
		}
		cursor = p.Next
	}
}
