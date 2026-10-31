package chat_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func ao16Strings(table ao16Table) [][]string {
	result := make([][]string, len(table.Rows))
	for i, row := range table.Rows {
		result[i] = make([]string, len(row))
		for j, v := range row {
			if v == nil {
				result[i][j] = "<NULL>"
			} else {
				result[i][j] = *v
			}
		}
	}
	return result
}

// These literals do not come from the encoder, manifest, source snapshot, or
// retained projection objects. Every raw column is checked independently.
func ao16AssertRaw(t *testing.T, db *sql.DB) {
	t.Helper()
	table, err := ao16ReadTable(db, "conversation_provider_events")
	ao16Must(t, err)
	want := [][]string{
		{"2", "ao16", "p1-1", "identified", "turn/completed", `{"state":"completed"}`, "2026-08-05T10:00:00.123456789Z", "ao16:root"},
		{"5", "ao16", "p1-1", "", "delta", `{ "text": "same\nbytes" }`, "2026-08-05T10:00:00.123456789Z", "ao16:root"},
		{"9", "ao16", "p1-1", "", "delta", `{ "text": "same\nbytes" }`, "2026-08-05T10:00:00.123456789Z", "ao16:root"},
		{"12", "ao16", "p1-1", "import:event", "error", `{"error":"provider failed","scope":"import:provider"}`, "2026-08-05T10:00:00.123456789Z", "child"},
		{"20", "ao16", "p1-1", "sibling:event", "turn/interrupted", `{"state":"interrupted"}`, "2026-08-05 10:00:00.123456789+00:00", "sibling"},
	}
	if !reflect.DeepEqual(ao16Strings(table), want) {
		t.Fatalf("exhaustive restored raw rows = %#v", ao16Strings(table))
	}
}

func ao16ReaderIDs(t *testing.T, st *sqlite.Store, db *sql.DB, branch string) []int64 {
	t.Helper()
	ao16Exec(t, db, `UPDATE conversations SET active_branch_id=? WHERE id='ao16'`, branch)
	var ids []int64
	var after int64
	for page := 0; page < 10; page++ {
		rows, err := st.ProviderEventsSince(context.Background(), "ao16", after, 1)
		ao16Must(t, err)
		if len(rows) == 0 {
			return ids
		}
		if len(rows) != 1 || rows[0].ID <= after {
			t.Fatal("pagination failed to advance")
		}
		ids = append(ids, rows[0].ID)
		after = rows[0].ID
	}
	t.Fatal("pagination did not terminate")
	return nil
}

func TestAO16RestoreIndependentRowsAndBranches(t *testing.T) {
	source, sourceDB := ao16Seed(t)
	ao16AssertRaw(t, sourceDB)
	wantBranches := map[string][]int64{"ao16:root": {2, 5, 9}, "child": {2, 5, 9, 12}, "sibling": {2, 5, 9, 20}}
	for branch, want := range wantBranches {
		if got := ao16ReaderIDs(t, source, sourceDB, branch); !reflect.DeepEqual(got, want) {
			t.Fatalf("source %s = %v", branch, got)
		}
	}
	ao16Exec(t, sourceDB, `UPDATE conversations SET active_branch_id='ao16:root' WHERE id='ao16'`)
	archive := ao16Archive(t, sourceDB)
	ao16Must(t, sourceDB.Close())
	ao16Must(t, source.Close())
	destination, db := ao16Open(t)
	ao16Must(t, ao16Restore(db, archive))
	ao16AssertRaw(t, db)
	for branch, want := range wantBranches {
		if got := ao16ReaderIDs(t, destination, db, branch); !reflect.DeepEqual(got, want) {
			t.Fatalf("restored %s = %v want %v", branch, got, want)
		}
	}
	var highWater int64
	ao16Must(t, db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='conversation_provider_events'`).Scan(&highWater))
	if highWater != 21 {
		t.Fatalf("lost ignored-duplicate cursor gap: %d", highWater)
	}
	// Direct context oracles preserve ownership, import scope, outcomes and
	// obligations. No terminal state or review absence establishes deletion rights.
	var parent, thread, scope, owner string
	var cutoff, scoped int
	ao16Must(t, db.QueryRow(`SELECT parent_branch_id,provider_conversation_id,provider_scope_id,session_id,fork_after_sequence,provider_ids_scoped FROM conversation_branches WHERE id='child'`).Scan(&parent, &thread, &scope, &owner, &cutoff, &scoped))
	if parent != "ao16:root" || thread != "thread-2" || scope != "import:provider" || owner != "p1-1" || cutoff != 2 || scoped != 1 {
		t.Fatalf("lost branch/import ownership: %s %s %s %s %d %d", parent, thread, scope, owner, cutoff, scoped)
	}
	turns, err := db.Query(`SELECT id,state,error_message FROM conversation_turns ORDER BY id`)
	ao16Must(t, err)
	var outcomes []string
	for turns.Next() {
		var id, state, message string
		ao16Must(t, turns.Scan(&id, &state, &message))
		outcomes = append(outcomes, id+":"+state+":"+message)
	}
	ao16Must(t, turns.Err())
	ao16Must(t, turns.Close())
	if !reflect.DeepEqual(outcomes, []string{"failed:failed:provider failed", "imported:recovered:", "interrupted:interrupted:", "settled:completed:"}) {
		t.Fatalf("outcomes = %v", outcomes)
	}
	var pinned int
	var review, checkpoint, native string
	ao16Must(t, db.QueryRow(`SELECT is_pinned,conversation_checkpoint_state,conversation_checkpoint_native_id FROM sessions WHERE id='p1-1'`).Scan(&pinned, &checkpoint, &native))
	ao16Must(t, db.QueryRow(`SELECT status FROM review_run WHERE id='pending-review' AND target_sha=?`, ao16Revision).Scan(&review))
	if pinned != 1 || review != "running" || checkpoint != "complete" || native != "thread-1" {
		t.Fatalf("lost HOLD evidence: pin=%d review=%s checkpoint=%s native=%s", pinned, review, checkpoint, native)
	}
	t.Log("HOLD: pending review/pin, incomplete native coverage and no live native Resume qualification; deletion_authority=NONE")
}

func ao16Fingerprint(t *testing.T, db *sql.DB) string {
	t.Helper()
	var snapshot []ao16Table
	for _, name := range append(append([]string{}, ao16Tables...), "change_log", "sqlite_sequence") {
		table, err := ao16ReadTable(db, name)
		ao16Must(t, err)
		snapshot = append(snapshot, table)
	}
	raw, err := json.Marshal(snapshot)
	ao16Must(t, err)
	return string(raw)
}

func TestAO16RestoreRejectsWithoutDestinationChanges(t *testing.T) {
	source, sourceDB := ao16Seed(t)
	archive := ao16Archive(t, sourceDB)
	ao16Must(t, sourceDB.Close())
	ao16Must(t, source.Close())
	var envelope ao16Envelope
	ao16Must(t, json.Unmarshal(archive, &envelope))
	cases := []struct {
		name   string
		mutate func(*ao16Body)
	}{
		{"sequence high-water", func(b *ao16Body) { b.EventHighWater = 19 }},
		{"version", func(b *ao16Body) { b.Version = 2 }},
		{"revision", func(b *ao16Body) { b.Revision = strings.Repeat("0", 40) }},
		{"schema", func(b *ao16Body) { b.Tables[9].Columns[5] = "wrong_payload" }},
		{"table order", func(b *ao16Body) { b.Tables[0], b.Tables[1] = b.Tables[1], b.Tables[0] }},
		{"reordered events", func(b *ao16Body) { b.Tables[9].Rows[0], b.Tables[9].Rows[1] = b.Tables[9].Rows[1], b.Tables[9].Rows[0] }},
		{"missing ID", func(b *ao16Body) { b.Tables[9].Rows = b.Tables[9].Rows[1:] }},
		{"wrong ID", func(b *ao16Body) { s := "3"; b.Tables[9].Rows[0][0] = &s }},
		{"duplicate ID", func(b *ao16Body) { s := "2"; b.Tables[9].Rows[1][0] = &s }},
		{"missing branch", func(b *ao16Body) { s := "unknown"; b.Tables[9].Rows[3][7] = &s }},
		{"wrong session", func(b *ao16Body) { s := "other-session"; b.Tables[9].Rows[3][2] = &s }},
		{"wrong conversation", func(b *ao16Body) { s := "other-conversation"; b.Tables[9].Rows[3][1] = &s }},
	}
	inputs := map[string][]byte{"malformed": []byte("{"), "truncated": archive[:len(archive)-9], "trailing": append(append([]byte{}, archive...), []byte(" {}")...)}
	badHash := envelope
	badHash.SHA256 = strings.Repeat("0", 64)
	var err error
	inputs["SHA-256"], err = json.Marshal(badHash)
	ao16Must(t, err)
	for _, tc := range cases {
		var body ao16Body
		ao16Must(t, json.Unmarshal(envelope.Body, &body))
		tc.mutate(&body)
		inputs[tc.name], err = ao16Seal(body)
		ao16Must(t, err)
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			_, db := ao16Open(t)
			ao16Exec(t, db, `INSERT INTO projects(id,path,registered_at) VALUES ('sentinel','/untouched',?)`, ao16Stamp)
			before := ao16Fingerprint(t, db)
			if err := ao16Restore(db, input); err == nil {
				t.Fatal("invalid archive admitted")
			}
			if after := ao16Fingerprint(t, db); after != before {
				t.Fatal("rejection changed destination rows, CDC or sequence")
			}
		})
	}
}

func ao16History() []ports.ChatEvent {
	return []ports.ChatEvent{
		{Kind: ports.ChatEventTurnStarted, ProviderEventID: "history-start", ProviderTurnID: "native-turn-1"},
		{Kind: ports.ChatEventUserMessageCompleted, ProviderEventID: "history-user", ProviderTurnID: "native-turn-1", ProviderItemID: "native-user", Text: "question"},
		{Kind: ports.ChatEventMessageCompleted, ProviderEventID: "history-answer", ProviderTurnID: "native-turn-1", ProviderItemID: "native-answer", Text: "answer"},
		{Kind: ports.ChatEventTurnCompleted, ProviderEventID: "history-complete", ProviderTurnID: "native-turn-1", TurnState: domain.TurnStateCompleted},
	}
}

// Fixed semantic expectations are independent of the archive and provider replay
// objects. Check every projected message, its native item/turn identity, ordering,
// owner and settled state after each fake Resume, including repeated imports.
func ao16AssertResumeProjection(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT m.id,m.conversation_id,m.turn_id,m.sequence,m.role,m.origin,m.text,m.provider_item_id,m.branch_id,m.streaming,t.provider_turn_id,t.handled_by_session_id,t.state,t.branch_id FROM conversation_messages m JOIN conversation_turns t ON t.id=m.turn_id WHERE m.conversation_id='ao16' ORDER BY m.sequence,m.id`)
	ao16Must(t, err)
	var got [][]string
	for rows.Next() {
		cells := make([]string, 14)
		pointers := make([]any, len(cells))
		for i := range cells {
			pointers[i] = &cells[i]
		}
		ao16Must(t, rows.Scan(pointers...))
		got = append(got, cells)
	}
	ao16Must(t, rows.Err())
	ao16Must(t, rows.Close())
	want := [][]string{
		{"user", "ao16", "settled", "1", "user", "human", "question", "native-user", "ao16:root", "0", "native-turn-1", "p1-1", "completed", "ao16:root"},
		{"answer", "ao16", "settled", "2", "assistant", "provider", "answer", "native-answer", "ao16:root", "0", "native-turn-1", "p1-1", "completed", "ao16:root"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fake Resume changed projected semantic history: got %#v want %#v", got, want)
	}
	var project, session, current, branch, owner, native string
	var sequence int64
	ao16Must(t, db.QueryRow(`SELECT c.project_id,c.session_id,c.current_session_id,c.active_branch_id,c.latest_sequence,b.session_id,b.provider_conversation_id FROM conversations c JOIN conversation_branches b ON b.id=c.active_branch_id WHERE c.id='ao16'`).Scan(&project, &session, &current, &branch, &sequence, &owner, &native))
	if project != "p1" || session != "p1-1" || current != "p1-1" || branch != "ao16:root" || sequence != 2 || owner != "p1-1" || native != "thread-1" {
		t.Fatalf("fake Resume changed conversation/owner binding: %s %s %s %s %d %s %s", project, session, current, branch, sequence, owner, native)
	}
}

func ao16ResumeRowFingerprint(t *testing.T, db *sql.DB) string {
	t.Helper()
	var tables []ao16Table
	for _, name := range []string{"conversation_turns", "conversation_messages", "conversation_activities", "conversation_provider_events"} {
		table, err := ao16ReadTable(db, name)
		ao16Must(t, err)
		tables = append(tables, table)
	}
	raw, err := json.Marshal(tables)
	ao16Must(t, err)
	return string(raw)
}

// Test-only fake Resume/history admission is separate from native provider
// continuation. In particular the serialized raw archive is not ReadHistory.
func ao16Resume(t *testing.T, st *sqlite.Store, db *sql.DB, variant string, continuation bool) {
	t.Helper()
	ctx := context.Background()
	rec, found, err := st.GetSession(ctx, testSession)
	ao16Must(t, err)
	if !found {
		t.Fatal("restored session missing")
	}
	events := ao16History()
	var wantMismatches []ports.ChatHistoryMismatchDimension
	switch variant {
	case "matching":
	case "missing history":
		events = nil
		wantMismatches = []ports.ChatHistoryMismatchDimension{ports.ChatHistoryMismatchTrustedTurn, ports.ChatHistoryMismatchTrustedUserText, ports.ChatHistoryMismatchTrustedAssistantText, ports.ChatHistoryMismatchAOHighWater}
	case "mismatched checkpoint":
		rec.Metadata.ConversationCheckpointNativeID = "other-thread"
		wantMismatches = []ports.ChatHistoryMismatchDimension{ports.ChatHistoryMismatchNativeIdentity}
	case "missing checkpoint", "missing checkpoint with matching history":
		rec.Metadata.ConversationCheckpointState = ""
		rec.Metadata.ConversationCheckpointGeneration = ""
		rec.Metadata.ConversationCheckpointNativeID = ""
		// Preserve the original combined negative, while the matching-history
		// control below proves that checkpoint absence alone is not a rejection.
		if variant == "missing checkpoint" {
			rec.Metadata.LatestUserPrompt = "unproven prompt absent from native history"
			wantMismatches = []ports.ChatHistoryMismatchDimension{ports.ChatHistoryMismatchUntrustedUserText}
		}
	default:
		t.Fatalf("unknown fake Resume case %q", variant)
	}
	ao16Must(t, st.UpdateSession(ctx, rec))
	var next atomic.Int64
	for attempt := 0; attempt < 2; attempt++ {
		conversation := &nativeHistoryConversation{fakeConversation: newFakeConversation(), events: events}
		conversation.providerConversationID = "thread-1"
		resumed := false
		svc := chatsvc.New(chatsvc.Options{Store: st, Sessions: st, Reader: fullSnapshotReader(st), Log: slog.New(slog.DiscardHandler), Drivers: fakeRegistry{driver: fakeDriver{resume: func(cfg ports.ChatResumeConfig) (ports.ChatConversation, error) {
			resumed = true
			if cfg.ProviderConversationID != "thread-1" {
				t.Errorf("Resume provider handle = %q", cfg.ProviderConversationID)
			}
			return conversation, nil
		}}}, NewID: func() string { return fmt.Sprintf("ao16-import-%d", next.Add(1)) }})
		t.Cleanup(func() { _ = svc.Stop(context.Background(), testSession) })
		beforeRows := ao16ResumeRowFingerprint(t, db)
		startCtx, cancel := context.WithTimeout(ctx, time.Second)
		controller, err := svc.Start(startCtx, chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex, WorkspacePath: "/synthetic/ao16", ProviderConversationID: "thread-1", HistoryMode: ports.ChatHistoryRequired, HistoryPolicy: domain.SessionInterfaceTransitionHistoryStrict})
		cancel()
		if len(wantMismatches) > 0 {
			var unsettled *ports.ChatHistoryUnsettledError
			if !errors.Is(err, ports.ErrChatHistoryUnsettled) || !errors.As(err, &unsettled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Fatalf("%s: expected typed history rejection, got %T: %v", variant, err, err)
			}
			if !reflect.DeepEqual(ports.ChatHistoryMismatchDimensions(err), wantMismatches) {
				t.Fatalf("%s: mismatch dimensions=%v want %v", variant, ports.ChatHistoryMismatchDimensions(err), wantMismatches)
			}
			if !resumed {
				t.Fatal("negative admission did not reach fake Resume")
			}
			// Native identity is a hard checkpoint gate before ReadHistory;
			// text/high-water mismatches must exercise the history reader.
			if reads := conversation.historyReads(); (variant == "mismatched checkpoint" && reads != 0) || (variant != "mismatched checkpoint" && reads == 0) {
				t.Fatalf("%s: unexpected history reads %d", variant, reads)
			}
			if controller != nil {
				t.Fatal("failed Start returned a controller")
			}
			if published, lookupErr := svc.Controller(testSession); published != nil || !errors.Is(lookupErr, chatsvc.ErrNoController) {
				t.Fatalf("failed Start published controller: %v, %v", published, lookupErr)
			}
			if after := ao16ResumeRowFingerprint(t, db); after != beforeRows {
				t.Fatal("rejected Start mutated projected/raw history")
			}
			ao16AssertResumeProjection(t, db)
			_ = svc.Stop(ctx, testSession)
			t.Logf("HOLD: %s rejected: %v", variant, err)
			return
		}
		if err != nil {
			_ = svc.Stop(ctx, testSession)
			t.Fatal(err)
		}
		if !resumed || conversation.historyReads() == 0 {
			t.Fatal("fake Resume/history path not exercised")
		}
		var messages int
		ao16Must(t, db.QueryRow(`SELECT count(*) FROM conversation_messages WHERE conversation_id='ao16'`).Scan(&messages))
		if messages != 2 {
			t.Fatalf("repeated import duplicated projected history: %d messages", messages)
		}
		ao16AssertResumeProjection(t, db)
		if continuation && attempt == 1 {
			// Unsupported normalized event kinds are still durable raw evidence.
			conversation.emit(ports.ChatEvent{Kind: ports.ChatEventKind("ao16.fixture.continuation"), ProviderEventID: "post-restore", ProviderConversationID: "thread-1", ProviderTurnID: "native-turn-1", ProviderItemID: "continuation-item", Text: "continuation evidence"})
			deadline := time.Now().Add(2 * time.Second)
			var count int
			for time.Now().Before(deadline) {
				ao16Must(t, db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE provider_event_id='post-restore'`).Scan(&count))
				if count == 1 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if count != 1 {
				t.Fatal("new fake-provider continuation event was not archived")
			}
			var conversationID, sessionID, branchID, method, payload string
			var eventID int64
			ao16Must(t, db.QueryRow(`SELECT id,conversation_id,session_id,branch_id,method,payload_json FROM conversation_provider_events WHERE provider_event_id='post-restore'`).Scan(&eventID, &conversationID, &sessionID, &branchID, &method, &payload))
			if eventID <= 21 || conversationID != "ao16" || sessionID != "p1-1" || branchID != "ao16:root" || method != "ao16.fixture.continuation" {
				t.Fatalf("wrong continuation order/ownership: %d %s %s %s %s", eventID, conversationID, sessionID, branchID, method)
			}
			var record map[string]json.RawMessage
			ao16Must(t, json.Unmarshal([]byte(payload), &record))
			for key, want := range map[string]string{"kind": "ao16.fixture.continuation", "providerEventId": "post-restore", "providerConversationId": "thread-1", "providerTurnId": "native-turn-1", "providerItemId": "continuation-item", "text": "continuation evidence"} {
				var got string
				ao16Must(t, json.Unmarshal(record[key], &got))
				if got != want {
					t.Fatalf("continuation payload %s=%q want %q", key, got, want)
				}
			}
			ao16AssertResumeProjection(t, db)
		}
		ao16Must(t, svc.Stop(ctx, testSession))
	}
}

func TestAO16RestoreFakeResumeEquivalence(t *testing.T) {
	for _, variant := range []string{"matching", "missing history", "missing checkpoint", "mismatched checkpoint", "missing checkpoint with matching history"} {
		t.Run(variant, func(t *testing.T) {
			source, sourceDB := ao16Seed(t)
			archive := ao16Archive(t, sourceDB)
			ao16Resume(t, source, sourceDB, variant, false)
			ao16Must(t, sourceDB.Close())
			ao16Must(t, source.Close())
			destination, db := ao16Open(t)
			ao16Must(t, ao16Restore(db, archive))
			ao16Resume(t, destination, db, variant, true)
		})
	}
	t.Log("HOLD: fake-provider author proof only; live native Resume, independent review, retention and deletion remain unqualified")
}
