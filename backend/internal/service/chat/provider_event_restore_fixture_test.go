package chat_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// This is a deliberately bounded test transport, not a production archive API
// or permission to prune. No provider-native files are reconstructed here.
const ao16Revision = "76c1daea92619fb4249ee0fdd587f5f4e0c53c65"
const ao16Stamp = "2026-08-05T10:00:00.123456789Z"

var ao16Tables = []string{"projects", "sessions", "conversations", "conversation_branches", "conversation_turns", "conversation_messages", "conversation_activities", "review", "review_run", "conversation_provider_events"}
var ao16EventColumns = []string{"id", "conversation_id", "session_id", "provider_event_id", "method", "payload_json", "received_at", "branch_id"}

type ao16Table struct {
	Name    string
	Columns []string
	Rows    [][]*string
}
type ao16Body struct {
	Version        int
	Revision       string
	EventIDs       []int64
	EventHighWater int64
	Tables         []ao16Table
}
type ao16Envelope struct {
	Body   json.RawMessage
	SHA256 string
}

func ao16Must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func ao16Open(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	st, err := sqlite.Open(dir) // Each destination starts empty and runs migrations.
	ao16Must(t, err)
	t.Cleanup(func() { _ = st.Close() })
	// A Windows drive must be in the URI path (file:///C:/...), not the
	// authority (file://C:/...). The destination remains this t.TempDir.
	path := filepath.ToSlash(filepath.Join(dir, "ao.db"))
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?_pragma=foreign_keys(1)")
	ao16Must(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return st, db
}

func ao16Exec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	_, err := db.Exec(query, args...)
	ao16Must(t, err)
}

func ao16Seed(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()
	st, db := ao16Open(t)
	ctx := context.Background()
	now, err := time.Parse(time.RFC3339Nano, ao16Stamp)
	ao16Must(t, err)
	ao16Must(t, st.UpsertProject(ctx, domain.ProjectRecord{ID: "p1", Path: "/synthetic/ao16", RegisteredAt: now}))
	_, err = st.CreateSession(ctx, domain.SessionRecord{ID: testSession, ProjectID: testProject, Kind: domain.KindWorker, Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, CreatedAt: now, UpdatedAt: now})
	ao16Must(t, err)
	// Checkpoint and pin are durable session facts, never a new display status.
	rec, found, err := st.GetSession(ctx, testSession)
	ao16Must(t, err)
	if !found {
		t.Fatal("missing seeded session")
	}
	rec.Metadata.ProviderConversationID = "thread-1"
	rec.Metadata.ControllerGeneration = "generation-1"
	rec.Metadata.ConversationCheckpointState = domain.ConversationCheckpointComplete
	rec.Metadata.ConversationCheckpointGeneration = "generation-1"
	rec.Metadata.ConversationCheckpointNativeID = "thread-1"
	rec.Metadata.ConversationCheckpointTurnID = "native-turn-1"
	rec.Metadata.LatestUserPrompt = "question"
	rec.Metadata.LatestAssistantUpdate = "answer"
	ao16Must(t, st.UpdateSession(ctx, rec))
	_, err = st.CreateConversation(ctx, "ao16", domain.ConversationScopeSession, testProject, testSession, now)
	ao16Must(t, err)
	ao16Exec(t, db, `UPDATE sessions SET is_pinned=1 WHERE id='p1-1'`)
	ao16Exec(t, db, `INSERT INTO conversation_branches(id,conversation_id,session_id,provider_conversation_id,parent_branch_id,fork_after_sequence,created_at,provider_scope_id,provider_ids_scoped) VALUES ('child','ao16','p1-1','thread-2','ao16:root',2,?,'import:provider',1),('sibling','ao16','p1-1','thread-3','ao16:root',2,?,'sibling:provider',1)`, ao16Stamp, ao16Stamp)
	for _, turn := range []struct{ id, provider, state, branch, err string }{
		{"settled", "native-turn-1", "completed", "ao16:root", ""},
		{"failed", "native-error", "failed", "child", "provider failed"},
		{"interrupted", "native-cancel", "interrupted", "sibling", ""},
		{"imported", "native-import", "recovered", "child", ""},
	} {
		ao16Exec(t, db, `INSERT INTO conversation_turns(id,conversation_id,handled_by_session_id,provider_turn_id,controller_generation,state,error_message,requested_at,completed_at,branch_id) VALUES (?,'ao16','p1-1',?,'generation-1',?,?,?,?,?)`, turn.id, turn.provider, turn.state, turn.err, ao16Stamp, ao16Stamp, turn.branch)
	}
	ao16Exec(t, db, `INSERT INTO conversation_messages(id,conversation_id,turn_id,sequence,role,origin,text,provider_item_id,created_at,updated_at,branch_id) VALUES ('user','ao16','settled',1,'user','human','question','native-user',?,?,'ao16:root'),('answer','ao16','settled',2,'assistant','provider','answer','native-answer',?,?,'ao16:root')`, ao16Stamp, ao16Stamp, ao16Stamp, ao16Stamp)
	ao16Exec(t, db, `UPDATE conversations SET latest_sequence=2 WHERE id='ao16'`)
	ao16Exec(t, db, `INSERT INTO review(id,session_id,project_id,harness,created_at,updated_at) VALUES ('review','p1-1','p1','codex',?,?)`, ao16Stamp, ao16Stamp)
	ao16Exec(t, db, `INSERT INTO review_run(id,review_id,session_id,harness,target_sha,status,created_at) VALUES ('pending-review','review','p1-1','codex',?,'running',?)`, ao16Revision, ao16Stamp)
	// Explicit non-contiguous IDs. Anonymous duplicates remain distinct; an
	// identified reconnect duplicate is ignored by the existing SQLite rule.
	for _, event := range []struct {
		id                                int
		provider, method, payload, branch string
	}{
		{2, "identified", "turn/completed", `{"state":"completed"}`, "ao16:root"},
		{5, "", "delta", `{ "text": "same\nbytes" }`, "ao16:root"},
		{9, "", "delta", `{ "text": "same\nbytes" }`, "ao16:root"},
		{12, "import:event", "error", `{"error":"provider failed","scope":"import:provider"}`, "child"},
		{20, "sibling:event", "turn/interrupted", `{"state":"interrupted"}`, "sibling"},
		{21, "identified", "turn/completed", `{"must":"not replace original"}`, "child"},
	} {
		received := ao16Stamp
		if event.id == 20 {
			received = "2026-08-05 10:00:00.123456789+00:00"
		}
		ao16Exec(t, db, `INSERT OR IGNORE INTO conversation_provider_events(id,conversation_id,session_id,provider_event_id,method,payload_json,received_at,branch_id) VALUES (?,'ao16','p1-1',?,?,?,?,?)`, event.id, event.provider, event.method, event.payload, received, event.branch)
	}
	return st, db
}

func ao16Quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func ao16ReadTable(db *sql.DB, name string) (ao16Table, error) {
	table := ao16Table{Name: name}
	rows, err := db.Query("SELECT * FROM " + ao16Quote(name) + " LIMIT 0")
	if err != nil {
		return table, err
	}
	table.Columns, err = rows.Columns()
	if err != nil {
		_ = rows.Close()
		return table, err
	}
	if err := rows.Close(); err != nil {
		return table, err
	}
	// CAST bypasses the driver's TIMESTAMP decoding: preserve lexical timestamp
	// bytes, offsets and fractional precision rather than normalizing time.Time.
	columns := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		columns[i] = "CAST(" + ao16Quote(column) + " AS TEXT)"
	}
	order := ao16Quote(table.Columns[0])
	rows, err = db.Query("SELECT " + strings.Join(columns, ",") + " FROM " + ao16Quote(name) + " ORDER BY " + order)
	if err != nil {
		return table, err
	}
	defer rows.Close()
	for rows.Next() {
		values := make([]any, len(table.Columns))
		ptr := make([]any, len(values))
		for i := range ptr {
			ptr[i] = &values[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			return table, err
		}
		record := make([]*string, len(values))
		for i, v := range values {
			if v == nil {
				continue
			}
			s := fmt.Sprint(v)
			record[i] = &s
		}
		table.Rows = append(table.Rows, record)
	}
	return table, rows.Err()
}
func ao16Seal(body ao16Body) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return json.Marshal(ao16Envelope{Body: raw, SHA256: hex.EncodeToString(sum[:])})
}
func ao16Archive(t *testing.T, db *sql.DB) []byte {
	t.Helper()
	body := ao16Body{Version: 1, Revision: ao16Revision, EventIDs: []int64{2, 5, 9, 12, 20}}
	ao16Must(t, db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='conversation_provider_events'`).Scan(&body.EventHighWater))
	for _, name := range ao16Tables {
		table, err := ao16ReadTable(db, name)
		ao16Must(t, err)
		body.Tables = append(body.Tables, table)
	}
	data, err := ao16Seal(body)
	ao16Must(t, err)
	if len(data) > 1<<20 {
		t.Fatal("archive exceeds 1 MiB")
	}
	return data
}
func ao16Decode(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing archive content: %v", err)
	}
	return nil
}

// Receives bytes and the independent destination only. Source objects and
// retained projection pointers are never part of this interface.
func ao16Restore(db *sql.DB, data []byte) error {
	if len(data) > 1<<20 {
		return fmt.Errorf("archive too large")
	}
	var envelope ao16Envelope
	if err := ao16Decode(data, &envelope); err != nil {
		return err
	}
	sum := sha256.Sum256(envelope.Body)
	if envelope.SHA256 != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("SHA-256 mismatch")
	}
	var body ao16Body
	if err := ao16Decode(envelope.Body, &body); err != nil {
		return err
	}
	if body.Version != 1 || body.Revision != ao16Revision || len(body.Tables) != len(ao16Tables) {
		return fmt.Errorf("archive identity mismatch")
	}
	for i, table := range body.Tables {
		if table.Name != ao16Tables[i] {
			return fmt.Errorf("table order mismatch")
		}
		current, err := ao16ReadTable(db, table.Name)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(table.Columns, current.Columns) {
			return fmt.Errorf("schema mismatch: %s", table.Name)
		}
		if table.Name == "conversation_provider_events" {
			if !reflect.DeepEqual(table.Columns, ao16EventColumns) || len(table.Rows) != len(body.EventIDs) || len(table.Rows) == 0 {
				return fmt.Errorf("event coverage mismatch")
			}
			var previous int64
			for j, row := range table.Rows {
				if len(row) != 8 || row[0] == nil {
					return fmt.Errorf("event shape mismatch")
				}
				id, err := strconv.ParseInt(*row[0], 10, 64)
				if err != nil || id <= previous || id != body.EventIDs[j] {
					return fmt.Errorf("event membership/order mismatch")
				}
				previous = id
			}
			if body.EventHighWater < previous {
				return fmt.Errorf("event sequence high-water mismatch")
			}
		}
		for _, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return fmt.Errorf("row shape mismatch")
			}
		}
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Native SQLite FK enforcement owns relational integrity, including lineage.
	if _, err = tx.Exec("PRAGMA defer_foreign_keys=ON"); err != nil {
		return err
	}
	for _, table := range body.Tables {
		names := make([]string, len(table.Columns))
		marks := make([]string, len(names))
		for i, name := range table.Columns {
			names[i] = ao16Quote(name)
			marks[i] = "?"
		}
		query := "INSERT INTO " + ao16Quote(table.Name) + " (" + strings.Join(names, ",") + ") VALUES (" + strings.Join(marks, ",") + ")"
		for _, row := range table.Rows {
			args := make([]any, len(row))
			for i, v := range row {
				if v != nil {
					args[i] = *v
				}
			}
			if _, err = tx.Exec(query, args...); err != nil {
				return err
			}
		}
	}
	// Branch IDs are not SQL foreign keys on raw events: validate the boundary
	// actually consumed by the archive, without inventing provider authority.
	var invalid int
	err = tx.QueryRow(`SELECT count(*) FROM conversation_provider_events e LEFT JOIN conversation_branches b ON b.id=e.branch_id WHERE b.id IS NULL OR b.session_id IS NULL OR b.conversation_id<>e.conversation_id OR b.session_id<>e.session_id`).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("event ownership mismatch")
	}
	// Ignored identified duplicates may advance AUTOINCREMENT without adding
	// rows. Keep that gap so a later event cannot reuse a prior cursor value.
	if _, err = tx.Exec(`UPDATE sqlite_sequence SET seq=? WHERE name='conversation_provider_events'`, body.EventHighWater); err != nil {
		return err
	}
	return tx.Commit()
}
