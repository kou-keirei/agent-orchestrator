package store_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// This is a transport experiment on tiny migrated scratch databases, not an
// archive format or retention writer. No production entry point deletes rows.
// Cells use raw bytes (including the stored timestamp representation), avoiding
// time.Time normalization and JSON payload reserialization. IDs are explicit.
type retentionArchiveRow struct {
	ID    int64     `json:"id"`
	Cells [7][]byte `json:"cells"`
}

type retentionArchiveManifest struct {
	Version           string   `json:"version"`
	Columns           []string `json:"columns"`
	Count             int      `json:"count"`
	FirstID           int64    `json:"first_id"`
	LastID            int64    `json:"last_id"`
	RowSHA256         []string `json:"row_sha256"`
	ArchiveSHA256     string   `json:"archive_sha256"`
	DeletionAuthority string   `json:"deletion_authority"`
}

func retentionRows(t *testing.T, db *sql.DB) []retentionArchiveRow {
	t.Helper()
	rows, err := db.Query(`SELECT id, CAST(conversation_id AS BLOB), CAST(session_id AS BLOB),
 CAST(provider_event_id AS BLOB), CAST(method AS BLOB), CAST(payload_json AS BLOB),
 CAST(received_at AS BLOB), CAST(branch_id AS BLOB) FROM conversation_provider_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []retentionArchiveRow
	for rows.Next() {
		var r retentionArchiveRow
		if err := rows.Scan(&r.ID, &r.Cells[0], &r.Cells[1], &r.Cells[2], &r.Cells[3], &r.Cells[4], &r.Cells[5], &r.Cells[6]); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func retentionJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func retentionHash(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

func TestRetentionArchiveFixtureRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	seedProject(t, s, "retention")
	rec := sampleRecord("retention")
	rec.Mode = domain.SessionModeChat
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "retention-conv", domain.ConversationScopeSession, "retention", session.ID, histClock)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimChatControllerGeneration(ctx, session.ID, "retention-gen"); err != nil {
		t.Fatal(err)
	}

	// The callback models durable projection, including a pending reviewer hold.
	project := func(txCtx context.Context) error {
		return s.UpsertActivity(txCtx, conv.ID, "", domain.ConversationActivity{
			ID: "pending-review", Kind: domain.ActivityKindApproval,
			Status: domain.ActivityStatusPending, Summary: "review required", ProviderItemID: "review-item",
		}, histClock)
	}
	payload := " {\"providerTurnId\":\"turn\", \"itemId\":\"item\", \"delta\":\"λ\\n\\u0000\"} \n"
	for _, event := range []struct{ id, method string }{
		{"", "command.output.delta"}, {"", "message.delta"}, {"", "message.delta"},
		{"identified", "message.delta"}, {"identified", "message.delta"},
	} {
		if _, err := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "retention-gen", event.id, event.method, payload, histClock, project); err != nil {
			t.Fatal(err)
		}
	}
	beforeProjection, err := s.LoadConversationSnapshot(ctx, conv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeProjection.Activities) != 1 || beforeProjection.Activities[0].Status != domain.ActivityStatusPending {
		t.Fatal("pending review fixture missing")
	}
	// Opening only the t.TempDir database; never resolve AO_DATA_DIR or ~/.ao.
	openRaw := func() *sql.DB {
		db, err := sql.Open("sqlite", filepath.ToSlash(filepath.Join(dir, "ao.db")))
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := openRaw()
	defer func() { _ = db.Close() }()
	before := retentionRows(t, db)
	if len(before) != 4 {
		t.Fatalf("anonymous replay must remain duplicated and identified replay deduped: %d", len(before))
	}
	for _, r := range before {
		p := store.PreviewRetentionEvent(gen.ConversationProviderEvent{ID: r.ID, Method: string(r.Cells[3]), ProviderEventID: string(r.Cells[2])})
		if p.Class != "UNKNOWN_REVIEW_REQUIRED" || p.ReviewerObligation != store.RetentionUnknown || p.NativeAcknowledged != store.RetentionUnknown || p.DeletionAuthority != "NONE" {
			t.Fatalf("review/replay protection lost: %+v", p)
		}
	}

	archive := retentionJSON(t, before)
	manifest := retentionArchiveManifest{
		Version: "ao16.fixture-only.v1", Columns: []string{"id", "conversation_id", "session_id", "provider_event_id", "method", "payload_json", "received_at", "branch_id"},
		Count: len(before), FirstID: before[0].ID, LastID: before[len(before)-1].ID,
		ArchiveSHA256: retentionHash(archive), DeletionAuthority: "NONE",
	}
	for _, r := range before {
		manifest.RowSHA256 = append(manifest.RowSHA256, retentionHash(retentionJSON(t, r)))
	}
	archivePath := filepath.Join(dir, "events.fixture.json")
	manifestPath := filepath.Join(dir, "manifest.fixture.json")
	if err := os.WriteFile(archivePath, archive, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, retentionJSON(t, manifest), 0600); err != nil {
		t.Fatal(err)
	}

	// Simulated interruption before commit: rollback and reopen, not a claim of
	// OS power-loss qualification. SQLite owns transaction/recovery semantics.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM conversation_provider_events`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db = openRaw()
	if !reflect.DeepEqual(before, retentionRows(t, db)) {
		t.Fatal("interrupted transaction changed archive")
	}

	// Restore a serialized archive in a single transaction, using explicit IDs
	// and TEXT cells. Test-only removal is confined to this scratch fixture.
	stored, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	storedManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var receipt retentionArchiveManifest
	if err := json.Unmarshal(storedManifest, &receipt); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(receipt, manifest) || retentionHash(stored) != receipt.ArchiveSHA256 {
		t.Fatal("archive/manifest mismatch")
	}
	var restored []retentionArchiveRow
	if err := json.Unmarshal(stored, &restored); err != nil {
		t.Fatal(err)
	}
	if len(restored) != receipt.Count {
		t.Fatal("archive count mismatch")
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM conversation_provider_events`); err != nil {
		t.Fatal(err)
	}
	for i, r := range restored {
		if retentionHash(retentionJSON(t, r)) != receipt.RowSHA256[i] {
			t.Fatal("row hash mismatch")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO conversation_provider_events
 (id, conversation_id, session_id, provider_event_id, method, payload_json, received_at, branch_id)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, r.ID, string(r.Cells[0]), string(r.Cells[1]), string(r.Cells[2]), string(r.Cells[3]), string(r.Cells[4]), string(r.Cells[5]), string(r.Cells[6])); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, retentionRows(t, db)) {
		t.Fatal("restore changed raw bytes, IDs or order")
	}
	afterProjection, err := s.LoadConversationSnapshot(ctx, conv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeProjection, afterProjection) {
		t.Fatal("transport changed projection or pending review")
	}
	// Restart replay still dedupes identified events after restore.
	applied, err := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "retention-gen", "identified", "message.delta", payload, histClock, func(context.Context) error { return errors.New("duplicate reprojected") })
	if err != nil || applied {
		t.Fatalf("identified replay after restore: %v %v", applied, err)
	}
	t.Logf("fixture-only manifest: %s", retentionJSON(t, receipt))
}

func TestRetentionPreviewNativeImportAndForkRemainUnknown(t *testing.T) {
	ctx := context.Background()
	s, session, conv := seededChatConversation(t)
	seedBranchTurns(t, s, session, conv)
	child := domain.ConversationBranch{ID: "retention-child", ConversationID: conv.ID, SessionID: session.ID,
		ProviderConversationID: "native-child", ParentBranchID: conv.ActiveBranchID, ReplacedTurnID: "turn-2", ForkAfterSequence: 2}
	if err := s.CreateConversationBranch(ctx, child, histClock); err != nil {
		t.Fatal(err)
	}
	activateTestBranch(t, s, session, conv, child.ID, child.ProviderConversationID, "child-gen")
	// Import a previously unseen native turn on the child, then repeat the
	// import to exercise the real store's history idempotency boundary.
	if err := s.AdoptProviderTurn(ctx, conv.ID, session.ID, "child-gen", "imported-turn", "native-turn", histClock); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.AppendImportedUserMessage(ctx, conv.ID, "native-turn", domain.ConversationMessage{ID: "imported", Text: "native prompt"}, histClock); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := s.LoadConversationSnapshot(ctx, conv.ID)
	if err != nil {
		t.Fatal(err)
	}
	imports := 0
	for _, message := range snapshot.Messages {
		if message.ID == "imported" && message.Text == "native prompt" {
			imports++
		}
	}
	if imports != 1 {
		t.Fatalf("native prompt imported %d times, want once", imports)
	}
	for _, method := range []string{"message.delta", "command.output.delta"} {
		if _, err := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "child-gen", "", method, `{"providerTurnId":"native-turn","itemId":"item","completed":true,"ack":true}`, histClock, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	events, err := s.ProviderEventsSince(ctx, conv.ID, 0, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("child replay: %d %v", len(events), err)
	}
	for _, event := range events {
		p := store.PreviewRetentionEvent(gen.ConversationProviderEvent{ID: event.ID, ConversationID: conv.ID, BranchID: child.ID, Method: event.Method, PayloadJson: event.PayloadJson})
		if p.Class != "UNKNOWN_REVIEW_REQUIRED" || p.StopReason != "SOURCE_GAP" || p.CoveringCheckpoint != store.RetentionUnknown || p.BranchReplayNeeded != store.RetentionUnknown || p.NativeAcknowledged != store.RetentionUnknown || p.DeletionAuthority != "NONE" {
			t.Fatalf("import/fork invented eligibility: %+v", p)
		}
	}
}
