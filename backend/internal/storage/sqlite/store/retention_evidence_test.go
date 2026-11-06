package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func evidenceOptions() store.RetentionCensusOptions {
	o := censusOptions()
	o.MaxReportBytes = 1 << 20
	return o
}

func beginEvidence(t *testing.T, s *sqlite.Store, o store.RetentionCensusOptions) *store.RetentionEvidence {
	t.Helper()
	e, err := s.BeginRetentionEvidence(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func evidencePage(t *testing.T, e *store.RetentionEvidence, cursor *store.RetentionCensusCursor) store.RetentionEvidencePage {
	t.Helper()
	p, err := e.Page(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if p.Version != "ao16.retention-evidence.v1" || p.SnapshotBinding != "SQLITE_READ_TRANSACTION" ||
		p.SnapshotID == "" || p.StopReason != "SOURCE_GAP" || p.CoverageComplete ||
		p.DeletionAuthority != "NONE" || p.FilesystemReclaimEstimate != "UNKNOWN" {
		t.Fatalf("unsafe evidence contract: %+v", p)
	}
	for _, item := range p.Items {
		if (item.Class != "KEEP" && item.Class != "UNKNOWN_REVIEW_REQUIRED") || item.EventNativeTurnItemBinding != store.RetentionUnknown {
			t.Fatalf("invented eligibility/native identity: %+v", item)
		}
		for _, reason := range []store.RetentionEvidenceReason{store.RetentionEvidenceACK, store.RetentionEvidenceTurn,
			store.RetentionEvidenceCheckpoint, store.RetentionEvidenceReview, store.RetentionEvidenceBranchReplay,
			store.RetentionEvidenceProjection, store.RetentionEvidenceRecovery} {
			if !slices.Contains(item.Reasons, reason) {
				t.Fatalf("lost independent obligation %s: %+v", reason, item)
			}
		}
	}
	return p
}

func evidenceTurn(t *testing.T, f providerBranchFixture, id, branch, state string) {
	t.Helper()
	archiveExec(t, f.db, `INSERT INTO conversation_turns
 (id, conversation_id, handled_by_session_id, branch_id, provider_turn_id, state, requested_at, completed_at)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, id, f.conversation, f.session, branch, "native-"+id, state, testNow, testNow)
}

func TestRetentionEvidenceMixedProvidersEditsAndInterruptedResume(t *testing.T) {
	f := newProviderBranchFixture(t)
	root := f.conversation + ":root"
	archiveExec(t, f.db, `UPDATE conversation_branches SET provider_conversation_id='claude-native', provider_scope_id='scope-A' WHERE id=?`, root)
	evidenceTurn(t, f, "original", root, "completed")
	if err := f.s.CreateConversationBranch(context.Background(), domain.ConversationBranch{
		ID: "edited", ConversationID: f.conversation, ParentBranchID: root, ProviderConversationID: "edited-native",
	}, testNow); err != nil {
		t.Fatal(err)
	}
	evidenceTurn(t, f, "replacement", "edited", "interrupted")
	archiveExec(t, f.db, `UPDATE conversation_branches SET replaced_turn_id='original', replacement_turn_id='replacement',
 strategy='replay', replay_truncated=1, replay_cutoff_sequence=100, provider_ids_scoped=1 WHERE id='edited'`)
	// Edited projected prose is not proof that earlier deltas are reconstructible.
	if err := f.s.AppendAssistantDelta(context.Background(), f.conversation, "item", "original", "old", "answer", testNow); err != nil {
		t.Fatal(err)
	}
	archiveExec(t, f.db, `UPDATE conversation_messages SET text='edited answer', revision=revision+1 WHERE id='answer'`)
	// Even a terminal-looking checkpoint and claimed ACK have no event window.
	archiveExec(t, f.db, `UPDATE sessions SET conversation_checkpoint_state='complete',
 conversation_checkpoint_native_id='claude-native', conversation_checkpoint_turn_id='native-original',
 native_checkpoint_evidence='{"ack":true,"independentReview":true}' WHERE id=?`, f.session)
	claim := `{"turnId":"original","ack":true,"checkpointComplete":true,"branchReplayComplete":true}`
	insertCensusEvent(t, f, root, "", "message.delta", claim)
	insertCensusEvent(t, f, "edited", "", "message.delta", claim)
	for _, method := range []string{"reasoning.delta", "command.output.delta", "error", "turn.completed"} {
		insertCensusEvent(t, f, root, "", method, claim)
	}
	insertCensusEvent(t, f, root, "shared-event-id", "message.delta", claim)
	// Repeated native ownership is a separate stored epoch, not a turn mapping.
	archiveExec(t, f.db, `INSERT INTO conversation_branches
 (id, conversation_id, session_id, provider_conversation_id, created_at) VALUES ('return-epoch', ?, ?, 'claude-native', ?)`, f.conversation, f.session, testNow)
	insertCensusEvent(t, f, "return-epoch", "", "message.delta", claim)
	rec := sampleRecord("archive")
	rec.Harness = domain.HarnessCodex
	otherSession, err := f.s.CreateSession(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	other, err := f.s.CreateConversation(context.Background(), "codex-conversation", domain.ConversationScopeSession, "archive", otherSession.ID, testNow)
	if err != nil {
		t.Fatal(err)
	}
	g := f
	g.conversation, g.session = other.ID, otherSession.ID
	archiveExec(t, f.db, `UPDATE conversation_branches SET provider_conversation_id='codex-native' WHERE id=?`, other.ID+":root")
	insertCensusEvent(t, g, other.ID+":root", "shared-event-id", "message.delta", claim)
	before := retentionRows(t, f.db)
	e := beginEvidence(t, f.s, evidenceOptions())
	var items []store.RetentionEvidenceItem
	var cursor *store.RetentionCensusCursor
	for {
		p := evidencePage(t, e, cursor)
		items = append(items, p.Items...)
		if p.Next == nil {
			if !p.WindowComplete || p.TotalScannedRows != 9 {
				t.Fatalf("incomplete mapping: %+v", p)
			}
			break
		}
		cursor = p.Next
	}
	for _, item := range items {
		if item.ConversationBinding != store.RetentionYes || item.SessionBinding != store.RetentionYes || item.BranchBinding != store.RetentionYes {
			t.Fatalf("lost stored associations: %+v", item)
		}
	}
	if items[0].Class != "UNKNOWN_REVIEW_REQUIRED" || items[0].ObservedCheckpointState != "complete" ||
		items[0].ObservedCheckpointTurnID != "native-original" || items[0].ObservedBranchNativeID != "claude-native" ||
		items[1].Class != "KEEP" || items[1].ObservedParentBranchID != root || items[1].ObservedReplacedTurnID != "original" ||
		items[1].ObservedReplacementTurnID != "replacement" || items[1].ObservedReplacementState != "interrupted" ||
		items[1].ObservedReplayStrategy != "replay" || !slices.Contains(items[1].Reasons, store.RetentionEvidenceEdited) ||
		items[7].BranchID != "return-epoch" || items[7].Class != "UNKNOWN_REVIEW_REQUIRED" ||
		items[8].ConversationID != other.ID || items[8].SessionID != string(otherSession.ID) || items[8].ObservedBranchNativeID != "codex-native" {
		t.Fatalf("misattributed native/edit evidence: %+v", items)
	}
	for _, i := range []int{2, 3, 4, 5, 6, 8} {
		if items[i].Class != "KEEP" {
			t.Fatalf("protected evidence lost: %+v", items[i])
		}
	}
	if !reflect.DeepEqual(before, retentionRows(t, f.db)) {
		t.Fatal("evidence read changed archive")
	}
}

func TestRetentionEvidenceSnapshotIncludesAssociationsAndCheckpoint(t *testing.T) {
	f := newProviderBranchFixture(t)
	root := f.conversation + ":root"
	archiveExec(t, f.db, `UPDATE conversation_branches SET provider_conversation_id='before' WHERE id=?`, root)
	for range 3 {
		insertCensusEvent(t, f, root, "", "message.delta", "old")
	}
	o := evidenceOptions()
	o.PageRows = 1
	e := beginEvidence(t, f.s, o)
	// Begin pins the native snapshot before even the first semantic page.
	done := make(chan error, 1)
	go func() {
		tx, err := f.db.Begin()
		if err == nil {
			defer tx.Rollback()
			_, err = tx.Exec(`UPDATE conversation_branches SET provider_conversation_id='after' WHERE id=?`, root)
			if err == nil {
				_, err = tx.Exec(`UPDATE sessions SET conversation_checkpoint_state='prompt', conversation_checkpoint_unsettled=1 WHERE id=?`, f.session)
			}
			if err == nil {
				_, err = tx.Exec(`UPDATE conversation_provider_events SET method='error', payload_json='new' WHERE id=2`)
			}
			if err == nil {
				_, err = tx.Exec(legacyProviderEventInsert, f.conversation, f.session, "new", "error", "new", testNow)
			}
			if err == nil {
				err = tx.Commit()
			}
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("evidence snapshot blocked WAL writer")
	}
	var cursor *store.RetentionCensusCursor
	for range 3 {
		p := evidencePage(t, e, cursor)
		item := p.Items[0]
		if p.HighWaterID != 3 || item.Method != "message.delta" || item.ObservedBranchNativeID != "before" ||
			item.ObservedCheckpointState != "empty" || item.Class != "UNKNOWN_REVIEW_REQUIRED" || *item.LogicalPayloadBytes != 3 {
			t.Fatalf("cross-snapshot evidence: %+v", p)
		}
		cursor = p.Next
	}
	o.PageRows = 10
	p := evidencePage(t, beginEvidence(t, f.s, o), nil)
	if len(p.Items) != 4 || p.Items[0].ObservedBranchNativeID != "after" || p.Items[0].ObservedCheckpointState != "prompt" ||
		!slices.Contains(p.Items[0].Reasons, store.RetentionEvidenceUnsettled) || p.Items[1].Method != "error" {
		t.Fatalf("fresh snapshot missed writer: %+v", p)
	}
}

func TestRetentionEvidenceBoundedTraversalCursorAndBytes(t *testing.T) {
	f := newProviderBranchFixture(t)
	tx, err := f.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := range 1007 {
		archiveExec(t, tx, legacyProviderEventInsert, f.conversation, f.session, "", "message.delta", fmt.Sprintf("é\x00%04d", i), testNow)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	o := evidenceOptions()
	o.PageRows, o.MaxPages = 101, 10
	e := beginEvidence(t, f.s, o)
	p := evidencePage(t, e, nil)
	if len(p.Items) != 101 || p.Items[0].LogicalPayloadBytes == nil || *p.Items[0].LogicalPayloadBytes != 7 {
		t.Fatalf("wrong logical bytes/bounds: %+v", p)
	}
	other := beginEvidence(t, f.s, o)
	if _, err := other.Page(context.Background(), p.Next); !errors.Is(err, store.ErrRetentionCensusCursor) {
		t.Fatalf("foreign cursor accepted: %v", err)
	}
	stale := *p.Next
	for p.Next != nil {
		p = evidencePage(t, e, p.Next)
		if len(p.Items) > 101 {
			t.Fatal("page row bound exceeded")
		}
		if p.Page == 2 {
			if _, err := e.Page(context.Background(), &stale); !errors.Is(err, store.ErrRetentionCensusCursor) {
				t.Fatalf("stale cursor accepted: %v", err)
			}
		}
	}
	if p.TotalScannedRows != 1007 || !p.WindowComplete || p.Page != 10 {
		t.Fatalf("bounded traversal lost events: %+v", p)
	}
	o.MaxPages = 1
	p = evidencePage(t, beginEvidence(t, f.s, o), nil)
	if p.WindowComplete || p.Next != nil || p.TraversalStopReason != "PAGE_LIMIT" {
		t.Fatalf("page exhaustion became coverage: %+v", p)
	}
	ceiling := int64(2)
	o.HighWaterID = &ceiling
	p = evidencePage(t, beginEvidence(t, f.s, o), nil)
	if len(p.Items) != 2 || !p.WindowComplete || p.SourceMaxID != 1007 || p.HighWaterID != 2 {
		t.Fatalf("wrong explicit event window: %+v", p)
	}
	o.HighWaterID, o.MaxReportBytes = nil, 8192
	e = beginEvidence(t, f.s, o)
	if p, err := e.Page(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "byte limit") || p.SnapshotID != "" || len(p.Items) != 0 {
		t.Fatalf("oversized report returned partial evidence: %+v %v", p, err)
	}
	if _, err := e.Page(context.Background(), nil); !errors.Is(err, store.ErrRetentionCensusClosed) {
		t.Fatalf("oversized snapshot reused: %v", err)
	}
}

func TestRetentionEvidenceMalformedLinksAndNoPayloadTransfer(t *testing.T) {
	f := newProviderBranchFixture(t)
	large := strings.Repeat("secret-payload", 1<<17)
	insertCensusEvent(t, f, "missing", "", "message.delta", large)
	insertCensusEvent(t, f, "missing", "", "message.delta", []byte{0, 255})
	insertCensusEvent(t, f, "missing", "", "message.delta", "x")
	insertCensusEvent(t, f, "foreign-branch", "", "message.delta", "x")
	// Existing but cross-conversation branch references must not supply native IDs.
	archiveExec(t, f.db, `INSERT INTO conversations (id, scope, project_id, created_at, updated_at)
 VALUES ('foreign', 'project', 'archive', ?, ?)`, testNow, testNow)
	archiveExec(t, f.db, `INSERT INTO conversation_branches
 (id, conversation_id, provider_conversation_id, created_at) VALUES ('foreign-branch', 'foreign', 'foreign-native', ?)`, testNow)
	// Corrupt only this disposable fixture: dangling links must not lose rows.
	archiveExec(t, f.db, `PRAGMA foreign_keys=OFF`)
	archiveExec(t, f.db, `UPDATE conversation_provider_events SET conversation_id='dangling', session_id='missing' WHERE id=2`)
	archiveExec(t, f.db, `UPDATE conversation_provider_events SET branch_id=?, method=? WHERE id=3`, large, string([]byte{0xff}))
	o := evidenceOptions()
	o.PageRows = 10
	p := evidencePage(t, beginEvidence(t, f.s, o), nil)
	if len(p.Items) != 4 || p.Items[0].ConversationBinding != store.RetentionYes || p.Items[0].BranchBinding != store.RetentionUnknown ||
		p.Items[1].ConversationBinding != store.RetentionUnknown || p.Items[1].SessionBinding != store.RetentionUnknown ||
		*p.Items[0].LogicalPayloadBytes != int64(len(large)) || *p.Items[1].LogicalPayloadBytes != 2 ||
		p.Items[2].BranchID != "" || p.Items[2].Method != "" || !slices.Contains(p.Items[2].Reasons, store.RetentionEvidenceMetadata) ||
		p.Items[3].BranchBinding != store.RetentionUnknown || p.Items[3].ObservedBranchNativeID != "" {
		t.Fatalf("malformed identity gained certainty/lost bytes: %+v", p)
	}
	for _, item := range p.Items {
		if !slices.Contains(item.Reasons, store.RetentionEvidenceIdentity) {
			t.Fatalf("invalid link unreported: %+v", item)
		}
	}
	b, err := json.Marshal(p)
	if err != nil || strings.Contains(string(b), "secret-payload") || len(b) > o.MaxReportBytes {
		t.Fatal("payload/unbounded metadata leaked")
	}
}

func TestRetentionEvidenceReadOnlyEmptyBoundsAndCancellation(t *testing.T) {
	f := newProviderBranchFixture(t)
	ro, err := sqlite.OpenReadOnly(context.Background(), strings.TrimSuffix(f.path, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ro.Close() })
	p := evidencePage(t, beginEvidence(t, ro, evidenceOptions()), nil)
	if len(p.Items) != 0 || !p.WindowComplete {
		t.Fatalf("empty window: %+v", p)
	}
	insertCensusEvent(t, f, f.conversation+":root", "", "message.delta", "read-only fixture")
	p = evidencePage(t, beginEvidence(t, ro, evidenceOptions()), nil)
	if len(p.Items) != 1 || p.Items[0].BranchBinding != store.RetentionYes || *p.Items[0].LogicalPayloadBytes != 17 {
		t.Fatalf("read-only association mapping: %+v", p)
	}
	fresh := evidencePage(t, beginEvidence(t, ro, evidenceOptions()), nil)
	if fresh.SnapshotID == p.SnapshotID || !reflect.DeepEqual(fresh.Items, p.Items) {
		t.Fatal("reused snapshot identity or nondeterministic semantic mapping")
	}
	o := evidenceOptions()
	o.PageRows = 0
	if _, err := ro.BeginRetentionEvidence(context.Background(), o); err == nil {
		t.Fatal("accepted unbounded options")
	}
	for _, parent := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		beginCtx, pageCtx := context.Background(), context.Background()
		if parent {
			beginCtx = ctx
		} else {
			pageCtx = ctx
		}
		e, err := ro.BeginRetentionEvidence(beginCtx, evidenceOptions())
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if p, err := e.Page(pageCtx, nil); !errors.Is(err, context.Canceled) || p.SnapshotID != "" {
			t.Fatalf("cancellation returned evidence: %+v %v", p, err)
		}
		if _, err := e.Page(context.Background(), nil); !errors.Is(err, store.ErrRetentionCensusClosed) {
			t.Fatalf("canceled snapshot reused: %v", err)
		}
	}
	o = evidenceOptions()
	o.MaxDuration = 25 * time.Millisecond
	e := beginEvidence(t, ro, o)
	time.Sleep(50 * time.Millisecond)
	if _, err := e.Page(context.Background(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline ignored: %v", err)
	}
}

// Explicit IDs exercise the inclusive first-page bound and strict subsequent
// cursor at both int64 extremes without overflow or skipped/duplicated rows.
func TestRetentionEvidenceStaticQueryCursorParametersAndNullLineage(t *testing.T) {
	f := newProviderBranchFixture(t)
	root := f.conversation + ":root"
	archiveExec(t, f.db, `UPDATE conversation_branches SET parent_branch_id=NULL,
 replaced_turn_id=NULL, replacement_turn_id=NULL WHERE id=?`, root)
	ids := []int64{math.MinInt64, -1, 0, 5, math.MaxInt64}
	for _, id := range ids {
		archiveExec(t, f.db, `INSERT INTO conversation_provider_events
 (id, conversation_id, session_id, branch_id, provider_event_id, method, payload_json, received_at)
 VALUES (?, ?, ?, ?, '', 'message.delta', ?, ?)`, id, f.conversation, f.session, root, "é\x00payload", testNow)
	}
	for _, tc := range []struct {
		name    string
		ceiling int64
		want    []int64
	}{
		{"full", math.MaxInt64, ids},
		{"zero ceiling", 0, ids[:3]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := evidenceOptions()
			o.PageRows, o.HighWaterID = 1, &tc.ceiling
			e := beginEvidence(t, f.s, o)
			var cursor *store.RetentionCensusCursor
			for i, want := range tc.want {
				p := evidencePage(t, e, cursor)
				if len(p.Items) != 1 || p.Items[0].EventID != want || p.Page != i+1 ||
					p.TotalScannedRows != int64(i+1) || p.HighWaterID != tc.ceiling || p.SourceMaxID != math.MaxInt64 {
					t.Fatalf("wrong cursor parameters/page: %+v", p)
				}
				item := p.Items[0]
				if item.ObservedParentBranchID != "" || item.ObservedReplacedTurnID != "" ||
					item.ObservedReplacementTurnID != "" || item.ObservedReplacementState != "" ||
					item.BranchBinding != store.RetentionYes || item.Class != "UNKNOWN_REVIEW_REQUIRED" ||
					slices.Contains(item.Reasons, store.RetentionEvidenceMetadata) ||
					slices.Contains(item.Reasons, store.RetentionEvidenceIdentity) ||
					item.LogicalPayloadBytes == nil || *item.LogicalPayloadBytes != int64(len("é\x00payload")) {
					t.Fatalf("null lineage changed bounded scalar mapping: %+v", item)
				}
				last := i == len(tc.want)-1
				if p.WindowComplete != last || (p.Next == nil) != last {
					t.Fatalf("wrong traversal boundary: %+v", p)
				}
				if !last && p.Next.AfterID != want {
					t.Fatalf("wrong bound after ID: %+v", p.Next)
				}
				cursor = p.Next
			}
		})
	}
}
