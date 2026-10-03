package store_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

func TestInsertReviewRunDuplicatePRSHAMapsToSentinel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerClaudeCode, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	run := domain.ReviewRun{
		ID: "run-1", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode,
		PRURL: "https://example/pr/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning, Verdict: domain.VerdictNone, CreatedAt: now,
	}
	if err := s.InsertReviewRun(ctx, run); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// A second run for the same (session_id, pr_url, target_sha, harness) hits the
	// partial unique index (migration 0041) and must surface as the sentinel so
	// the engine can fall back to the existing run.
	dup := run
	dup.ID = "run-2"
	if err := s.InsertReviewRun(ctx, dup); !errors.Is(err, domain.ErrDuplicateReviewRun) {
		t.Fatalf("duplicate insert err = %v, want ErrDuplicateReviewRun", err)
	}

	otherPR := run
	otherPR.ID = "run-other-pr"
	otherPR.PRURL = "https://example/pr/2"
	if err := s.InsertReviewRun(ctx, otherPR); err != nil {
		t.Fatalf("same sha on different PR should insert: %v", err)
	}

	if ok, err := s.UpdateReviewRunResult(ctx, "run-1", domain.ReviewRunFailed, domain.VerdictNone, "claude: not found", "", true); err != nil {
		t.Fatalf("mark failed: %v", err)
	} else if !ok {
		t.Fatal("mark failed: got ok=false")
	}
	if err := s.InsertReviewRun(ctx, dup); err != nil {
		t.Fatalf("retry after failed insert: %v", err)
	}

	// An empty target_sha is excluded from the index, so two are allowed.
	for _, id := range []string{"run-empty-1", "run-empty-2"} {
		r := run
		r.ID, r.TargetSHA = id, ""
		if err := s.InsertReviewRun(ctx, r); err != nil {
			t.Fatalf("empty-sha insert %s: %v", id, err)
		}
	}
}

func TestReviewRunEvidenceRoundTripsAndLegacyShapeRemainsUnknown(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerClaudeCode, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	want := domain.ReviewRun{
		ID: "run-evidence", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode,
		PRURL: "https://example/pr/evidence", TargetSHA: "sha-evidence", LaunchID: "launch-evidence",
		TargetBinding: domain.ReviewTargetBinding{TargetSHA: "sha-evidence", ExecutionBaseSHA: "base-evidence", WorkspacePath: "/ws/evidence", ObservedHeadSHA: "sha-evidence", WorkspaceDirtyKnown: true, IndexStagedKnown: true, ObservationPhase: "review_run_creation", ObservedAt: now},
		Provenance:    domain.ReviewerProvenance{Requested: domain.ReviewerSettings{Harness: domain.ReviewerCodex, Observed: true}, ProviderEffective: domain.ReviewerSettings{Observed: false}},
		Status:        domain.ReviewRunRunning, Verdict: domain.VerdictNone, CreatedAt: now,
	}
	if err := s.InsertReviewRun(ctx, want); err != nil {
		t.Fatalf("insert evidence run: %v", err)
	}
	got, ok, err := s.GetReviewRun(ctx, want.ID)
	if err != nil || !ok {
		t.Fatalf("get evidence run: ok=%v err=%v", ok, err)
	}
	if got.TargetBinding != want.TargetBinding || got.Provenance.Requested != want.Provenance.Requested || got.LaunchID != want.LaunchID || got.Provenance.ProviderEffective.Observed {
		t.Fatalf("round-tripped evidence = %+v", got)
	}
}

func TestReviewerCleanupObligationRoundTripsAcrossReopenAndResolvesIdempotently(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()
	s, err := sqlitetest.Open(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	now := time.Unix(123, 0).UTC()
	want := domain.ReviewerCleanupObligation{
		ID:             "cleanup-1",
		SessionID:      "session-1",
		ReviewID:       "review-1",
		Harness:        domain.ReviewerClaudeCode,
		HandleID:       "runtime-1",
		LaunchID:       "launch-1",
		AgentSessionID: "native-1",
		Status:         domain.ReviewerCleanupUnresolved,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := s.InsertReviewerCleanupObligation(ctx, want); err != nil {
		t.Fatalf("insert cleanup obligation: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}
	s, err = sqlite.OpenPreMigrated(dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.ListUnresolvedReviewerCleanupObligations(ctx, want.SessionID)
	if err != nil || len(got) != 1 {
		t.Fatalf("list after reopen = %v, %v; want one obligation", got, err)
	}
	if got[0].ID != want.ID || got[0].HandleID != want.HandleID || got[0].LaunchID != want.LaunchID || got[0].Harness != want.Harness || got[0].Status != domain.ReviewerCleanupUnresolved {
		t.Fatalf("reopened obligation = %+v, want exact unresolved identity", got[0])
	}
	if cleared, err := s.ResolveReviewerCleanupObligation(ctx, want.ID, now.Add(time.Second)); err != nil || !cleared {
		t.Fatalf("resolve obligation = %v, %v; want true", cleared, err)
	}
	if cleared, err := s.ResolveReviewerCleanupObligation(ctx, want.ID, now.Add(2*time.Second)); err != nil || cleared {
		t.Fatalf("repeat resolve = %v, %v; want idempotent false", cleared, err)
	}
	remaining, err := s.ListUnresolvedReviewerCleanupObligations(ctx, want.SessionID)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("unresolved obligations after resolve = %v, %v; want none", remaining, err)
	}
}

func TestResolveReviewRunCleanupMarkerTerminalizesAndIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{ID: "rev-marker", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "runtime-marker", ReviewerLaunchID: "launch-marker", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	marker := "[ao reviewer cleanup pending handle: runtime-marker generation: launch-marker]"
	if err := s.InsertReviewRun(ctx, domain.ReviewRun{ID: "run-marker", ReviewID: "rev-marker", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode, PRURL: "https://example/pr/marker", TargetSHA: "sha-marker", LaunchID: "launch-marker", Status: domain.ReviewRunFailed, Body: "cleanup uncertain\n" + marker, CreatedAt: now}); err != nil {
		t.Fatalf("insert marker run: %v", err)
	}
	if cleared, err := s.ResolveReviewRunCleanupMarker(ctx, "run-marker", marker, "cleanup complete"); err != nil || !cleared {
		t.Fatalf("resolve marker = %v, %v; want true", cleared, err)
	}
	got, ok, err := s.GetReviewRun(ctx, "run-marker")
	if err != nil || !ok {
		t.Fatalf("get marker run: ok=%v err=%v", ok, err)
	}
	if got.Status != domain.ReviewRunFailed || got.Body != "cleanup complete" {
		t.Fatalf("resolved marker run = %+v", got)
	}
	if cleared, err := s.ResolveReviewRunCleanupMarker(ctx, "run-marker", marker, "should not rewrite"); err != nil || cleared {
		t.Fatalf("repeat marker resolve = %v, %v; want idempotent false", cleared, err)
	}
}

func TestAppendReviewRunNativeEvidenceAllowsOmittedIdsAndRejectsContradictions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{ID: "rev-append", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerCodex, ReviewerLaunchID: "launch-append", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	if err := s.InsertReviewRun(ctx, domain.ReviewRun{ID: "run-append", ReviewID: "rev-append", SessionID: rec.ID, Harness: domain.ReviewerCodex, PRURL: "https://example/pr/append", TargetSHA: "sha-append", LaunchID: "launch-append", Status: domain.ReviewRunRunning, CreatedAt: now}); err != nil {
		t.Fatalf("insert review run: %v", err)
	}
	if updated, err := s.AppendReviewRunNativeEvidence(ctx, "run-append", "launch-append", string(rec.ID), "native-append", "turn-append", "", now); err != nil || !updated {
		t.Fatalf("first native append: updated=%v err=%v", updated, err)
	}
	if updated, err := s.AppendReviewRunNativeEvidence(ctx, "run-append", "launch-append", string(rec.ID), "native-append", "turn-append", "", now.Add(time.Second)); err != nil || !updated {
		t.Fatalf("equal repeated native append: updated=%v err=%v", updated, err)
	}
	if updated, err := s.AppendReviewRunNativeEvidence(ctx, "run-append", "launch-append", string(rec.ID), "", "", "", now.Add(2*time.Second)); err != nil || updated {
		t.Fatalf("omitted native ids: updated=%v err=%v, want no-claim", updated, err)
	}
	if updated, err := s.AppendReviewRunNativeEvidence(ctx, "run-append", "launch-append", string(rec.ID), "native-append", "", "turn-append", now.Add(3*time.Second)); err != nil || !updated {
		t.Fatalf("equal repeated completion append: updated=%v err=%v", updated, err)
	}
	if updated, err := s.AppendReviewRunNativeEvidence(ctx, "run-append", "launch-append", string(rec.ID), "native-other", "different-turn", "", now.Add(4*time.Second)); err == nil {
		t.Fatalf("contradictory append updated=%v err=%v, want fail-closed error", updated, err)
	} else if updated {
		t.Fatal("contradictory native evidence was appended")
	}
	got, ok, err := s.GetReviewRun(ctx, "run-append")
	if err != nil || !ok {
		t.Fatalf("get appended run: ok=%v err=%v", ok, err)
	}
	if got.Provenance.ProviderNative.SessionID != "native-append" || got.Provenance.ProviderNative.FirstTurnID != "turn-append" || got.Provenance.ProviderNative.CompletionID != "turn-append" {
		t.Fatalf("native evidence = %+v", got.Provenance.ProviderNative)
	}
	review, ok, err := s.GetReviewByID(ctx, "rev-append")
	if err != nil || !ok {
		t.Fatalf("get appended review: ok=%v err=%v", ok, err)
	}
	if review.AgentSessionID != "native-append" {
		t.Fatalf("parent review native session = %q, want unchanged native-append", review.AgentSessionID)
	}
}

func TestAcceptReviewerReplacementCommitsAuthorityAndPredecessorAtomically(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	incumbent := domain.Review{
		ID: "rev-replace", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerClaudeCode,
		ReviewerHandleID: "review-old", AgentSessionID: "native-old", ReviewerLaunchID: "launch-old", ReviewerActivityState: domain.ActivityActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.UpsertReview(ctx, incumbent); err != nil {
		t.Fatalf("upsert incumbent: %v", err)
	}
	predecessor := domain.ReviewRun{
		ID: "run-old", ReviewID: incumbent.ID, SessionID: rec.ID, Harness: domain.ReviewerClaudeCode,
		TriggerSource: domain.ReviewTriggerManual,
		PRURL:         "https://example/pr/replacement", TargetSHA: "sha-replacement", LaunchID: incumbent.ReviewerLaunchID,
		TargetBinding: domain.ReviewTargetBinding{TargetSHA: "sha-replacement", ExecutionBaseSHA: "base", WorkspacePath: "/ws/mer", ObservedHeadSHA: "sha-replacement", ObservationPhase: "trigger", ObservedAt: now},
		Provenance:    domain.ReviewerProvenance{AOResolved: domain.ReviewerSettings{Harness: domain.ReviewerClaudeCode, Model: "old", Observed: true}},
		Status:        domain.ReviewRunRunning, Verdict: domain.VerdictNone, CreatedAt: now, AutoInjectReview: true,
	}
	if err := s.InsertReviewRun(ctx, predecessor); err != nil {
		t.Fatalf("insert predecessor: %v", err)
	}
	replacement := incumbent
	replacement.ReviewerHandleID = "review-new"
	replacement.AgentSessionID = ""
	replacement.ReviewerLaunchID = "launch-new"
	replacement.UpdatedAt = now.Add(time.Second)
	cleanup := domain.ReviewerCleanupObligation{
		ID: "cleanup-retired", SessionID: rec.ID, ReviewID: incumbent.ID,
		Harness: incumbent.Harness, HandleID: incumbent.ReviewerHandleID,
		LaunchID: incumbent.ReviewerLaunchID, AgentSessionID: incumbent.AgentSessionID,
		Status: domain.ReviewerCleanupUnresolved, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.AcceptReviewerReplacement(ctx, incumbent, replacement, []domain.ReviewRun{predecessor}, true, cleanup); err != nil {
		t.Fatalf("accept replacement: %v", err)
	}
	gotReview, ok, err := s.GetReviewByID(ctx, incumbent.ID)
	if err != nil || !ok {
		t.Fatalf("get replacement review: ok=%v err=%v", ok, err)
	}
	if gotReview.ReviewerHandleID != "review-new" || gotReview.AgentSessionID != "" || gotReview.ReviewerLaunchID != "launch-new" || gotReview.ReviewerActivityState != domain.ActivityActive {
		t.Fatalf("replacement review = %+v", gotReview)
	}
	gotRun, ok, err := s.GetReviewRun(ctx, predecessor.ID)
	if err != nil || !ok {
		t.Fatalf("get predecessor: ok=%v err=%v", ok, err)
	}
	if gotRun.Status != domain.ReviewRunCancelled || gotRun.LaunchID != predecessor.LaunchID || gotRun.TargetBinding != predecessor.TargetBinding || gotRun.Provenance != predecessor.Provenance {
		t.Fatalf("predecessor after replacement = %+v", gotRun)
	}
	pending, err := s.ListUnresolvedReviewerCleanupObligations(ctx, rec.ID)
	if err != nil || len(pending) != 1 || pending[0] != cleanup {
		t.Fatalf("retired identity after acceptance = %+v, %v", pending, err)
	}
}

func TestAcceptReviewerReplacementRejectsStalePredecessorWithoutPartialAuthority(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	incumbent := domain.Review{ID: "rev-stale", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "review-old", AgentSessionID: "native-old", ReviewerLaunchID: "launch-old", ReviewerActivityState: domain.ActivityActive, CreatedAt: now, UpdatedAt: now}
	if err := s.UpsertReview(ctx, incumbent); err != nil {
		t.Fatalf("upsert incumbent: %v", err)
	}
	predecessor := domain.ReviewRun{ID: "run-stale", ReviewID: incumbent.ID, SessionID: rec.ID, Harness: domain.ReviewerClaudeCode, PRURL: "https://example/pr/stale", TargetSHA: "sha-stale", LaunchID: "launch-old", Status: domain.ReviewRunRunning, Verdict: domain.VerdictNone, CreatedAt: now}
	if err := s.InsertReviewRun(ctx, predecessor); err != nil {
		t.Fatalf("insert predecessor: %v", err)
	}
	stale := predecessor
	stale.LaunchID = "different-launch"
	replacement := incumbent
	replacement.ReviewerHandleID = "review-new"
	replacement.ReviewerLaunchID = "launch-new"
	cleanup := domain.ReviewerCleanupObligation{
		ID: "cleanup-retired", SessionID: rec.ID, ReviewID: incumbent.ID,
		Harness: incumbent.Harness, HandleID: incumbent.ReviewerHandleID,
		LaunchID: incumbent.ReviewerLaunchID, AgentSessionID: incumbent.AgentSessionID,
		Status: domain.ReviewerCleanupUnresolved, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.AcceptReviewerReplacement(ctx, incumbent, replacement, []domain.ReviewRun{stale}, false, cleanup); err == nil {
		t.Fatal("stale predecessor was accepted")
	}
	gotReview, ok, err := s.GetReviewByID(ctx, incumbent.ID)
	if err != nil || !ok {
		t.Fatalf("get incumbent: ok=%v err=%v", ok, err)
	}
	if gotReview.ReviewerHandleID != incumbent.ReviewerHandleID || gotReview.AgentSessionID != incumbent.AgentSessionID || gotReview.ReviewerLaunchID != incumbent.ReviewerLaunchID || gotReview.ReviewerActivityState != incumbent.ReviewerActivityState {
		t.Fatalf("incumbent after rejected replacement = %+v", gotReview)
	}
	gotRun, ok, err := s.GetReviewRun(ctx, predecessor.ID)
	if err != nil || !ok {
		t.Fatalf("get predecessor after rejection: ok=%v err=%v", ok, err)
	}
	if gotRun.Status != domain.ReviewRunRunning {
		t.Fatalf("predecessor after rejected replacement = %+v, want running", gotRun)
	}
	pending, err := s.ListUnresolvedReviewerCleanupObligations(ctx, rec.ID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("rejected acceptance left cleanup = %+v, %v", pending, err)
	}
}

func TestAcceptReviewerReplacementRetiredCleanupRefusalPreservesIncumbentSQLite(t *testing.T) {
	for _, fault := range []string{"missing", "foreign generation", "foreign handle", "foreign session", "foreign review", "foreign harness", "foreign native session", "conflicting id", "resolved id", "insert refusal", "post-insert refusal", "exact duplicate"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			s, err := sqlitetest.Open(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			seedProject(t, s, "mer")
			rec, err := s.CreateSession(ctx, sampleRecord("mer"))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(123, 0).UTC()
			incumbent := domain.Review{ID: "review-old", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerClaudeCode, ReviewerHandleID: "ptyhost-v1:review-old", ReviewerLaunchID: "old-generation", AgentSessionID: "native-old", CreatedAt: now, UpdatedAt: now}
			if err := s.UpsertReview(ctx, incumbent); err != nil {
				t.Fatal(err)
			}
			predecessor := domain.ReviewRun{ID: "run-old", ReviewID: incumbent.ID, SessionID: rec.ID, Harness: incumbent.Harness, TriggerSource: domain.ReviewTriggerManual, PRURL: "https://example/pr/cleanup", TargetSHA: "sha", LaunchID: incumbent.ReviewerLaunchID, Status: domain.ReviewRunRunning, CreatedAt: now}
			if err := s.InsertReviewRun(ctx, predecessor); err != nil {
				t.Fatal(err)
			}
			replacement := incumbent
			replacement.ReviewerHandleID, replacement.ReviewerLaunchID = "ptyhost-v1:review-new", "new-generation"
			cleanup := domain.ReviewerCleanupObligation{ID: "cleanup-old", SessionID: rec.ID, ReviewID: incumbent.ID, Harness: incumbent.Harness, HandleID: incumbent.ReviewerHandleID, LaunchID: incumbent.ReviewerLaunchID, AgentSessionID: incumbent.AgentSessionID, Status: domain.ReviewerCleanupUnresolved, CreatedAt: now, UpdatedAt: now}
			existingCount := 0
			switch fault {
			case "missing":
				cleanup = domain.ReviewerCleanupObligation{}
			case "foreign generation":
				cleanup.LaunchID = "foreign"
			case "foreign handle":
				cleanup.HandleID = "foreign"
			case "foreign session":
				cleanup.SessionID = "foreign"
			case "foreign review":
				cleanup.ReviewID = "foreign"
			case "foreign harness":
				cleanup.Harness = domain.ReviewerCodex
			case "foreign native session":
				cleanup.AgentSessionID = "foreign"
			case "conflicting id", "resolved id", "exact duplicate":
				existing := cleanup
				if fault == "conflicting id" {
					existing.HandleID = "foreign"
				}
				if err := s.InsertReviewerCleanupObligation(ctx, existing); err != nil {
					t.Fatal(err)
				}
				existingCount = 1
				if fault == "resolved id" {
					if ok, err := s.ResolveReviewerCleanupObligation(ctx, cleanup.ID, now); err != nil || !ok {
						t.Fatalf("resolve seed: %t, %v", ok, err)
					}
					existingCount = 0
				}
			case "insert refusal", "post-insert refusal":
				file := url.URL{Path: filepath.Join(dataDir, "ao.db")}
				db, err := sql.Open("sqlite", "file:"+file.EscapedPath())
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				statement := "CREATE TRIGGER refuse_cleanup BEFORE INSERT ON reviewer_cleanup_obligation BEGIN SELECT RAISE(ABORT, 'injected atomic cleanup refusal'); END"
				if fault == "post-insert refusal" {
					statement = "CREATE TRIGGER refuse_replacement BEFORE UPDATE ON review BEGIN SELECT RAISE(ABORT, 'injected atomic authority refusal'); END"
				}
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			acceptErr := s.AcceptReviewerReplacement(ctx, incumbent, replacement, []domain.ReviewRun{predecessor}, false, cleanup)
			if fault == "exact duplicate" {
				if acceptErr != nil {
					t.Fatalf("exact duplicate rejected: %v", acceptErr)
				}
			} else if acceptErr == nil {
				t.Fatal("replacement accepted without durable exact unresolved cleanup")
			} else if strings.Contains(fault, "refusal") && !strings.Contains(acceptErr.Error(), "injected atomic") {
				t.Fatalf("did not exercise SQLite refusal: %v", acceptErr)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = sqlite.OpenPreMigrated(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			current, found, err := s.GetReviewByID(ctx, incumbent.ID)
			want := incumbent
			wantStatus := domain.ReviewRunRunning
			if fault == "exact duplicate" {
				want, wantStatus = replacement, domain.ReviewRunCancelled
			}
			if err != nil || !found || current.ReviewerHandleID != want.ReviewerHandleID || current.ReviewerLaunchID != want.ReviewerLaunchID || current.AgentSessionID != want.AgentSessionID {
				t.Fatalf("authority after reopen = %+v, %t, %v", current, found, err)
			}
			fresh, found, err := s.GetReviewRun(ctx, predecessor.ID)
			if err != nil || !found || fresh.Status != wantStatus {
				t.Fatalf("predecessor after reopen = %+v, %t, %v", fresh, found, err)
			}
			pending, err := s.ListUnresolvedReviewerCleanupObligations(ctx, rec.ID)
			if err != nil || len(pending) != existingCount {
				t.Fatalf("cleanup after acceptance/reopen = %+v, %v", pending, err)
			}
			t.Logf("fault=%s acceptance=%v authority=%s/%s predecessor=%s pending=%d", fault, acceptErr, current.ReviewerHandleID, current.ReviewerLaunchID, fresh.Status, len(pending))
		})
	}
}

// Harness is part of the idempotency key, so a different reviewer on the same
// commit is a second opinion rather than a duplicate. Without this the reviewer
// picker is inert on an already-reviewed commit.
func TestInsertReviewRunAllowsADifferentHarnessForTheSameCommit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerClaudeCode, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	first := domain.ReviewRun{
		ID: "run-1", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode,
		PRURL: "https://example/pr/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning, Verdict: domain.VerdictNone, CreatedAt: now,
	}
	if err := s.InsertReviewRun(ctx, first); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	other := first
	other.ID = "run-other-harness"
	other.Harness = domain.ReviewerCodex
	if err := s.InsertReviewRun(ctx, other); err != nil {
		t.Fatalf("a different harness on the same commit should insert: %v", err)
	}

	// ...but the same harness twice is still a duplicate.
	same := first
	same.ID = "run-same-harness"
	if err := s.InsertReviewRun(ctx, same); !errors.Is(err, domain.ErrDuplicateReviewRun) {
		t.Fatalf("same harness duplicate err = %v, want ErrDuplicateReviewRun", err)
	}
}

func TestInsertReviewRunAllowsConfigOnlySecondOpinionForSameHarness(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{ID: "rev-config", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerClaudeCode, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	base := domain.ReviewRun{ID: "run-config-1", ReviewID: "rev-config", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode, PRURL: "https://example/pr/config", TargetSHA: "sha-config", LaunchID: "launch-config-1", Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved, CreatedAt: now, Provenance: domain.ReviewerProvenance{AOResolved: domain.ReviewerSettings{Harness: domain.ReviewerClaudeCode, Model: "claude", Mode: "balanced", Observed: true}}}
	if err := s.InsertReviewRun(ctx, base); err != nil {
		t.Fatalf("insert first config opinion: %v", err)
	}
	second := base
	second.ID = "run-config-2"
	second.LaunchID = "launch-config-2"
	second.CreatedAt = now.Add(time.Second)
	second.Provenance.AOResolved.Mode = "thorough"
	if err := s.InsertReviewRun(ctx, second); err != nil {
		t.Fatalf("insert distinct config opinion: %v", err)
	}
}

func TestInsertReviewRunAllowsRerunAfterChangesRequested(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerClaudeCode, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	run := domain.ReviewRun{
		ID: "run-1", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode,
		PRURL: "https://example/pr/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning, Verdict: domain.VerdictNone, CreatedAt: now,
	}
	if err := s.InsertReviewRun(ctx, run); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if ok, err := s.UpdateReviewRunResult(ctx, "run-1", domain.ReviewRunComplete, domain.VerdictChangesRequested, "please fix", "rev-1", true); err != nil {
		t.Fatalf("mark changes requested: %v", err)
	} else if !ok {
		t.Fatal("mark changes requested: got ok=false")
	}

	rerun := run
	rerun.ID = "run-2"
	rerun.CreatedAt = now.Add(time.Second)
	if err := s.InsertReviewRun(ctx, rerun); err != nil {
		t.Fatalf("rerun after changes_requested insert: %v", err)
	}
}

func TestInsertReviewRunAllowsRerunAfterTerminalEmptyVerdict(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerClaudeCode, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	run := domain.ReviewRun{
		ID: "run-1", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerClaudeCode,
		PRURL: "https://example/pr/1", TargetSHA: "sha1", Status: domain.ReviewRunComplete, Verdict: domain.VerdictNone, CreatedAt: now,
	}
	if err := s.InsertReviewRun(ctx, run); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	rerun := run
	rerun.ID = "run-2"
	rerun.Status = domain.ReviewRunRunning
	rerun.CreatedAt = now.Add(time.Second)
	if err := s.InsertReviewRun(ctx, rerun); err != nil {
		t.Fatalf("rerun after terminal empty-verdict insert: %v", err)
	}
}

func TestReviewUpsertReusesRowAndRunRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)

	// First upsert creates the review row.
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerClaudeCode, PRURL: "https://example/pr/1",
		ReviewerHandleID: "review-mer-1",
		CreatedAt:        now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	// A different harness gets its own row for the same worker session.
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-2", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerHarness("greptile"), PRURL: "https://example/pr/2",
		ReviewerHandleID: "review-mer-1b",
		ReviewerLaunchID: "launch-2",
		CreatedAt:        now, UpdatedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("upsert review (second harness): %v", err)
	}
	got, ok, err := s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerClaudeCode)
	if err != nil || !ok {
		t.Fatalf("get claude review: ok=%v err=%v", ok, err)
	}
	if got.ID != "rev-1" {
		t.Fatalf("claude review id = %q, want rev-1", got.ID)
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get greptile review: ok=%v err=%v", ok, err)
	}
	if got.ID != "rev-2" {
		t.Fatalf("greptile review id = %q, want rev-2", got.ID)
	}
	if got.Harness != domain.ReviewerHarness("greptile") || got.PRURL != "https://example/pr/2" || got.ReviewerHandleID != "review-mer-1b" {
		t.Fatalf("second harness fields: %+v", got)
	}
	// A same-harness upsert reuses that row and records the native session id.
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-3", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerHarness("greptile"), PRURL: "https://example/pr/2",
		ReviewerHandleID: "review-mer-1b",
		AgentSessionID:   "reviewer-native-1",
		CreatedAt:        now, UpdatedAt: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("upsert review (agent session): %v", err)
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get review after agent session update: ok=%v err=%v", ok, err)
	}
	if got.ID != "rev-2" {
		t.Fatalf("same-harness upsert changed row id = %q, want rev-2", got.ID)
	}
	if got.AgentSessionID != "reviewer-native-1" {
		t.Fatalf("agent session id = %q, want reviewer-native-1", got.AgentSessionID)
	}
	updated, err := s.UpdateReviewActivity(ctx, "rev-1", domain.ActivityIdle, "claude-native-1", "")
	if err != nil || !updated {
		t.Fatalf("update claude activity/native session: updated=%v err=%v", updated, err)
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerClaudeCode)
	if err != nil || !ok {
		t.Fatalf("get claude review after native update: ok=%v err=%v", ok, err)
	}
	if got.AgentSessionID != "claude-native-1" {
		t.Fatalf("claude agent session id = %q, want claude-native-1", got.AgentSessionID)
	}
	if got.ReviewerActivityState != domain.ActivityIdle {
		t.Fatalf("claude reviewer activity state = %q, want idle", got.ReviewerActivityState)
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get greptile review after claude update: ok=%v err=%v", ok, err)
	}
	if got.AgentSessionID != "reviewer-native-1" {
		t.Fatalf("greptile agent session id = %q, want reviewer-native-1", got.AgentSessionID)
	}
	if got.ReviewerLaunchID != "launch-2" {
		t.Fatalf("greptile reviewer launch id = %q, want launch-2", got.ReviewerLaunchID)
	}
	updated, err = s.UpdateReviewActivity(ctx, "rev-2", domain.ActivityIdle, "", "stale-launch")
	if err != nil {
		t.Fatalf("stale launch update: %v", err)
	}
	if updated {
		t.Fatal("stale launch update unexpectedly succeeded")
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get greptile review after stale launch update: ok=%v err=%v", ok, err)
	}
	if got.ReviewerActivityState != "" {
		t.Fatalf("stale launch update changed activity state = %q, want empty", got.ReviewerActivityState)
	}
	updated, err = s.UpdateReviewActivity(ctx, "rev-2", domain.ActivityBlocked, "legacy-native", "")
	if err != nil {
		t.Fatalf("empty launch update against claimed generation: %v", err)
	}
	if updated {
		t.Fatal("empty launch update unexpectedly succeeded against claimed generation")
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get greptile review after empty launch update: ok=%v err=%v", ok, err)
	}
	if got.ReviewerActivityState != "" || got.AgentSessionID != "reviewer-native-1" {
		t.Fatalf("empty launch update changed review = %+v", got)
	}
	updated, err = s.UpdateReviewActivity(ctx, "rev-2", domain.ActivityIdle, "", "launch-2")
	if err != nil || !updated {
		t.Fatalf("matching launch update: updated=%v err=%v", updated, err)
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get greptile review after matching launch update: ok=%v err=%v", ok, err)
	}
	if got.ReviewerActivityState != domain.ActivityIdle {
		t.Fatalf("matching launch update changed activity state = %q, want idle", got.ReviewerActivityState)
	}
	if err := s.ClearReviewerHandle(ctx, rec.ID); err != nil {
		t.Fatalf("clear reviewer handle: %v", err)
	}
	got, ok, err = s.GetReviewBySessionAndHarness(ctx, rec.ID, domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("get review after clear: ok=%v err=%v", ok, err)
	}
	if got.ReviewerHandleID != "" {
		t.Fatalf("reviewer handle after clear = %q, want empty", got.ReviewerHandleID)
	}

	// A run inserts running and updates to complete/changes_requested.
	if err := s.InsertReviewRun(ctx, domain.ReviewRun{
		ID: "run-1", ReviewID: got.ID, SessionID: rec.ID, BatchID: "batch-1", Harness: domain.ReviewerHarness("greptile"),
		PRURL: got.PRURL, TargetSHA: "sha1", Status: domain.ReviewRunRunning, Verdict: domain.VerdictNone,
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if ok, err := s.UpdateReviewRunResult(ctx, "run-1", domain.ReviewRunComplete, domain.VerdictChangesRequested, "please fix", "rev-987", false); err != nil {
		t.Fatalf("update run: %v", err)
	} else if !ok {
		t.Fatal("update run: got ok=false")
	}

	gotRun, ok, err := s.GetReviewRun(ctx, "run-1")
	if err != nil || !ok {
		t.Fatalf("get run: ok=%v err=%v", ok, err)
	}
	if gotRun.ID != "run-1" || gotRun.SessionID != rec.ID || gotRun.BatchID != "batch-1" || gotRun.TargetSHA != "sha1" {
		t.Fatalf("get run = %+v", gotRun)
	}

	bySHA, ok, err := s.GetReviewRunBySessionPRAndSHA(ctx, rec.ID, got.PRURL, "sha1")
	if err != nil || !ok {
		t.Fatalf("by sha: ok=%v err=%v", ok, err)
	}
	if bySHA.Status != domain.ReviewRunComplete || bySHA.Verdict != domain.VerdictChangesRequested || bySHA.Body != "please fix" || bySHA.GithubReviewID != "rev-987" {
		t.Fatalf("run result not persisted: %+v", bySHA)
	}
	byHarness, ok, err := s.GetReviewRunBySessionPRSHAAndHarness(ctx, rec.ID, got.PRURL, "sha1", domain.ReviewerHarness("greptile"))
	if err != nil || !ok {
		t.Fatalf("by harness: ok=%v err=%v", ok, err)
	}
	if byHarness.ID != "run-1" {
		t.Fatalf("by harness = %+v, want run-1", byHarness)
	}
	if _, ok, _ := s.GetReviewRunBySessionPRSHAAndHarness(ctx, rec.ID, got.PRURL, "sha1", domain.ReviewerCodex); ok {
		t.Fatal("unexpected run for a different harness")
	}
	if _, ok, _ := s.GetReviewRunBySessionPRAndSHA(ctx, rec.ID, got.PRURL, "other"); ok {
		t.Fatal("unexpected run for a different sha")
	}

	runs, err := s.ListReviewRunsBySession(ctx, rec.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != "run-1" || runs[0].AutoInjectReview {
		t.Fatalf("list runs = %+v", runs)
	}
	batchRuns, err := s.ListReviewRunsByBatch(ctx, rec.ID, "batch-1")
	if err != nil {
		t.Fatalf("list batch runs: %v", err)
	}
	if len(batchRuns) != 1 || batchRuns[0].ID != "run-1" || batchRuns[0].BatchID != "batch-1" {
		t.Fatalf("batch runs = %+v", batchRuns)
	}

	if ok, err := s.UpdateReviewRunResult(ctx, "run-1", domain.ReviewRunComplete, domain.VerdictApproved, "again", "", true); err != nil {
		t.Fatalf("second update: %v", err)
	} else if ok {
		t.Fatal("second update completed an already-complete run")
	}
}

func TestCancelRunningReviewRunsBySession(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.UpsertReview(ctx, domain.Review{
		ID: "rev-1", SessionID: rec.ID, ProjectID: rec.ProjectID,
		Harness: domain.ReviewerCodex, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("upsert review: %v", err)
	}
	for _, run := range []domain.ReviewRun{
		{ID: "run-1", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerCodex, PRURL: "https://example/pr/1", TargetSHA: "sha1", Status: domain.ReviewRunRunning, CreatedAt: now},
		{ID: "run-2", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerCodex, PRURL: "https://example/pr/2", TargetSHA: "sha2", Status: domain.ReviewRunRunning, CreatedAt: now.Add(time.Second)},
		{ID: "run-3", ReviewID: "rev-1", SessionID: rec.ID, Harness: domain.ReviewerCodex, PRURL: "https://example/pr/3", TargetSHA: "sha3", Status: domain.ReviewRunComplete, Verdict: domain.VerdictApproved, CreatedAt: now.Add(2 * time.Second)},
	} {
		if err := s.InsertReviewRun(ctx, run); err != nil {
			t.Fatalf("insert %s: %v", run.ID, err)
		}
	}
	running, err := s.ListRunningReviewRunsBySession(ctx, rec.ID)
	if err != nil {
		t.Fatalf("list running: %v", err)
	}
	if len(running) != 2 || running[0].ID != "run-2" || running[1].ID != "run-1" {
		t.Fatalf("running = %+v", running)
	}
	n, err := s.CancelRunningReviewRunsBySession(ctx, rec.ID, "cancelled by user")
	if err != nil {
		t.Fatalf("cancel running: %v", err)
	}
	if n != 2 {
		t.Fatalf("cancelled rows = %d, want 2", n)
	}
	runs, err := s.ListReviewRunsBySession(ctx, rec.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	byID := map[string]domain.ReviewRun{}
	for _, run := range runs {
		byID[run.ID] = run
	}
	if byID["run-1"].Status != domain.ReviewRunCancelled || byID["run-2"].Status != domain.ReviewRunCancelled {
		t.Fatalf("running runs not cancelled: %+v", byID)
	}
	if byID["run-3"].Status != domain.ReviewRunComplete {
		t.Fatalf("complete run changed: %+v", byID["run-3"])
	}
}

func TestReviewGettersMissing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, ok, err := s.GetReviewBySession(ctx, "mer-1"); err != nil || ok {
		t.Fatalf("missing review: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.GetReviewRunBySessionPRAndSHA(ctx, "mer-1", "pr1", "sha1"); err != nil || ok {
		t.Fatalf("missing run: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s.GetReviewRun(ctx, "run-missing"); err != nil || ok {
		t.Fatalf("missing run by id: ok=%v err=%v", ok, err)
	}
}
