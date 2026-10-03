package review

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	reviewcore "github.com/aoagents/agent-orchestrator/backend/internal/review"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

// Embedding only Store models an adapter that lacks the optional ledger port.
type reconciliationStoreWithoutCleanup struct{ reviewcore.Store }

func TestCleanupReconciliationFailuresPreserveSQLiteObligationAndResult(t *testing.T) {
	for _, scenario := range []string{"zero-row resolution", "missing cleanup capability"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			st, err := sqlitetest.Open(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			now := time.Unix(123, 0).UTC()
			worker := domain.SessionRecord{
				ID: "worker", ProjectID: "project", Kind: domain.KindWorker, Harness: domain.HarnessClaudeCode,
				Metadata:        domain.SessionMetadata{WorkspacePath: "/ws/worker", ExecutionBaseSHA: "base-sha"},
				ReviewerHarness: domain.ReviewerClaudeCode, ReviewerConfig: domain.AgentConfig{Model: "old-model"},
			}
			if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: "/ws", RegisteredAt: now}); err != nil {
				t.Fatal(err)
			}
			worker, err = st.CreateSession(ctx, worker)
			if err != nil {
				t.Fatal(err)
			}
			old := domain.Review{
				ID: "review-old", SessionID: worker.ID, ProjectID: worker.ProjectID, Harness: worker.ReviewerHarness,
				ReviewerHandleID: "ptyhost-v1:review-worker", ReviewerLaunchID: "old-generation", AgentSessionID: "native-old",
				CreatedAt: now, UpdatedAt: now,
			}
			if err := st.UpsertReview(ctx, old); err != nil {
				t.Fatal(err)
			}
			launcher := &cleanupReplacementLauncher{probe: ports.FencedProbeResult{Liveness: ports.FencedUnknown, Reason: ports.FencedReasonProbeFailed}}
			engine := reviewcore.New(reviewcore.Deps{
				Store: st, Sessions: st, PRs: cleanupReplacementPRs{}, Projects: st,
				Launcher: launcher, Workspace: cleanupReplacementWorkspace{},
			})
			if _, err := engine.Trigger(ctx, worker.ID, worker.ReviewerHarness, domain.AgentConfig{Model: "new-model"}); err == nil {
				t.Fatal("replacement should report old-host teardown failure")
			}
			runs, err := st.ListReviewRunsBySession(ctx, worker.ID)
			if err != nil || len(runs) != 1 {
				t.Fatalf("runs = %+v, %v", runs, err)
			}
			if _, err := New(engine, st).Submit(ctx, worker.ID, runs[0].ID, domain.VerdictApproved, "accepted body", "provider-id"); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = sqlite.OpenPreMigrated(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			before, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
			if err != nil || len(before) != 1 {
				t.Fatalf("pending before reconciliation = %+v, %v", before, err)
			}
			if before[0].SessionID != worker.ID || before[0].ReviewID != old.ID || before[0].RunID != runs[0].ID || before[0].Harness != old.Harness || before[0].HandleID != old.ReviewerHandleID || before[0].LaunchID != old.ReviewerLaunchID || before[0].AgentSessionID != old.AgentSessionID {
				t.Fatalf("retired identity = %+v", before[0])
			}
			launcher.probe = ports.FencedProbeResult{Liveness: ports.FencedDead, Reason: ports.FencedReasonExactAbsent}
			var store reviewcore.Store = st
			var db *sql.DB
			wantError := "durable reviewer cleanup obligations are unavailable"
			if scenario == "missing cleanup capability" {
				store = reconciliationStoreWithoutCleanup{Store: st}
			} else {
				wantError = "was not unresolved"
				db, err = sql.Open("sqlite", filepath.Join(dataDir, "ao.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.ExecContext(ctx, `CREATE TRIGGER cleanup_resolution_ignored BEFORE UPDATE OF status ON reviewer_cleanup_obligation WHEN NEW.status='cleared' BEGIN SELECT RAISE(IGNORE); END;`); err != nil {
					t.Fatal(err)
				}
			}
			assertResult := func() {
				t.Helper()
				fresh, found, err := st.GetReviewRun(ctx, runs[0].ID)
				if err != nil || !found || fresh.Status != domain.ReviewRunComplete || fresh.Body != "accepted body" || fresh.Verdict != domain.VerdictApproved || fresh.GithubReviewID != "provider-id" {
					t.Fatalf("submitted result = %+v, found=%t, %v", fresh, found, err)
				}
				current, found, err := st.GetReviewBySessionAndHarness(ctx, worker.ID, worker.ReviewerHarness)
				if err != nil || !found || current.ReviewerHandleID != "ptyhost-v1:review-worker-replacement" || current.ReviewerLaunchID != runs[0].LaunchID {
					t.Fatalf("replacement authority = %+v, found=%t, %v", current, found, err)
				}
			}
			recovered := reviewcore.New(reviewcore.Deps{Store: store, Sessions: st, Launcher: launcher})
			for attempt := 1; attempt <= 2; attempt++ {
				if err := recovered.ReconcileReviewerCleanup(ctx, worker.ID); err == nil || !strings.Contains(err.Error(), wantError) {
					t.Errorf("attempt %d reconciliation = %v, want %q", attempt, err, wantError)
				}
				pending, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
				if err != nil || len(pending) != 1 || pending[0] != before[0] {
					t.Fatalf("pending identity changed = %+v, %v", pending, err)
				}
				assertResult()
			}
			if len(launcher.destroyed) != 1 {
				t.Fatalf("reconciliation destroyed a host: %v", launcher.destroyed)
			}
			if db != nil {
				if _, err := db.ExecContext(ctx, `DROP TRIGGER cleanup_resolution_ignored`); err != nil {
					t.Fatal(err)
				}
			}
			recovered = reviewcore.New(reviewcore.Deps{Store: st, Sessions: st, Launcher: launcher})
			if err := recovered.ReconcileReviewerCleanup(ctx, worker.ID); err != nil {
				t.Fatalf("retry after storage capability recovers: %v", err)
			}
			pending, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
			if err != nil || len(pending) != 0 {
				t.Fatalf("pending after successful retry = %+v, %v", pending, err)
			}
			assertResult()
		})
	}
}
