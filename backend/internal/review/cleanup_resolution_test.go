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
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

func TestCleanupRestoredReviewerZeroRowResolutionPreservesSQLiteObligation(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := sqlitetest.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	worker := liveWorker()
	seedReviewWorker(t, st, worker)
	now := time.Unix(123, 0).UTC()
	prior := domain.Review{
		ID: "review", SessionID: worker.ID, ProjectID: worker.ProjectID, Harness: domain.ReviewerClaudeCode,
		ReviewerHandleID: "ptyhost-v1:restore-worker", ReviewerLaunchID: "restore-generation", AgentSessionID: "native-restore",
		CreatedAt: now, UpdatedAt: now,
	}
	if err := st.UpsertReview(ctx, prior); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dataDir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER restore_cleanup_ignored BEFORE UPDATE OF status ON reviewer_cleanup_obligation WHEN NEW.status='cleared' BEGIN SELECT RAISE(IGNORE); END;`); err != nil {
		t.Fatal(err)
	}
	launcher := &fakeLauncher{}
	engine := New(Deps{Store: st, Sessions: st, Launcher: launcher, Clock: func() time.Time { return now }, NewID: func() string { return "restore-cleanup" }})
	if err := engine.cleanupRestoredReviewer(ctx, worker.ID, prior, true, nil, prior.Harness, prior.ReviewerLaunchID, prior.AgentSessionID, prior.ReviewerHandleID); err == nil || !strings.Contains(err.Error(), "was not unresolved") {
		t.Errorf("restore cleanup = %v, want visible zero-row resolution failure", err)
	}
	if launcher.destroyCalls != 1 || launcher.destroyedHandle != prior.ReviewerHandleID {
		t.Fatalf("destroy calls=%d handle=%q", launcher.destroyCalls, launcher.destroyedHandle)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = sqlite.OpenPreMigrated(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending after reopen = %+v, %v", pending, err)
	}
	got := pending[0]
	if got.ID != "restore-cleanup" || got.SessionID != worker.ID || got.ReviewID != prior.ID || got.RunID != "" || got.Harness != prior.Harness || got.HandleID != prior.ReviewerHandleID || got.LaunchID != prior.ReviewerLaunchID || got.AgentSessionID != prior.AgentSessionID || got.Status != domain.ReviewerCleanupUnresolved || got.ResolvedAt != nil {
		t.Fatalf("restore cleanup identity = %+v", got)
	}
	current, found, err := st.GetReviewBySessionAndHarness(ctx, worker.ID, prior.Harness)
	if err != nil || !found || current.ReviewerHandleID != prior.ReviewerHandleID || current.ReviewerLaunchID != prior.ReviewerLaunchID || current.AgentSessionID != prior.AgentSessionID {
		t.Fatalf("restore authority = %+v, found=%t, %v", current, found, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER restore_cleanup_ignored`); err != nil {
		t.Fatal(err)
	}
	launcher.generationProbe = ports.FencedProbeResult{Liveness: ports.FencedDead, Reason: ports.FencedReasonExactAbsent}
	engine = New(Deps{Store: st, Sessions: st, Launcher: launcher})
	if err := engine.ReconcileReviewerCleanup(ctx, worker.ID); err != nil {
		t.Fatalf("retry restored cleanup: %v", err)
	}
	pending, err = st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
	if err != nil || len(pending) != 0 || launcher.destroyCalls != 1 {
		t.Fatalf("retry pending=%+v destroy calls=%d, %v", pending, launcher.destroyCalls, err)
	}
}
