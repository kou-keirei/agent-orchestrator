package review

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	reviewcore "github.com/aoagents/agent-orchestrator/backend/internal/review"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// Faults replace only the failing storage operation. Acceptance, result
// submission, remaining persistence, restart and readback use production code.
type replacementCleanupFaultStore struct {
	*sqlitestore.Store
	fault string
}

func (s *replacementCleanupFaultStore) SupersedeStaleRunningReviewRuns(ctx context.Context, sessionID domain.SessionID, prURL, targetSHA, body string) (int64, error) {
	if s.fault == "post-accept supersession" {
		return 0, errors.New("injected stale-run write refusal")
	}
	return s.Store.SupersedeStaleRunningReviewRuns(ctx, sessionID, prURL, targetSHA, body)
}

func (s *replacementCleanupFaultStore) InsertReviewerCleanupObligation(ctx context.Context, obligation domain.ReviewerCleanupObligation) error {
	if s.fault == "cleanup ledger write" {
		return errors.New("injected cleanup-ledger write refusal")
	}
	return s.Store.InsertReviewerCleanupObligation(ctx, obligation)
}

func (s *replacementCleanupFaultStore) ResolveReviewerCleanupObligation(ctx context.Context, id string, at time.Time) (bool, error) {
	if s.fault == "cleanup resolution" {
		return false, errors.New("injected cleanup resolution refusal")
	}
	if s.fault == "cleanup resolution stale" {
		return false, nil
	}
	return s.Store.ResolveReviewerCleanupObligation(ctx, id, at)
}

type replacementCleanupLauncher struct {
	cleanupReplacementLauncher
	destroySucceeds bool
}

func (l *replacementCleanupLauncher) Destroy(ctx context.Context, handle string) error {
	if !l.destroySucceeds {
		return l.cleanupReplacementLauncher.Destroy(ctx, handle)
	}
	l.destroyed = append(l.destroyed, handle)
	return nil
}

func TestReplacementStorageFailureKeepsRetiredIdentitySQLite(t *testing.T) {
	for _, fault := range []string{"post-accept supersession", "cleanup ledger write", "cleanup resolution", "cleanup resolution stale", "successful teardown"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			st, err := sqlitetest.Open(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if st != nil {
					_ = st.Close()
				}
			})
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
			incumbent := domain.Review{
				ID: "review-old", SessionID: worker.ID, ProjectID: worker.ProjectID, Harness: worker.ReviewerHarness,
				ReviewerHandleID: "ptyhost-v1:review-worker", ReviewerLaunchID: "old-generation",
				AgentSessionID: "native-old", CreatedAt: now, UpdatedAt: now,
			}
			if err := st.UpsertReview(ctx, incumbent); err != nil {
				t.Fatal(err)
			}
			launcher := &replacementCleanupLauncher{
				cleanupReplacementLauncher: cleanupReplacementLauncher{probe: ports.FencedProbeResult{Liveness: ports.FencedUnknown, Reason: ports.FencedReasonProbeFailed}},
				destroySucceeds:            strings.HasPrefix(fault, "cleanup resolution") || fault == "successful teardown",
			}
			engine := reviewcore.New(reviewcore.Deps{
				Store: &replacementCleanupFaultStore{Store: st, fault: fault}, Sessions: st, PRs: cleanupReplacementPRs{}, Projects: st,
				Launcher: launcher, Workspace: cleanupReplacementWorkspace{}, Clock: func() time.Time { return now },
			})
			_, triggerErr := engine.Trigger(ctx, worker.ID, worker.ReviewerHarness, domain.AgentConfig{Model: "new-model"})
			wantError := "injected"
			if fault == "cleanup resolution stale" {
				wantError = "was not unresolved"
			}
			if fault == "successful teardown" {
				if triggerErr != nil {
					t.Fatalf("successful replacement: %v", triggerErr)
				}
			} else if triggerErr == nil || !strings.Contains(triggerErr.Error(), wantError) {
				t.Fatalf("trigger did not exercise storage refusal: %v", triggerErr)
			}
			runs, err := st.ListReviewRunsBySession(ctx, worker.ID)
			if err != nil || len(runs) != 1 || runs[0].Status != domain.ReviewRunRunning {
				t.Fatalf("accepted replacement run = %+v, err=%v", runs, err)
			}
			if _, err := New(engine, st).Submit(ctx, worker.ID, runs[0].ID, domain.VerdictApproved, "normal accepted replacement result", "provider-review"); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = sqlite.OpenPreMigrated(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			current, found, err := st.GetReviewBySessionAndHarness(ctx, worker.ID, worker.ReviewerHarness)
			if err != nil || !found {
				t.Fatalf("fresh review authority: %+v, found=%t err=%v", current, found, err)
			}
			pending, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
			if err != nil {
				t.Fatal(err)
			}
			identityRetained := current.ReviewerHandleID == incumbent.ReviewerHandleID && current.ReviewerLaunchID == incumbent.ReviewerLaunchID
			for _, obligation := range pending {
				identityRetained = identityRetained || (obligation.HandleID == incumbent.ReviewerHandleID && obligation.LaunchID == incumbent.ReviewerLaunchID)
			}
			freshRuns, err := st.ListReviewRunsBySession(ctx, worker.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, run := range freshRuns {
				identityRetained = identityRetained || (strings.Contains(run.Body, incumbent.ReviewerHandleID) && strings.Contains(run.Body, incumbent.ReviewerLaunchID))
			}
			recovered := reviewcore.New(reviewcore.Deps{Store: st, Sessions: st, PRs: cleanupReplacementPRs{}, Projects: st, Launcher: launcher, Workspace: cleanupReplacementWorkspace{}})
			reconcileErr := recovered.ReconcileReviewerCleanup(ctx, worker.ID)
			t.Logf("fault=%s trigger=%v current=%s/%s pending=%d destroys=%v reconciliation=%v", fault, triggerErr, current.ReviewerHandleID, current.ReviewerLaunchID, len(pending), launcher.destroyed, reconcileErr)
			assertResult := func() {
				t.Helper()
				fresh, found, err := st.GetReviewRun(ctx, runs[0].ID)
				if err != nil || !found || fresh.Status != domain.ReviewRunComplete || fresh.Verdict != domain.VerdictApproved || fresh.Body != "normal accepted replacement result" || fresh.GithubReviewID != "provider-review" {
					t.Fatalf("persisted replacement result = %+v, found=%t err=%v", fresh, found, err)
				}
			}
			assertResult()
			if fault == "successful teardown" {
				if len(pending) != 0 || reconcileErr != nil || len(launcher.destroyed) != 1 || launcher.destroyed[0] != incumbent.ReviewerHandleID || current.ReviewerHandleID != "ptyhost-v1:review-worker-replacement" || current.ReviewerLaunchID != runs[0].LaunchID {
					t.Fatalf("successful cleanup = %+v, %v, destroys=%v, authority=%+v", pending, reconcileErr, launcher.destroyed, current)
				}
				return
			}
			if !identityRetained {
				t.Fatal("accepted replacement erased the unreleased incumbent cleanup identity across storage refusal and restart")
			}
			if current.ReviewerHandleID != "ptyhost-v1:review-worker-replacement" || current.ReviewerLaunchID != runs[0].LaunchID || len(pending) != 1 {
				t.Fatalf("replacement authority and retired cleanup = %+v, %+v", current, pending)
			}
			got := pending[0]
			if got.ReviewID != incumbent.ID || got.RunID != runs[0].ID || got.Harness != incumbent.Harness || got.HandleID != incumbent.ReviewerHandleID || got.LaunchID != incumbent.ReviewerLaunchID || got.AgentSessionID != incumbent.AgentSessionID {
				t.Fatalf("retired cleanup lost exact identity: %+v", got)
			}
			if reconcileErr == nil {
				t.Fatal("unknown generation falsely discharged retired cleanup")
			}
			launcher.probe = ports.FencedProbeResult{Liveness: ports.FencedDead, Reason: ports.FencedReasonExactAbsent}
			for i := 0; i < 2; i++ {
				if err := recovered.ReconcileReviewerCleanup(ctx, worker.ID); err != nil {
					t.Fatalf("reconcile released generation: %v", err)
				}
			}
			remaining, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
			if err != nil || len(remaining) != 0 {
				t.Fatalf("released cleanup still pending: %+v, %v", remaining, err)
			}
			assertResult()
		})
	}
}
