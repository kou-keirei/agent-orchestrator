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
)

// Only the external launch/runtime and workspace observation boundaries are
// substituted: trigger, replacement acceptance, submit and readback use the
// production engine, service and SQLite store.
type cleanupReplacementLauncher struct {
	reviewcore.Launcher
	destroyed []string
	probe     ports.FencedProbeResult
}

func (*cleanupReplacementLauncher) Preflight(context.Context, domain.ReviewerHarness, string) error {
	return nil
}

func (*cleanupReplacementLauncher) Spawn(_ context.Context, spec reviewcore.LaunchSpec) (reviewcore.LaunchResult, error) {
	return reviewcore.LaunchResult{HandleID: "ptyhost-v1:review-worker-replacement", LaunchID: spec.LaunchID}, nil
}

func (*cleanupReplacementLauncher) Alive(context.Context, string) (bool, error) { return true, nil }
func (*cleanupReplacementLauncher) Reusable(domain.ReviewerHarness) bool        { return true }

func (l *cleanupReplacementLauncher) Destroy(_ context.Context, handle string) error {
	l.destroyed = append(l.destroyed, handle)
	return errors.New("old host teardown refused")
}

func (l *cleanupReplacementLauncher) ProbeReviewerGeneration(_ context.Context, _, generation string) ports.FencedProbeResult {
	if generation == "" {
		return ports.FencedProbeResult{Liveness: ports.FencedUnknown, Reason: ports.FencedReasonIdentityMissing}
	}
	return l.probe
}

type cleanupReplacementPRs struct{}

func (cleanupReplacementPRs) ListPRsBySession(context.Context, domain.SessionID) ([]domain.PullRequest, error) {
	return []domain.PullRequest{{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: "sha1"}}, nil
}

type cleanupReplacementWorkspace struct{}

func (cleanupReplacementWorkspace) ObserveWorkspace(_ context.Context, info ports.WorkspaceInfo) (ports.WorkspaceObservation, error) {
	return ports.WorkspaceObservation{Path: info.Path, Branch: info.Branch, HeadSHA: "sha1"}, nil
}

func TestReplacementResultPreservesOldHostCleanupSQLite(t *testing.T) {
	for _, verdict := range []domain.ReviewVerdict{domain.VerdictApproved, domain.VerdictChangesRequested} {
		t.Run(string(verdict), func(t *testing.T) {
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
			launcher := &cleanupReplacementLauncher{
				probe: ports.FencedProbeResult{Liveness: ports.FencedUnknown, Reason: ports.FencedReasonProbeFailed},
			}
			makeEngine := func() *reviewcore.Engine {
				return reviewcore.New(reviewcore.Deps{
					Store: st, Sessions: st, PRs: cleanupReplacementPRs{}, Projects: st,
					Launcher: launcher, Workspace: cleanupReplacementWorkspace{}, Clock: func() time.Time { return now },
				})
			}
			engine := makeEngine()
			if _, err := engine.Trigger(ctx, worker.ID, worker.ReviewerHarness, domain.AgentConfig{Model: "new-model"}); err == nil || !strings.Contains(err.Error(), "destroy previous reviewer") {
				t.Fatalf("replacement trigger = %v, want old-host teardown failure", err)
			}
			runs, err := st.ListReviewRunsBySession(ctx, worker.ID)
			if err != nil || len(runs) != 1 || runs[0].Status != domain.ReviewRunRunning {
				t.Fatalf("replacement runs = %+v, %v", runs, err)
			}
			body := "valid replacement review result"
			if _, err := New(engine, st).Submit(ctx, worker.ID, runs[0].ID, verdict, body, "provider-review"); err != nil {
				t.Fatalf("normal result submission: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = sqlite.OpenPreMigrated(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			engine = makeEngine()
			assertResult := func() {
				t.Helper()
				fresh, found, err := st.GetReviewRun(ctx, runs[0].ID)
				if err != nil || !found || fresh.Status != domain.ReviewRunComplete || fresh.Verdict != verdict || fresh.Body != body || fresh.GithubReviewID != "provider-review" {
					t.Fatalf("durable replacement result = %+v, found=%t err=%v", fresh, found, err)
				}
				current, found, err := st.GetReviewBySessionAndHarness(ctx, worker.ID, worker.ReviewerHarness)
				if err != nil || !found || current.ReviewerHandleID != "ptyhost-v1:review-worker-replacement" || current.ReviewerLaunchID != runs[0].LaunchID {
					t.Fatalf("replacement authority = %+v, found=%t err=%v", current, found, err)
				}
			}
			assertResult()
			assertPending := func() {
				t.Helper()
				pending, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
				if err != nil || len(pending) != 1 {
					t.Fatalf("result persistence lost unresolved old-host cleanup: %+v, %v", pending, err)
				}
				got := pending[0]
				if got.HandleID != incumbent.ReviewerHandleID || got.LaunchID != incumbent.ReviewerLaunchID || got.ReviewID != incumbent.ID || got.RunID != runs[0].ID || got.Harness != incumbent.Harness || got.AgentSessionID != incumbent.AgentSessionID {
					t.Fatalf("pending old-host identity = %+v", got)
				}
			}
			assertPending()
			if err := engine.ReconcileReviewerCleanup(ctx, worker.ID); err == nil {
				t.Fatal("unknown old-host probe falsely settled cleanup")
			}
			assertPending()
			assertResult()
			launcher.probe = ports.FencedProbeResult{Liveness: ports.FencedAlive, Reason: ports.FencedReasonExactMatch}
			if err := engine.ReconcileReviewerCleanup(ctx, worker.ID); err == nil {
				t.Fatal("live retired host without current durable authority falsely settled cleanup")
			}
			assertPending()
			launcher.probe = ports.FencedProbeResult{Liveness: ports.FencedDead, Reason: ports.FencedReasonExactAbsent}
			for i := 0; i < 2; i++ {
				if err := engine.ReconcileReviewerCleanup(ctx, worker.ID); err != nil {
					t.Fatalf("released old-host cleanup: %v", err)
				}
			}
			pending, err := st.ListUnresolvedReviewerCleanupObligations(ctx, worker.ID)
			if err != nil || len(pending) != 0 || len(launcher.destroyed) != 1 || launcher.destroyed[0] != incumbent.ReviewerHandleID {
				t.Fatalf("released cleanup = %+v, destroys=%v, err=%v", pending, launcher.destroyed, err)
			}
			assertResult()
		})
	}
}
