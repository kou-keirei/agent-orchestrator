package review

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestCleanupMarkersKeepExactGenerationSQLite(t *testing.T) {
	for _, boundFirst := range []bool{false, true} {
		name := "unbound-first"
		if boundFirst {
			name = "bound-first"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := newSQLiteReviewStore(t)
			worker := liveWorker()
			seedReviewWorker(t, st, worker)
			handle, generation := "ptyhost-v1:review-mer-1", "old-generation"
			if err := st.UpsertReview(ctx, domain.Review{
				ID: "review-old", SessionID: worker.ID, ProjectID: worker.ProjectID, Harness: domain.ReviewerClaudeCode,
				ReviewerHandleID: "ptyhost-v1:review-mer-1-replacement", ReviewerLaunchID: "new-generation",
				CreatedAt: time.Unix(123, 0).UTC(), UpdatedAt: time.Unix(123, 0).UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			legacy := reviewerCleanupPendingMarker(handle)
			bound := reviewerCleanupPendingMarkerForGeneration(handle, generation)
			body := "teardown failed\n" + legacy + "\n" + bound
			if boundFirst {
				body = "teardown failed\n" + bound + "\n" + legacy
			}
			run := domain.ReviewRun{
				ID: "run-old", ReviewID: "review-old", SessionID: worker.ID, Harness: domain.ReviewerClaudeCode,
				PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", LaunchID: generation,
				Status: domain.ReviewRunFailed, Body: body, CreatedAt: time.Unix(123, 0).UTC(),
			}
			if err := st.InsertReviewRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			_, gotHandle, gotGeneration, ok := reviewerCleanupPendingIdentity(body)
			if !ok || gotHandle != handle || gotGeneration != generation {
				t.Errorf("marker identity = %q/%q, want %q/%q", gotHandle, gotGeneration, handle, generation)
			}
			rt := &fakeRuntime{}
			eng := newEngineForTest(st, fakeSessions{rec: worker, ok: true}, prAt("sha1"), fakeProjects{}, NewLauncher(fakeReviewerResolver{}, rt, t.TempDir()))
			for i := 0; i < 2; i++ {
				if err := eng.ReconcileReviewerCleanup(ctx, worker.ID); err != nil {
					t.Errorf("exact absent generation cleanup retry %d: %v", i, err)
				}
			}
			fresh, found, err := st.GetReviewRun(ctx, run.ID)
			if err != nil || !found || strings.Contains(fresh.Body, reviewerCleanupPendingPrefix) || fresh.Status != domain.ReviewRunFailed {
				t.Errorf("fresh marker readback = %+v, found=%t, err=%v", fresh, found, err)
			}
			if len(rt.fencedRefs) != 1 || rt.fencedRefs[0].Handle.ID != handle || rt.fencedRefs[0].SessionID != "review-mer-1" || rt.fencedRefs[0].Generation != generation || rt.destroyCalls != 0 {
				t.Errorf("runtime identity = %+v, destroys=%d", rt.fencedRefs, rt.destroyCalls)
			}
		})
	}
}

func TestCleanupMarkerIdentityBoundaries(t *testing.T) {
	bound := reviewerCleanupPendingMarkerForGeneration("exact-handle", "exact-generation")
	legacy := reviewerCleanupPendingMarker("exact-handle")
	for _, tc := range []struct {
		name, body, handle, generation string
		ok                             bool
	}{
		{name: "empty"},
		{name: "ordinary text", body: "teardown failed"},
		{name: "missing terminator", body: reviewerCleanupPendingPrefix + "exact-handle"},
		{name: "empty handle", body: reviewerCleanupPendingPrefix + "]"},
		{name: "unbound", body: legacy, handle: "exact-handle", ok: true},
		{name: "empty generation", body: reviewerCleanupPendingPrefix + "exact-handle generation: ]", handle: "exact-handle", ok: true},
		{name: "whitespace", body: reviewerCleanupPendingPrefix + " exact-handle generation: exact-generation ]", handle: "exact-handle", generation: "exact-generation", ok: true},
		{name: "duplicate", body: bound + "\n" + bound, handle: "exact-handle", generation: "exact-generation", ok: true},
		{name: "malformed before bound", body: reviewerCleanupPendingPrefix + "broken\n" + bound, handle: "exact-handle", generation: "exact-generation", ok: true},
		{name: "foreign unbound before bound", body: reviewerCleanupPendingMarker("foreign-handle") + "\n" + bound, handle: "exact-handle", generation: "exact-generation", ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, handle, generation, ok := reviewerCleanupPendingIdentity(tc.body)
			if handle != tc.handle || generation != tc.generation || ok != tc.ok {
				t.Fatalf("identity = %q/%q/%t, want %q/%q/%t", handle, generation, ok, tc.handle, tc.generation, tc.ok)
			}
		})
	}
}

func TestCleanupMarkersRetainUnprovenIdentitySQLite(t *testing.T) {
	for _, foreign := range []string{
		reviewerCleanupPendingMarker("foreign-handle"),
		reviewerCleanupPendingPrefix + "malformed-handle",
	} {
		t.Run(foreign, func(t *testing.T) {
			ctx := context.Background()
			st := newSQLiteReviewStore(t)
			worker := liveWorker()
			seedReviewWorker(t, st, worker)
			if err := st.UpsertReview(ctx, domain.Review{
				ID: "review-old", SessionID: worker.ID, ProjectID: worker.ProjectID, Harness: domain.ReviewerClaudeCode,
				CreatedAt: time.Unix(123, 0).UTC(), UpdatedAt: time.Unix(123, 0).UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			released := reviewerCleanupPendingMarkerForGeneration("released-handle", "released-generation")
			pending := reviewerCleanupPendingMarkerForGeneration("pending-handle", "pending-generation")
			legacy := reviewerCleanupPendingMarker("pending-handle")
			run := domain.ReviewRun{
				ID: "run-old", ReviewID: "review-old", SessionID: worker.ID, Harness: domain.ReviewerClaudeCode,
				PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1", LaunchID: "old-generation",
				Status: domain.ReviewRunFailed, Body: strings.Join([]string{foreign, released, pending, legacy}, "\n"), CreatedAt: time.Unix(123, 0).UTC(),
			}
			if err := st.InsertReviewRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			rt := &fakeRuntime{generationProbes: map[string]ports.FencedProbeResult{
				"pending-generation": {Liveness: ports.FencedUnknown, Reason: ports.FencedReasonProbeFailed},
			}}
			eng := newEngineForTest(st, fakeSessions{rec: worker, ok: true}, prAt("sha1"), fakeProjects{}, NewLauncher(fakeReviewerResolver{}, rt, t.TempDir()))
			if err := eng.ReconcileReviewerCleanup(ctx, worker.ID); err == nil {
				t.Fatal("unknown identity falsely settled cleanup")
			}
			fresh, found, err := st.GetReviewRun(ctx, run.ID)
			if err != nil || !found || strings.Contains(fresh.Body, released) || !strings.Contains(fresh.Body, pending) || !strings.Contains(fresh.Body, legacy) || !strings.Contains(fresh.Body, foreign) || rt.destroyCalls != 0 {
				t.Fatalf("isolated cleanup = %+v, found=%t, destroys=%d, err=%v", fresh, found, rt.destroyCalls, err)
			}
			// Releasing the known pending generation must still leave a foreign
			// unbound or malformed identity unresolved on the next retry.
			rt.generationProbes["pending-generation"] = ports.FencedProbeResult{Liveness: ports.FencedDead, Reason: ports.FencedReasonExactAbsent}
			if err := eng.ReconcileReviewerCleanup(ctx, worker.ID); err == nil {
				t.Fatal("foreign or malformed marker silently became success")
			}
			fresh, found, err = st.GetReviewRun(ctx, run.ID)
			if err != nil || !found || fresh.Body != foreign || rt.destroyCalls != 0 {
				t.Fatalf("unproven identity readback = %+v, found=%t, destroys=%d, err=%v", fresh, found, rt.destroyCalls, err)
			}
		})
	}
}
