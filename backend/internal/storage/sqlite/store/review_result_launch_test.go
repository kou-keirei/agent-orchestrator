package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestUpdateReviewRunResultRequiresCurrentOwnerLaunch(t *testing.T) {
	for _, tc := range []struct {
		name, reviewLaunch, runLaunch           string
		wrongSession, wrongReview, wrongHarness bool
		wantUpdated                             bool
	}{
		{name: "current launch", reviewLaunch: "B", runLaunch: "B", wantUpdated: true},
		{name: "stale launch", reviewLaunch: "B", runLaunch: "A"},
		{name: "legacy both empty", wantUpdated: true},
		{name: "legacy run against claimed owner", reviewLaunch: "B"},
		{name: "claimed run against legacy owner", runLaunch: "A"},
		{name: "wrong worker session", reviewLaunch: "B", runLaunch: "B", wrongSession: true},
		{name: "wrong review id same launch", reviewLaunch: "B", runLaunch: "B", wrongReview: true},
		{name: "wrong harness same launch", reviewLaunch: "B", runLaunch: "B", wrongHarness: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			s := newTestStore(t)
			seedProject(t, s, "mer")
			worker, err := s.CreateSession(ctx, sampleRecord("mer"))
			if err != nil {
				t.Fatal(err)
			}
			other, err := s.CreateSession(ctx, sampleRecord("mer"))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			owner := domain.Review{ID: "owner", SessionID: worker.ID, ProjectID: "mer", Harness: domain.ReviewerCodex,
				ReviewerLaunchID: tc.reviewLaunch, CreatedAt: now, UpdatedAt: now}
			if err := s.UpsertReview(ctx, owner); err != nil {
				t.Fatal(err)
			}
			otherOwner := owner
			otherOwner.ID, otherOwner.SessionID = "other-owner", other.ID
			if err := s.UpsertReview(ctx, otherOwner); err != nil {
				t.Fatal(err)
			}
			run := domain.ReviewRun{ID: "run", ReviewID: owner.ID, SessionID: worker.ID, Harness: owner.Harness,
				LaunchID: tc.runLaunch, Status: domain.ReviewRunRunning, CreatedAt: now}
			if tc.wrongSession {
				run.SessionID = other.ID
			}
			if tc.wrongReview {
				run.ReviewID = otherOwner.ID
			}
			if tc.wrongHarness {
				run.Harness = domain.ReviewerClaudeCode
			}
			if err := s.InsertReviewRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			updated, err := s.UpdateReviewRunResult(ctx, run.ID, domain.ReviewRunComplete, domain.VerdictApproved, "result", "provider-review", true)
			if err != nil || updated != tc.wantUpdated {
				t.Errorf("updated=%v err=%v, want %v", updated, err, tc.wantUpdated)
			}
			got, ok, err := s.GetReviewRun(ctx, run.ID)
			if err != nil || !ok {
				t.Fatalf("get: ok=%v err=%v", ok, err)
			}
			if !tc.wantUpdated {
				if got.Status != run.Status || got.Verdict != run.Verdict || got.Body != run.Body || got.GithubReviewID != "" || got.AutoInjectReview {
					t.Fatalf("rejected mutation changed durable result: %+v", got)
				}
			} else if got.Status != domain.ReviewRunComplete || got.Verdict != domain.VerdictApproved || got.Body != "result" || got.GithubReviewID != "provider-review" || !got.AutoInjectReview {
				t.Fatalf("accepted mutation missing durable fields: %+v", got)
			}
		})
	}
}

// Launch failures can be settled before the new reviewer is claimed, and
// replacement cancels predecessors after the new reviewer is claimed. Neither
// operation may promote a verdict, but both must retain their lifecycle result.
func TestUpdateReviewRunResultAllowsNonAuthoritativeSettlement(t *testing.T) {
	for _, status := range []domain.ReviewRunStatus{domain.ReviewRunFailed, domain.ReviewRunCancelled} {
		t.Run(string(status), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			s := newTestStore(t)
			seedProject(t, s, "mer")
			worker, err := s.CreateSession(ctx, sampleRecord("mer"))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			owner := domain.Review{ID: "owner", SessionID: worker.ID, ProjectID: "mer", Harness: domain.ReviewerCodex,
				ReviewerLaunchID: "B", CreatedAt: now, UpdatedAt: now}
			if err := s.UpsertReview(ctx, owner); err != nil {
				t.Fatal(err)
			}
			run := domain.ReviewRun{ID: "run", ReviewID: owner.ID, SessionID: worker.ID, Harness: owner.Harness,
				LaunchID: "A", Status: domain.ReviewRunRunning, CreatedAt: now}
			if err := s.InsertReviewRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			// A terminal label must never be used to smuggle in a fresh verdict.
			if updated, err := s.UpdateReviewRunResult(ctx, run.ID, status, domain.VerdictApproved, "false verdict", "provider-review", true); err != nil || updated {
				t.Errorf("stale verdict under %s: updated=%v err=%v", status, updated, err)
			}
			if updated, err := s.UpdateReviewRunResult(ctx, run.ID, status, domain.VerdictNone, "settled", "", false); err != nil || !updated {
				t.Errorf("settlement under %s: updated=%v err=%v", status, updated, err)
			}
			got, ok, err := s.GetReviewRun(ctx, run.ID)
			if err != nil || !ok || got.Status != status || got.Verdict != domain.VerdictNone || got.Body != "settled" {
				t.Fatalf("settlement result=%+v ok=%v err=%v", got, ok, err)
			}
		})
	}
}
