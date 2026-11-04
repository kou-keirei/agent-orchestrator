package review

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

// Pause after submitOne has read the running run, before its result write.
// A separate SQLite store can replace the durable launch during that interval.
type resultLaunchBarrierStore struct {
	*sqlite.Store
	beforeWrite func()
}

func (s *resultLaunchBarrierStore) GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	if s.beforeWrite != nil {
		s.beforeWrite()
	}
	return s.Store.GetSession(ctx, id)
}

func TestSubmitManyRejectsReplacedLaunchSQLite(t *testing.T) {
	for _, verdict := range []domain.ReviewVerdict{domain.VerdictApproved, domain.VerdictChangesRequested} {
		for _, withSibling := range []bool{false, true} {
			name := string(verdict)
			if withSibling {
				name += "/with-current-sibling"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				dataDir := t.TempDir()
				st := sqlitetest.MustOpenAt(t, dataDir)
				other, err := sqlite.OpenPreMigrated(dataDir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = other.Close() })
				now := time.Now().UTC().Truncate(time.Second)
				worker, err := st.CreateSession(ctx, domain.SessionRecord{
					Kind: domain.KindWorker, Harness: domain.HarnessCodex,
					Activity:         domain.Activity{State: domain.ActivityActive, LastActivityAt: now},
					AutoInjectReview: true, CreatedAt: now, UpdatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				// A project row is required by review even for this isolated worker.
				if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "test", Path: t.TempDir(), RegisteredAt: now}); err != nil {
					t.Fatal(err)
				}
				owner := domain.Review{ID: "review", SessionID: worker.ID, ProjectID: "test", Harness: domain.ReviewerCodex,
					ReviewerLaunchID: "launch-A", CreatedAt: now, UpdatedAt: now}
				if err := st.UpsertReview(ctx, owner); err != nil {
					t.Fatal(err)
				}
				stale := domain.ReviewRun{ID: "run-A", ReviewID: owner.ID, SessionID: worker.ID, Harness: owner.Harness,
					BatchID: "batch", PRURL: "https://example/pr/1", TargetSHA: "sha", LaunchID: "launch-A",
					Status: domain.ReviewRunRunning, CreatedAt: now}
				if err := st.InsertReviewRun(ctx, stale); err != nil {
					t.Fatal(err)
				}
				if err := st.WriteSCMObservation(ctx, domain.PullRequest{SessionID: worker.ID, URL: stale.PRURL, HeadSHA: stale.TargetSHA}, nil, nil, nil, nil, ports.ReviewWritePreserve); err != nil {
					t.Fatal(err)
				}
				reducer := &fakeReducer{outcome: lifecycle.ReviewDeliverySent}
				barrier := &resultLaunchBarrierStore{Store: st}
				barrier.beforeWrite = func() {
					barrier.beforeWrite = nil
					owner.ReviewerLaunchID = "launch-B"
					if err := other.UpsertReview(ctx, owner); err != nil {
						t.Fatal(err)
					}
					current := stale
					current.ID, current.LaunchID, current.PRURL = "run-B", "launch-B", "https://example/pr/2"
					if err := other.InsertReviewRun(ctx, current); err != nil {
						t.Fatal(err)
					}
					if err := other.WriteSCMObservation(ctx, domain.PullRequest{SessionID: worker.ID, URL: current.PRURL, HeadSHA: current.TargetSHA}, nil, nil, nil, nil, ports.ReviewWritePreserve); err != nil {
						t.Fatal(err)
					}
				}
				svc := New(nil, barrier, WithLifecycleReducer(reducer))
				if _, err := svc.SubmitMany(ctx, "wrong-worker", []SubmittedReview{{RunID: stale.ID, Verdict: verdict, Body: "wrong owner"}}); !errors.Is(err, ErrInvalid) {
					t.Fatalf("wrong worker accepted: %v", err)
				}
				submissions := []SubmittedReview{{RunID: stale.ID, Verdict: verdict, Body: "late A", GithubReviewID: "provider-A"}}
				if withSibling {
					submissions = append(submissions, SubmittedReview{RunID: "run-B", Verdict: domain.VerdictChangesRequested, Body: "current B"})
				}
				runs, err := svc.SubmitMany(ctx, worker.ID, submissions)
				if withSibling {
					if err != nil || len(runs) != 1 || runs[0].ID != "run-B" || runs[0].Status != domain.ReviewRunDelivered {
						t.Errorf("mixed submission = %+v, %v; want only current B delivered", runs, err)
					}
				} else {
					if !errors.Is(err, ErrInvalid) || len(runs) != 0 || reducer.batchCalls != 0 {
						t.Errorf("late A = %+v, %v, delivery calls=%d; want rejection without delivery", runs, err, reducer.batchCalls)
					}
				}
				got, ok, err := other.GetReviewRun(ctx, stale.ID)
				if err != nil || !ok || got.Status != domain.ReviewRunRunning || got.Verdict != domain.VerdictNone || got.Body != "" || got.GithubReviewID != "" || got.AutoInjectReview || got.DeliveredAt != nil {
					t.Errorf("stale persisted result changed: %+v, ok=%v err=%v", got, ok, err)
				}
				if !withSibling {
					if _, err := svc.SubmitMany(ctx, worker.ID, []SubmittedReview{{RunID: "run-B", Verdict: verdict, Body: "current B"}}); err != nil {
						t.Fatalf("current B rejected: %v", err)
					}
				}
				current, ok, err := other.GetReviewRun(ctx, "run-B")
				if err != nil || !ok || current.LaunchID != "launch-B" || current.Verdict == domain.VerdictNone || current.Body != "current B" || !current.AutoInjectReview {
					t.Errorf("current B result missing: %+v, ok=%v err=%v", current, ok, err)
				}
				for _, delivered := range reducer.gotBatch {
					if delivered.RunID != "run-B" {
						t.Errorf("stale result delivered: %+v", delivered)
					}
				}
			})
		}
	}
}

func TestSubmitManyCompletedHistoricalLaunchRetry(t *testing.T) {
	st := &fakeStore{
		ok: true, reviewOK: true,
		review: domain.Review{ID: "review", SessionID: "worker", ReviewerLaunchID: "B"},
		run: domain.ReviewRun{ID: "run", ReviewID: "review", SessionID: "worker", LaunchID: "A",
			PRURL: "pr", TargetSHA: "sha", Status: domain.ReviewRunComplete,
			Verdict: domain.VerdictChangesRequested, Body: "recorded A", GithubReviewID: "provider-A", AutoInjectReview: true},
		prs: []domain.PullRequest{{URL: "pr", HeadSHA: "sha"}},
	}
	reducer := &fakeReducer{outcome: lifecycle.ReviewDeliverySent}
	svc := New(nil, st, WithLifecycleReducer(reducer))
	runs, err := svc.SubmitMany(context.Background(), "worker", []SubmittedReview{{RunID: "run", Verdict: domain.VerdictChangesRequested, Body: "recorded A"}})
	if err != nil || len(runs) != 1 || runs[0].Status != domain.ReviewRunDelivered || runs[0].LaunchID != "A" || runs[0].Body != "recorded A" || runs[0].GithubReviewID != "provider-A" || st.updateCalls != 0 {
		t.Fatalf("historical completed retry: runs=%+v err=%v updates=%d", runs, err, st.updateCalls)
	}
}
