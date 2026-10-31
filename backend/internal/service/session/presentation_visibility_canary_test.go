package session

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

// This synthetic per-ID canary does not qualify connected batch Hide, revision-
// CAS apply, or the Show Hidden UI. It never creates a workspace or provider.
func TestPresentationVisibilityCanary300Workers(t *testing.T) {
	const workers = 300
	ctx := context.Background()
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	fixture := func() (*fakeStore, []domain.SessionID) {
		st := newFakeStore()
		ids := make([]domain.SessionID, 0, workers)
		states := []domain.ActivityState{
			domain.ActivityActive, domain.ActivityIdle, domain.ActivityWaitingInput,
			domain.ActivityBlocked, domain.ActivityExited,
		}
		for i := 0; i < workers; i++ {
			id := domain.SessionID(fmt.Sprintf("synthetic-hide-canary-worker-%03d", i))
			ids = append(ids, id)
			path := fmt.Sprintf("/synthetic-hide-canary/workspaces/worker-%03d", i)
			branch := fmt.Sprintf("synthetic-hide-canary/worker-%03d", i)
			launch := fmt.Sprintf("synthetic-launch-%03d", i)
			native := fmt.Sprintf("synthetic-native-%03d", i)
			mode := domain.SessionModeTUI
			if i%2 == 0 {
				mode = domain.SessionModeChat
			}
			// SessionRecord has no hibernation flag at this base. Preserve the
			// actual lifecycle and controller ownership facts, without inventing one.
			st.sessions[id] = domain.SessionRecord{
				ID: id, ProjectID: "synthetic-hide-canary", Kind: domain.KindWorker,
				Harness: domain.HarnessCodex, Mode: mode,
				Activity:     domain.Activity{State: states[i%len(states)], LastActivityAt: now},
				IsTerminated: i%3 == 0, IsPinned: i%2 == 0,
				TerminateOnPRMerge: i%2 == 0, AutoReviewEnabled: i%3 == 0,
				AutoInjectReview: i%2 != 0, AutoInjectCI: i%3 != 0,
				CleanupGeneration: int64(i + 1), Revision: int64(i + 2), TerminalRevision: int64(i + 3),
				CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
				Metadata: domain.SessionMetadata{
					WorkspacePath: path, WorkspaceRepoPath: path, Branch: branch,
					RuntimeHandleID: fmt.Sprintf("synthetic-runtime-%03d", i),
					RuntimeLaunchID: launch, AgentSessionID: native, AgentSessionIDLaunchID: launch,
					NativeIdentityObservedAt: now, ProviderConversationID: native,
					ControllerGeneration:             fmt.Sprintf("synthetic-controller-%03d", i),
					ConversationCheckpointState:      domain.ConversationCheckpointComplete,
					ConversationCheckpointGeneration: launch, ConversationCheckpointNativeID: native,
				},
			}
			url := fmt.Sprintf("https://synthetic.invalid/hide-canary/pull/%d", i+1)
			head := fmt.Sprintf("synthetic-head-%03d", i)
			ci := domain.CIPassing
			if i%2 == 0 {
				ci = domain.CIFailing
			}
			facts := domain.PRFacts{
				URL: url, Number: i + 1, CI: ci, Review: domain.ReviewRequired,
				SourceBranch: branch, TargetBranch: "synthetic-main", HeadSHA: head,
				ReviewComments: i%2 == 0, ExternalChangesRequested: i%2 == 0, UpdatedAt: now,
			}
			st.pr[id] = facts
			st.prFacts[id] = []domain.PRFacts{facts}
			st.prs[id] = []domain.PullRequest{{
				URL: url, SessionID: id, Number: i + 1, CI: ci, Review: facts.Review,
				SourceBranch: branch, TargetBranch: facts.TargetBranch, HeadSHA: head,
			}}
			st.checks[url] = []domain.PullRequestCheck{{Name: "synthetic-check", CommitHash: head, Conclusion: "failure"}}
			st.reviews[url] = []domain.PullRequestReview{{ID: "synthetic-review", Author: "synthetic-human", State: domain.ReviewChangesRequest, TargetSHA: head}}
			st.threads[url] = []domain.PullRequestReviewThread{{ThreadID: "synthetic-thread", Path: "synthetic.go", Resolved: i%2 == 0}}
			st.comments[url] = []domain.PullRequestComment{{ID: "synthetic-comment", Author: "synthetic-human", Body: "synthetic review fact"}}
			st.reviewRuns[id] = []domain.CurrentHeadReviewRun{{SessionID: id, PRURL: url, ID: fmt.Sprintf("synthetic-review-run-%03d", i), Status: domain.ReviewRunRunning, CreatedAt: now}}
			st.worktrees[id] = []domain.SessionWorktreeRecord{{SessionID: id, RepoName: "synthetic-repo", Branch: branch, WorktreePath: path, PreservedRef: "synthetic-preserved-ref"}}
		}
		return st, ids
	}
	st, ids := fixture()
	// Build the expectation independently: no shared maps, slices, or timestamp
	// pointers can let a mutation silently rewrite its own baseline.
	wantStore, _ := fixture()
	// Nil command/provider dependencies make any unconditional call fail this
	// canary; no real manager, SCM, tracker, daemon, SQLite, or OS resource exists.
	svc := &Service{store: st, clock: func() time.Time { return now }}
	before := make(map[domain.SessionID]domain.Session, workers)
	for _, id := range ids {
		got, err := svc.Get(ctx, id)
		if err != nil {
			t.Fatalf("initial Get(%s): %v", id, err)
		}
		before[id] = got
	}
	assertStore := func(stage string) {
		t.Helper()
		got := *st
		// Read counters are instrumentation, not durable session/PR facts.
		got.listPRFactsCalls = wantStore.listPRFactsCalls
		got.listReviewRunsCalls = wantStore.listReviewRunsCalls
		if len(st.sessions) != workers || !reflect.DeepEqual(got, *wantStore) {
			t.Fatalf("%s: rows or lifecycle/resource/PR/review/CI/ownership facts changed", stage)
		}
	}
	assertView := func(stage string, id domain.SessionID, got domain.Session, hidden bool) {
		t.Helper()
		if hidden {
			if got.HiddenAt == nil || !got.HiddenAt.Equal(now) {
				t.Fatalf("%s(%s): HiddenAt = %v, want %v", stage, id, got.HiddenAt, now)
			}
		} else if got.HiddenAt != nil {
			t.Fatalf("%s(%s): HiddenAt = %v, want nil", stage, id, got.HiddenAt)
		}
		got.HiddenAt = nil
		if !reflect.DeepEqual(got, before[id]) {
			t.Fatalf("%s(%s): read model changed beyond HiddenAt", stage, id)
		}
	}
	assertStore("initial snapshot")
	for _, hidden := range []bool{true, false} {
		stage, apply := "Hide", svc.Hide
		if !hidden {
			stage, apply = "Unhide", svc.Unhide
		}
		for _, id := range ids {
			got, err := apply(ctx, id)
			if err != nil {
				t.Fatalf("%s(%s): %v", stage, id, err)
			}
			rec := wantStore.sessions[id]
			rec.HiddenAt = nil
			if hidden {
				rec.HiddenAt = &now
			}
			wantStore.sessions[id] = rec
			assertView(stage, id, got, hidden)
			direct, err := svc.Get(ctx, id)
			if err != nil {
				t.Fatalf("%s direct Get(%s): %v", stage, id, err)
			}
			assertView(stage+" direct Get", id, direct, hidden)
			assertStore(stage + " " + string(id))
		}
		// Check missing IDs with all 300 hidden, then again with all visible.
		missing := domain.SessionID("synthetic-hide-canary-missing")
		for _, operation := range []struct {
			name string
			call func(context.Context, domain.SessionID) (domain.Session, error)
		}{{"Hide", svc.Hide}, {"Unhide", svc.Unhide}, {"Get", svc.Get}} {
			_, err := operation.call(ctx, missing)
			var apiErr *apierr.Error
			if !errors.As(err, &apiErr) || apiErr.Kind != apierr.KindNotFound || apiErr.Code != "SESSION_NOT_FOUND" {
				t.Fatalf("%s missing after %s: error = %v, want typed SESSION_NOT_FOUND", operation.name, stage, err)
			}
			assertStore(operation.name + " missing after " + stage)
		}
	}
}
