package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func requireParentHold(t *testing.T, target domain.WorkItemParentTarget, err error, reason domain.WorkItemParentHoldReason) {
	t.Helper()
	var hold *domain.WorkItemParentHold
	if target != (domain.WorkItemParentTarget{}) || !errors.As(err, &hold) || hold.Reason != reason ||
		!errors.Is(err, domain.ErrWorkItemParentHeld) ||
		errors.Is(err, domain.ErrWorkItemParentBlocksPR) != (reason == domain.WorkItemParentIssuanceUnbound) {
		t.Fatalf("target=%+v error=%v; want zero target and %s", target, err, reason)
	}
}

// seedParentPersistence supplies the trusted internal adapter's observation.
// Positive live-controller acquisition itself is exercised in service/chat.
func seedParentPersistence(t *testing.T, s *sqlite.Store) (store.WorkItemRecord, domain.WorkItemParentTarget) {
	t.Helper()
	ctx := t.Context()
	seedProject(t, s, "parent-issuance")
	rec := sampleRecord("parent-issuance")
	rec.Mode = domain.SessionModeChat
	rec.Metadata.ProviderConversationID = "original-parent-native"
	rec.Metadata.ControllerGeneration = "original-generation"
	parent, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := s.CreateSession(ctx, sampleRecord("parent-issuance"))
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := s.CreateConversation(ctx, "parent-conversation", domain.ConversationScopeSession, parent.ProjectID, parent.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	branch, err := s.ConversationBranch(ctx, conversation.ID, conversation.ActiveBranchID)
	if err != nil {
		t.Fatal(err)
	}
	r := workItemRecord()
	r.Identity.ProjectID = parent.ProjectID
	r.Identity.OriginalParentSessionID = parent.ID
	r.Identity.OriginalWorkerSessionID, r.Identity.OwnerSessionID = worker.ID, worker.ID
	for _, id := range []domain.SessionID{parent.ID, worker.ID} {
		path := r.Identity.WorktreePath
		if id == parent.ID {
			path += "/parent"
		}
		if err := s.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: id, RepoName: r.Identity.Repository,
			WorktreePath: path, Branch: r.Identity.Branch, BaseSHA: r.Identity.ExecutionBaseSHA}); err != nil {
			t.Fatal(err)
		}
	}
	return r, domain.WorkItemParentTarget{WorkItemID: r.Identity.ID, AttemptID: r.Identity.AttemptID,
		OriginalParentSessionID: parent.ID, NativeSessionID: rec.Metadata.ProviderConversationID,
		ProviderConversationID: rec.Metadata.ProviderConversationID, ControllerGeneration: rec.Metadata.ControllerGeneration,
		ProviderScopeID: branch.ProviderScopeID, IssuedBySessionID: parent.ID,
		IssuedByControllerGeneration: rec.Metadata.ControllerGeneration, Provenance: domain.WorkItemParentLiveProvenance}
}

func TestWorkItemParentPositiveConcurrentRetryColdReopenAndLeaseIdentity(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, target := seedParentPersistence(t, s)
	other, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	const writers = 10
	var wg sync.WaitGroup
	errs := make([]error, writers)
	targets := make([]domain.WorkItemParentTarget, writers)
	start := make(chan struct{})
	for n := range writers {
		wg.Go(func() {
			<-start
			chosen := s
			if n%2 != 0 {
				chosen = other
			}
			targets[n], errs[n] = chosen.PersistWorkItemOriginalParent(ctx, r.Identity, r.OriginalNativeSessionID, target)
		})
	}
	close(start)
	wg.Wait()
	for n := range writers {
		if errs[n] != nil || targets[n] != target {
			t.Fatalf("writer %d: %+v %v", n, targets[n], errs[n])
		}
	}
	requireWorkItem(t, s, r)
	lease, err := s.AcquireWorkItemLease(ctx, r.Identity, domain.WorkLeaseGrant{ID: "parent-lease", Duration: time.Minute, AllowedEffects: []string{"report"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, r.Identity, lease); err != nil {
		t.Fatal(err)
	}
	report := domain.WorkerReport{ID: "positive-parent-report", Type: "checkpoint", Version: 1, Identity: r.Identity,
		LeaseID: lease.Grant.ID, LeaseRevision: lease.Revision, WorkerSessionID: r.Identity.OriginalWorkerSessionID,
		NativeSessionID: r.OriginalNativeSessionID, NativeTurnID: "worker-turn", OriginalNativeSessionID: r.OriginalNativeSessionID,
		Content: "explicit checkpoint", Digest: reportDigest("explicit checkpoint")}
	receipt, err := s.AcceptWorkerReport(ctx, report)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.PersistWorkItemOriginalParent(ctx, r.Identity, r.OriginalNativeSessionID, target)
	if err != nil || got != target {
		t.Fatalf("cold exact retry: %+v %v", got, err)
	}
	if _, err := s.SupersedeWorkItemLease(ctx, r.Identity, lease, "replacement-worker", domain.WorkLeaseGrant{ID: "replacement-lease", Duration: time.Minute}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := s.GetWorkItemOriginalParent(ctx, r.Identity.ID, r.Identity.AttemptID)
		if err != nil || got != target {
			t.Fatalf("original parent after supersession/reopen: %+v %v", got, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PersistWorkItemOriginalParent(ctx, r.Identity, r.OriginalNativeSessionID, target); !errors.Is(err, domain.ErrStaleWorkOwner) {
		t.Fatalf("stale issuance retry: %v", err)
	}
	if got, err := s.AcceptWorkerReport(ctx, report); err != nil || got != receipt {
		t.Fatalf("report retry: %+v %v", got, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events: %+v %v", events, err)
	}
	event := events[0]
	if event.ID != receipt.EventID || event.State != domain.OrchestrationPending || event.DestinationSessionID != "" || event.AttemptCount != 0 || !event.SubmittedAt.IsZero() || !event.AcknowledgedAt.IsZero() {
		t.Fatalf("parent persistence advanced delivery or ACK: %+v", event)
	}
}

func TestWorkItemParentInsertFailureRollsBackWorkAndAllowsExactRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, target := seedParentPersistence(t, s)
	db := reportDB(t, dir)
	if _, err := db.Exec(`CREATE TRIGGER reject_test_parent BEFORE INSERT ON work_item_parents BEGIN SELECT RAISE(ABORT,'test parent insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	got, err := s.PersistWorkItemOriginalParent(t.Context(), r.Identity, r.OriginalNativeSessionID, target)
	if got != (domain.WorkItemParentTarget{}) || err == nil {
		t.Fatalf("failed target committed: %+v %v", got, err)
	}
	if _, found, err := s.GetWorkItem(t.Context(), r.Identity.ID); err != nil || found {
		t.Fatalf("partial work survived: %v %v", found, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_test_parent`); err != nil {
		t.Fatal(err)
	}
	got, err = s.PersistWorkItemOriginalParent(t.Context(), r.Identity, r.OriginalNativeSessionID, target)
	if err != nil || got != target {
		t.Fatalf("exact retry after rollback: %+v %v", got, err)
	}
}

func TestWorkItemParentPersistenceRejectsForeignAndStaleWithoutCreatingWork(t *testing.T) {
	for _, tc := range []string{"generation", "provider", "scope", "parent", "repo", "workspace", "branch", "base", "project", "terminated", "worker-native-as-parent"} {
		t.Run(tc, func(t *testing.T) {
			s := newTestStore(t)
			r, target := seedParentPersistence(t, s)
			switch tc {
			case "generation":
				target.ControllerGeneration, target.IssuedByControllerGeneration = "stale", "stale"
			case "provider":
				target.ProviderConversationID, target.NativeSessionID = "foreign", "foreign"
			case "scope":
				target.ProviderScopeID = "foreign-scope"
			case "parent":
				target.OriginalParentSessionID, target.IssuedBySessionID = "foreign", "foreign"
			case "repo":
				r.Identity.Repository = "foreign"
			case "workspace":
				r.Identity.WorktreePath += "/foreign"
			case "branch":
				r.Identity.Branch += "-foreign"
			case "base":
				r.Identity.ExecutionBaseSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			case "project":
				r.Identity.ProjectID = "foreign-project"
			case "terminated":
				rec, _, _ := s.GetSession(t.Context(), r.Identity.OriginalParentSessionID)
				rec.IsTerminated = true
				if err := s.UpdateSession(t.Context(), rec); err != nil {
					t.Fatal(err)
				}
			case "worker-native-as-parent":
				target.NativeSessionID = r.OriginalNativeSessionID
			}
			got, err := s.PersistWorkItemOriginalParent(t.Context(), r.Identity, r.OriginalNativeSessionID, target)
			if err == nil || got != (domain.WorkItemParentTarget{}) {
				t.Fatalf("accepted %s: %+v %v", tc, got, err)
			}
			if _, found, err := s.GetWorkItem(t.Context(), r.Identity.ID); err != nil || found {
				t.Fatalf("rejected issuance leaked work: %v %v", found, err)
			}
		})
	}
}

func TestWorkItemParentLegacyCannotBackfillAndPositiveCannotRetarget(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		s := newTestStore(t)
		r, target := seedParentPersistence(t, s)
		if legacy {
			if _, err := s.CreateWorkItem(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			got, err := s.PersistWorkItemOriginalParent(t.Context(), r.Identity, r.OriginalNativeSessionID, target)
			requireParentHold(t, got, err, domain.WorkItemParentLegacyMissing)
			next, err := s.ClaimWorkItemOwner(t.Context(), r, "replacement-owner")
			if err != nil {
				t.Fatal(err)
			}
			got, err = s.PersistWorkItemOriginalParent(t.Context(), next.Identity, next.OriginalNativeSessionID, target)
			requireParentHold(t, got, err, domain.WorkItemParentLegacyMissing)
		} else {
			if _, err := s.PersistWorkItemOriginalParent(t.Context(), r.Identity, r.OriginalNativeSessionID, target); err != nil {
				t.Fatal(err)
			}
			replacement := target
			replacement.ControllerGeneration, replacement.IssuedByControllerGeneration = "replacement", "replacement"
			if _, err := s.PersistWorkItemOriginalParent(t.Context(), r.Identity, r.OriginalNativeSessionID, replacement); !errors.Is(err, domain.ErrInvalidWorkItemParent) {
				t.Fatalf("retarget: %v", err)
			}
			got, err := s.GetWorkItemOriginalParent(t.Context(), r.Identity.ID, r.Identity.AttemptID)
			if err != nil || got != target {
				t.Fatalf("target changed: %+v %v", got, err)
			}
		}
	}
}

func TestWorkItemParentUnboundConcurrentIssuanceCreatesNothing(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	stores := make([]*sqlite.Store, 2)
	for n := range stores {
		s, err := sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		stores[n] = s
		t.Cleanup(func() { _ = s.Close() })
	}
	r := workItemRecord()
	const writers = 12
	targets := make([]domain.WorkItemParentTarget, writers)
	errs := make([]error, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for n := range writers {
		wg.Go(func() {
			<-start
			var issuer ports.WorkItemParentIssuer = stores[n%2]
			// A worker-supplied native reference must never become parent origin.
			targets[n], errs[n] = issuer.IssueWorkItemFromOriginalParent(ctx, r.Identity, "asserted-parent-native")
		})
	}
	close(start)
	wg.Wait()
	for n := range writers {
		requireParentHold(t, targets[n], errs[n], domain.WorkItemParentIssuanceUnbound)
	}
	for _, s := range stores {
		if _, found, err := s.GetWorkItem(ctx, r.Identity.ID); err != nil || found {
			t.Fatalf("refused issuance created work: %v %v", found, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, found, err := reopened.GetWorkItem(ctx, r.Identity.ID); err != nil || found {
		t.Fatalf("cold reopen manufactured work: %v %v", found, err)
	}
	target, err := reopened.IssueWorkItemFromOriginalParent(ctx, r.Identity, r.OriginalNativeSessionID)
	requireParentHold(t, target, err, domain.WorkItemParentIssuanceUnbound)
}

func TestWorkItemParentLegacyHoldAcrossClaimsLeasesReportsAndReopen(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := seedWorkerReport(t, s)
	receipt, err := s.AcceptWorkerReport(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	work, found, err := s.GetWorkItem(ctx, r.Identity.ID)
	if err != nil || !found {
		t.Fatalf("work: %v %v", found, err)
	}
	for round := range 3 {
		var reader ports.WorkItemParentReader = s
		target, err := reader.GetWorkItemOriginalParent(ctx, r.Identity.ID, r.Identity.AttemptID)
		requireParentHold(t, target, err, domain.WorkItemParentLegacyMissing)
		// No late issuance/backfill, even while the original owner still holds it.
		target, err = s.IssueWorkItemFromOriginalParent(ctx, work.Identity, work.OriginalNativeSessionID)
		requireParentHold(t, target, err, domain.WorkItemParentIssuanceUnbound)
		got, err := s.AcceptWorkerReport(ctx, r)
		if err != nil || got != receipt {
			t.Fatalf("report retry changed: %+v %v", got, err)
		}
		if round == 0 {
			lease, found, err := s.GetWorkItemLease(ctx, work.Identity.ID)
			if err != nil || !found {
				t.Fatalf("lease: %v %v", found, err)
			}
			if _, err := s.ReleaseWorkItemLease(ctx, work.Identity, lease); err != nil {
				t.Fatal(err)
			}
			previous := work
			work, err = s.ClaimWorkItemOwner(ctx, previous, "replacement-worker")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.ClaimWorkItemOwner(ctx, previous, "replacement-worker"); !errors.Is(err, domain.ErrStaleWorkOwner) {
				t.Fatalf("duplicate claim: %v", err)
			}
		}
		requireWorkItem(t, s, work)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
	}
	// No target read/issuance may bind, submit or acknowledge the existing outbox.
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events: %+v %v", events, err)
	}
	e := events[0]
	if e.ID != receipt.EventID || e.State != domain.OrchestrationPending || e.DestinationSessionID != "" ||
		e.LeaseToken != "" || e.AttemptCount != 0 || !e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() {
		t.Fatalf("outbox advanced: %+v", e)
	}
}

func TestWorkItemParentMissingForeignAndCanceled(t *testing.T) {
	s := newTestStore(t)
	r := workItemRecord()
	if _, err := s.CreateWorkItem(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id      domain.WorkItemID
		attempt domain.WorkAttemptID
		want    error
	}{
		{"missing", r.Identity.AttemptID, store.ErrWorkItemNotFound},
		{r.Identity.ID, "foreign-attempt", domain.ErrWorkIdentityMismatch},
	} {
		target, err := s.GetWorkItemOriginalParent(t.Context(), tc.id, tc.attempt)
		if target != (domain.WorkItemParentTarget{}) || !errors.Is(err, tc.want) {
			t.Fatalf("foreign read: %+v %v", target, err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.IssueWorkItemFromOriginalParent(canceled, r.Identity, "native"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled issuance: %v", err)
	}
	if _, err := s.GetWorkItemOriginalParent(canceled, r.Identity.ID, r.Identity.AttemptID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
	r.Identity.OriginalParentSessionID = ""
	if _, err := s.IssueWorkItemFromOriginalParent(t.Context(), r.Identity, "native"); !errors.Is(err, domain.ErrInvalidWorkIdentity) {
		t.Fatalf("invalid issuance: %v", err)
	}
}
