package session

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// All positive observations here are synthetic, not qualification of native #20.
func finalizeFixture() FinalizeSnapshot {
	return FinalizeSnapshot{
		SessionID: "worker-1",
		Session: domain.SessionRecord{ID: "worker-1", Kind: domain.KindWorker,
			IsTerminated: true, CleanupGeneration: 7, Revision: 12,
			Activity: domain.Activity{State: domain.ActivityExited}},
		SessionRevisionReadback: 12,
		Cleanup: domain.SessionCleanupRecord{SessionID: "worker-1", SessionGeneration: 7,
			RuntimeReleasedAt: time.Unix(1, 0), WorkspaceDisposition: domain.DispositionPending},
		CleanupPresent: true, CleanupRevision: "cleanup-r1", CleanupRevisionReadback: "cleanup-r1",
		TaskCompleted: FinalizeYes, PendingTurns: FinalizeNo, PendingApprovals: FinalizeNo,
		PendingReview: FinalizeNo, PendingRepair: FinalizeNo, Protected: FinalizeNo,
		Resumable: FinalizeNo, ProviderResume: FinalizeYes, Backup: FinalizeYes, Restore: FinalizeYes,
		OwnerReleased: FinalizeYes, PRsComplete: FinalizeYes, WorktreesComplete: FinalizeYes,
		PRs: []domain.PRFacts{{Merged: true, CI: domain.CIPassing, Review: domain.ReviewApproved,
			SourceBranch: "ao/worker-1/root", HeadSHA: "c8f8438471b1e1586bd4637a87af896c2d92fdd5"}},
		Worktrees: []FinalizeWorktree{{Record: domain.SessionWorktreeRecord{
			SessionID: "worker-1", Branch: "ao/worker-1/root", WorktreePath: "/synthetic/worktree"},
			Branch: "ao/worker-1/root", BranchReadback: "ao/worker-1/root",
			Head: "c8f8438471b1e1586bd4637a87af896c2d92fdd5", HeadReadback: "c8f8438471b1e1586bd4637a87af896c2d92fdd5",
			OwnerKnown: FinalizeYes, Protected: FinalizeNo, Dirty: FinalizeNo,
			Untracked: FinalizeNo, UniqueCommits: FinalizeNo, LinkOrReparse: FinalizeNo}},
	}
}

func requireFinalizeHeld(t *testing.T, p FinalizePlan) {
	t.Helper()
	if p.State != FinalizeHeld || p.PhysicalRemovalEligible || !slices.Contains(p.Holds,
		FinalizeHold{"owner.release_qualification", FinalizeReleaseUnqualified}) {
		t.Fatalf("plan escaped permanent hold: %+v", p)
	}
}

func TestFinalizePlanHolds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*FinalizeSnapshot)
		field  string
		code   FinalizeHoldCode
	}{
		{"idle is not completed", func(s *FinalizeSnapshot) {
			s.Session.Activity.State = domain.ActivityIdle
			s.TaskCompleted = FinalizeNo
		}, "task.completed", FinalizeProtected},
		{"live idle", func(s *FinalizeSnapshot) {
			s.Session.Activity.State = domain.ActivityIdle
			s.Session.IsTerminated = false
		}, "session.terminated", FinalizeProtected},
		{"active", func(s *FinalizeSnapshot) { s.Session.Activity.State = domain.ActivityActive }, "session.activity", FinalizeProtected},
		{"pending turn", func(s *FinalizeSnapshot) { s.PendingTurns = FinalizeYes }, "pending.turns", FinalizeProtected},
		{"approval", func(s *FinalizeSnapshot) { s.PendingApprovals = FinalizeYes }, "pending.approvals", FinalizeProtected},
		{"review", func(s *FinalizeSnapshot) { s.PendingReview = FinalizeYes }, "pending.review", FinalizeProtected},
		{"repair", func(s *FinalizeSnapshot) { s.PendingRepair = FinalizeYes }, "pending.repair", FinalizeProtected},
		{"open PR", func(s *FinalizeSnapshot) { s.PRs[0].Merged = false }, "prs[0].pending", FinalizeProtected},
		{"CI pending", func(s *FinalizeSnapshot) { s.PRs[0].CI = domain.CIPending }, "prs[0].ci", FinalizeProtected},
		{"CI failed", func(s *FinalizeSnapshot) { s.PRs[0].CI = domain.CIFailing }, "prs[0].ci", FinalizeProtected},
		{"CI unknown", func(s *FinalizeSnapshot) { s.PRs[0].CI = domain.CIUnknown }, "prs[0].ci", FinalizeMissing},
		{"PR comments", func(s *FinalizeSnapshot) { s.PRs[0].ReviewComments = true }, "prs[0].review", FinalizeProtected},
		{"PR head missing", func(s *FinalizeSnapshot) { s.PRs[0].HeadSHA = "" }, "prs[0].git_identity", FinalizeMissing},
		{"PR head stale", func(s *FinalizeSnapshot) { s.PRs[0].HeadSHA = "old-head" }, "prs[0].git_identity", FinalizeIdentityMismatch},
		{"PR branch foreign", func(s *FinalizeSnapshot) { s.PRs[0].SourceBranch = "foreign" }, "prs[0].git_identity", FinalizeIdentityMismatch},
		{"PR review unknown", func(s *FinalizeSnapshot) { s.PRs[0].Review = "" }, "prs[0].review", FinalizeMissing},
		{"PR enumeration partial", func(s *FinalizeSnapshot) { s.PRsComplete = FinalizeUnknown }, "prs.complete", FinalizeMissing},
		{"worktree enumeration partial", func(s *FinalizeSnapshot) { s.WorktreesComplete = FinalizeUnknown }, "worktrees.complete", FinalizeMissing},
		{"dirty", func(s *FinalizeSnapshot) { s.Worktrees[0].Dirty = FinalizeYes }, "worktrees[0].dirty", FinalizeProtected},
		{"untracked", func(s *FinalizeSnapshot) { s.Worktrees[0].Untracked = FinalizeYes }, "worktrees[0].untracked", FinalizeProtected},
		{"unique commits", func(s *FinalizeSnapshot) { s.Worktrees[0].UniqueCommits = FinalizeYes }, "worktrees[0].unique_commits", FinalizeProtected},
		{"symlink or reparse", func(s *FinalizeSnapshot) { s.Worktrees[0].LinkOrReparse = FinalizeYes }, "worktrees[0].link_or_reparse", FinalizeProtected},
		{"link inspection missing", func(s *FinalizeSnapshot) { s.Worktrees[0].LinkOrReparse = FinalizeUnknown }, "worktrees[0].link_or_reparse", FinalizeMissing},
		{"owner unknown", func(s *FinalizeSnapshot) { s.Worktrees[0].OwnerKnown = FinalizeUnknown }, "worktrees[0].owner", FinalizeMissing},
		{"release unknown", func(s *FinalizeSnapshot) { s.OwnerReleased = FinalizeUnknown }, "owner.release", FinalizeMissing},
		{"active owner", func(s *FinalizeSnapshot) { s.OwnerReleased = FinalizeNo }, "owner.release", FinalizeProtected},
		{"provider resume missing", func(s *FinalizeSnapshot) { s.ProviderResume = FinalizeUnknown }, "provider.resume", FinalizeMissing},
		{"backup missing", func(s *FinalizeSnapshot) { s.Backup = FinalizeUnknown }, "backup", FinalizeMissing},
		{"restore missing", func(s *FinalizeSnapshot) { s.Restore = FinalizeUnknown }, "restore", FinalizeMissing},
		{"resumable", func(s *FinalizeSnapshot) { s.Resumable = FinalizeYes }, "session.resumable", FinalizeProtected},
		{"pinned", func(s *FinalizeSnapshot) { s.Session.IsPinned = true }, "session.pinned", FinalizeProtected},
		{"protected", func(s *FinalizeSnapshot) { s.Protected = FinalizeYes }, "session.protected", FinalizeProtected},
		{"protected worktree", func(s *FinalizeSnapshot) { s.Worktrees[0].Protected = FinalizeYes }, "worktrees[0].protected", FinalizeProtected},
		{"preserved ref", func(s *FinalizeSnapshot) { s.Worktrees[0].Record.PreservedRef = "refs/ao/saved" }, "worktrees[0].preserved_ref", FinalizeProtected},
		{"stale generation", func(s *FinalizeSnapshot) { s.Cleanup.SessionGeneration-- }, "cleanup.generation", FinalizeGenerationMismatch},
		{"future generation", func(s *FinalizeSnapshot) { s.Cleanup.SessionGeneration++ }, "cleanup.generation", FinalizeGenerationMismatch},
		{"session mismatch", func(s *FinalizeSnapshot) { s.Session.ID = "worker-2" }, "session.id", FinalizeIdentityMismatch},
		{"cleanup mismatch", func(s *FinalizeSnapshot) { s.Cleanup.SessionID = "worker-2" }, "cleanup.session_id", FinalizeIdentityMismatch},
		{"worktree mismatch", func(s *FinalizeSnapshot) { s.Worktrees[0].Record.SessionID = "worker-2" }, "worktrees[0].owner", FinalizeIdentityMismatch},
		{"branch mismatch", func(s *FinalizeSnapshot) { s.Worktrees[0].Branch = "main" }, "worktrees[0].branch", FinalizeIdentityMismatch},
		{"Git changed", func(s *FinalizeSnapshot) { s.Worktrees[0].HeadReadback = "different" }, "worktrees[0].git_readback", FinalizeRevisionMismatch},
		{"head missing", func(s *FinalizeSnapshot) { s.Worktrees[0].Head = "" }, "worktrees[0].head", FinalizeMissing},
		{"session revision changed", func(s *FinalizeSnapshot) { s.SessionRevisionReadback++ }, "session.revision", FinalizeRevisionMismatch},
		{"cleanup revision changed", func(s *FinalizeSnapshot) { s.CleanupRevisionReadback = "r2" }, "cleanup.revision", FinalizeRevisionMismatch},
		{"cleanup revision unavailable", func(s *FinalizeSnapshot) { s.CleanupRevision = "" }, "cleanup.revision", FinalizeMissing},
		{"cleanup absent", func(s *FinalizeSnapshot) { s.CleanupPresent = false }, "cleanup", FinalizeMissing},
		{"runtime release missing", func(s *FinalizeSnapshot) { s.Cleanup.RuntimeReleasedAt = time.Time{} }, "cleanup.runtime_release", FinalizeMissing},
		{"dirty disposition", func(s *FinalizeSnapshot) { s.Cleanup.WorkspaceDisposition = domain.DispositionPreservedDirty }, "cleanup.disposition", FinalizeProtected},
		{"orchestrator", func(s *FinalizeSnapshot) { s.Session.Kind = domain.KindOrchestrator }, "session.kind", FinalizeProtected},
		{"invalid fact", func(s *FinalizeSnapshot) { s.Backup = FinalizeFact(255) }, "backup", FinalizeInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := finalizeFixture()
			tc.change(&s)
			p := PlanFinalize(s)
			requireFinalizeHeld(t, p)
			if !slices.Contains(p.Holds, FinalizeHold{tc.field, tc.code}) {
				t.Fatalf("missing %s/%s: %+v", tc.field, tc.code, p.Holds)
			}
		})
	}
}

func TestFinalizePlanAlwaysHeldAndPreservesFences(t *testing.T) {
	for _, disposition := range []domain.WorkspaceDisposition{domain.DispositionPending, domain.DispositionRemoved, domain.DispositionNotApplicable} {
		s := finalizeFixture()
		s.Cleanup.WorkspaceDisposition = disposition
		p := PlanFinalize(s)
		requireFinalizeHeld(t, p)
		if len(p.Holds) != 1 {
			t.Fatalf("completed fixture: %+v", p.Holds)
		}
		if p.Generation != 7 || p.Revision != 12 || p.SessionRevisionReadback != 12 || p.Cleanup != s.Cleanup ||
			!p.CleanupPresent || p.CleanupRevision != s.CleanupRevision || p.CleanupRevisionReadback != s.CleanupRevisionReadback ||
			!reflect.DeepEqual(p.Worktrees, s.Worktrees) {
			t.Fatalf("lost identity fences: %+v", p)
		}
	}
	p := PlanFinalize(FinalizeSnapshot{})
	requireFinalizeHeld(t, p)
	if len(p.Holds) < 10 {
		t.Fatalf("zero evidence must retain diagnostic holds: %+v", p)
	}
}

func TestFinalizePlanPureAndOwnsOutput(t *testing.T) {
	s := finalizeFixture()
	before := finalizeFixture()
	first, second := PlanFinalize(s), PlanFinalize(s)
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(s, before) {
		t.Fatal("nondeterminism or input mutation")
	}
	first.Worktrees[0].Record.Branch = "changed"
	first.Holds[0].Field = "changed"
	if !reflect.DeepEqual(s, before) || reflect.DeepEqual(first, second) {
		t.Fatal("output aliases input or another result")
	}
	// Impossible paths are deliberate: this kernel neither stats paths nor
	// invokes Git/runtime/SQL. Side effects are unavailable in its call graph.
	s.Worktrees[0].Record.WorktreePath = "does-not-exist/\x00/no-io"
	requireFinalizeHeld(t, PlanFinalize(s))
}

func TestFinalizePlanPaginationBeyond1000(t *testing.T) {
	var snapshots []FinalizeSnapshot
	for i := 1202; i >= 0; i-- {
		s := finalizeFixture()
		s.SessionID = domain.SessionID(fmt.Sprintf("worker-%04d", i))
		s.Session.ID, s.Cleanup.SessionID, s.Worktrees[0].Record.SessionID = s.SessionID, s.SessionID, s.SessionID
		snapshots = append(snapshots, s)
	}
	before := slices.Clone(snapshots)
	count := 0
	for offset := 0; offset != -1; {
		page, err := PlanFinalizePage(snapshots, offset)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 1203 || len(page.Plans) > FinalizePlanPageSize || len(page.Plans) == 0 {
			t.Fatalf("bad page: %+v", page)
		}
		for _, p := range page.Plans {
			requireFinalizeHeld(t, p)
			if p.SessionID != domain.SessionID(fmt.Sprintf("worker-%04d", count)) {
				t.Fatalf("gap or duplicate at %d: %s", count, p.SessionID)
			}
			count++
		}
		if page.NextOffset != -1 && page.NextOffset != count {
			t.Fatalf("bad cursor: %+v", page)
		}
		offset = page.NextOffset
	}
	if count != 1203 || !reflect.DeepEqual(snapshots, before) {
		t.Fatal("pagination lost rows or changed inputs")
	}
	end, err := PlanFinalizePage(snapshots, len(snapshots))
	if err != nil || len(end.Plans) != 0 || end.NextOffset != -1 {
		t.Fatalf("end page: %+v, %v", end, err)
	}
	for _, offset := range []int{-1, len(snapshots) + 1} {
		if _, err := PlanFinalizePage(snapshots, offset); err == nil {
			t.Fatal("invalid offset accepted")
		}
	}
	for _, id := range []domain.SessionID{"", " worker", snapshots[0].SessionID} {
		bad := slices.Clone(snapshots)
		bad[len(bad)-1].SessionID = id
		if _, err := PlanFinalizePage(bad, 0); err == nil {
			t.Fatal("invalid ID beyond page accepted")
		}
	}
	empty, err := PlanFinalizePage(nil, 0)
	if err != nil || empty.Total != 0 || empty.NextOffset != -1 {
		t.Fatalf("empty page: %+v, %v", empty, err)
	}
}
