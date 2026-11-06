package session

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

var _ FinalizeSource = (*sqlite.Store)(nil)

// Only reads are exposed; hooks simulate concurrent native writes without any
// runtime, filesystem, network, or live user database capability.
type finalizeReadFixture struct {
	records []domain.SessionRecord
	cleanup domain.SessionCleanupRecord
	present bool
	prs     []domain.PRFacts
	wts     []domain.SessionWorktreeRecord
	calls   map[string]int
	hook    func(string, int)
	fail    string
}

func (f *finalizeReadFixture) read(name string) error {
	f.calls[name]++
	if f.hook != nil {
		f.hook(name, f.calls[name])
	}
	if f.fail == name {
		return errors.New("unavailable")
	}
	return nil
}
func (f *finalizeReadFixture) ListFinalizeSessions(_ context.Context, max int) ([]domain.SessionRecord, error) {
	if max != FinalizeSourceMaxSessions {
		panic("incorrect inventory bound")
	}
	if err := f.read("inventory"); err != nil {
		return nil, err
	}
	return finalizeFixtureBounded(f.records, max)
}
func (f *finalizeReadFixture) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	err := f.read("session")
	for _, r := range f.records {
		if r.ID == id {
			return r, true, err
		}
	}
	return domain.SessionRecord{}, false, err
}
func (f *finalizeReadFixture) GetSessionCleanupFacts(_ context.Context, id domain.SessionID) (domain.SessionCleanupRecord, bool, error) {
	err := f.read("cleanup")
	c := f.cleanup
	if c.SessionID == "" && c.WorkspaceDisposition != "" {
		c.SessionID = id
	}
	return c, f.present, err
}
func (f *finalizeReadFixture) ListFinalizeWorktrees(_ context.Context, _ domain.SessionID, max int) ([]domain.SessionWorktreeRecord, error) {
	if max != FinalizeSourceMaxFacts {
		panic("incorrect worktree bound")
	}
	if err := f.read("worktrees"); err != nil {
		return nil, err
	}
	return finalizeFixtureBounded(f.wts, max)
}
func (f *finalizeReadFixture) ListFinalizePRs(_ context.Context, _ domain.SessionID, max int) ([]domain.PRFacts, error) {
	if max != FinalizeSourceMaxFacts {
		panic("incorrect PR bound")
	}
	if err := f.read("prs"); err != nil {
		return nil, err
	}
	return finalizeFixtureBounded(f.prs, max)
}

// The fixture stands in for a native bounded cursor: stop at the first excess
// record and allocate only after proving the fixture set fits. No legacy list
// method participates, including during readbacks.
func finalizeFixtureBounded[T any](records []T, max int) ([]T, error) {
	count := 0
	for range records {
		if count == max {
			return nil, errors.New("native collection limit")
		}
		count++
	}
	return slices.Clone(records), nil
}

func (*finalizeReadFixture) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	panic("unbounded inventory read")
}
func (*finalizeReadFixture) ListPRFactsForSession(context.Context, domain.SessionID) ([]domain.PRFacts, error) {
	panic("unbounded PR read")
}
func (*finalizeReadFixture) ListSessionWorktrees(context.Context, domain.SessionID) ([]domain.SessionWorktreeRecord, error) {
	panic("unbounded worktree read")
}

type finalizeGitFunc func(context.Context, FinalizeGitBinding) (FinalizeGitObservation, error)

func (f finalizeGitFunc) ObserveFinalizeWorktree(ctx context.Context, b FinalizeGitBinding) (FinalizeGitObservation, error) {
	return f(ctx, b)
}

func newFinalizeReadFixture() *finalizeReadFixture {
	return &finalizeReadFixture{
		records: []domain.SessionRecord{{ID: "exact-1", Revision: 12, CleanupGeneration: 7,
			Kind: domain.KindWorker, IsTerminated: true, Activity: domain.Activity{State: domain.ActivityExited}}},
		cleanup: domain.SessionCleanupRecord{SessionGeneration: 7,
			RuntimeReleasedAt: time.Unix(1, 0), WorkspaceDisposition: domain.DispositionPending},
		present: true, calls: make(map[string]int),
		wts: []domain.SessionWorktreeRecord{{SessionID: "exact-1", RepoName: "repo", Branch: "branch", WorktreePath: "/synthetic/never-inspected"}},
		prs: []domain.PRFacts{{SourceBranch: "branch", HeadSHA: "head", Merged: true, CI: domain.CIPassing, Review: domain.ReviewApproved}},
	}
}

func finalizeReadGit(_ context.Context, b FinalizeGitBinding) (FinalizeGitObservation, error) {
	return FinalizeGitObservation{Binding: b, Evidence: FinalizeWorktree{Record: b.Record,
		Branch: b.Record.Branch, BranchReadback: b.Record.Branch, Head: "head", HeadReadback: "head",
		OwnerKnown: FinalizeYes, Protected: FinalizeNo, Dirty: FinalizeNo, Untracked: FinalizeNo,
		UniqueCommits: FinalizeNo, LinkOrReparse: FinalizeNo}}, nil
}

func finalizeReadRow(t *testing.T, source FinalizeSource, git FinalizeGitSource) FinalizeSourceRow {
	t.Helper()
	p, err := NewFinalizePreview(context.Background(), source, git)
	if err != nil {
		t.Fatal(err)
	}
	page, err := p.Page(context.Background(), 0)
	if err != nil || page.Mode != FinalizePlanOnly || len(page.Rows) != 1 {
		t.Fatalf("page: %+v, %v", page, err)
	}
	row := page.Rows[0]
	if row.Plan.State != FinalizeHeld || row.Plan.PhysicalRemovalEligible || !slices.Contains(row.Plan.Holds,
		FinalizeHold{"owner.release_qualification", FinalizeReleaseUnqualified}) {
		t.Fatalf("escaped qualification hold: %+v", row.Plan)
	}
	return row
}

func assertFinalizeSourceHold(t *testing.T, row FinalizeSourceRow, field string, code FinalizeHoldCode) {
	t.Helper()
	if !slices.Contains(row.Plan.Holds, FinalizeHold{field, code}) {
		t.Fatalf("missing %s/%s: %+v", field, code, row.Plan.Holds)
	}
}

func TestFinalizeSourceNativeEvidenceAndUnknowns(t *testing.T) {
	f := newFinalizeReadFixture()
	row := finalizeReadRow(t, f, finalizeGitFunc(finalizeReadGit))
	s := row.Evidence
	if s.Session.ID != "exact-1" || s.Session.Revision != 12 || s.SessionRevisionReadback != 12 ||
		s.Session.CleanupGeneration != 7 || s.Cleanup.SessionGeneration != 7 || !s.CleanupPresent ||
		s.PRsComplete != FinalizeYes || s.WorktreesComplete != FinalizeYes || s.Worktrees[0].Head != "head" {
		t.Fatalf("lost native evidence: %+v", s)
	}
	if s.OwnerReleased != FinalizeUnknown || s.CleanupRevision != "" || s.CleanupRevisionReadback != "" ||
		s.PendingTurns != FinalizeUnknown || s.PendingApprovals != FinalizeUnknown || s.PendingReview != FinalizeUnknown ||
		s.PendingRepair != FinalizeUnknown || s.TaskCompleted != FinalizeUnknown || s.Backup != FinalizeUnknown ||
		s.Restore != FinalizeUnknown || s.ProviderResume != FinalizeUnknown || s.Resumable != FinalizeUnknown {
		t.Fatal("invented positive evidence from native teardown bookkeeping")
	}
	assertFinalizeSourceHold(t, row, "owner.release", FinalizeMissing)
	assertFinalizeSourceHold(t, row, "pending.review", FinalizeMissing)
	assertFinalizeSourceHold(t, row, "cleanup.revision", FinalizeMissing)
	// Results own their slices, even if the injected store reuses its buffers.
	f.prs[0].HeadSHA, f.wts[0].Branch = "mutated", "mutated"
	if s.PRs[0].HeadSHA != "head" || s.Worktrees[0].Record.Branch != "branch" {
		t.Fatal("source buffers alias returned evidence")
	}
}

func TestFinalizeSourceConflictsAndPendingFacts(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		code        FinalizeHoldCode
		change      func(*finalizeReadFixture)
	}{
		{"stale cleanup", "cleanup.generation", FinalizeGenerationMismatch, func(f *finalizeReadFixture) { f.cleanup.SessionGeneration-- }},
		{"foreign cleanup", "cleanup.session_id", FinalizeIdentityMismatch, func(f *finalizeReadFixture) { f.cleanup.SessionID = "other" }},
		{"missing cleanup", "cleanup", FinalizeMissing, func(f *finalizeReadFixture) { f.present = false; f.cleanup = domain.SessionCleanupRecord{} }},
		{"empty cleanup", "cleanup.disposition", FinalizeMissing, func(f *finalizeReadFixture) { f.cleanup = domain.SessionCleanupRecord{} }},
		{"pending PR", "prs[0].pending", FinalizeProtected, func(f *finalizeReadFixture) { f.prs[0].Merged = false }},
		{"pending CI", "prs[0].ci", FinalizeProtected, func(f *finalizeReadFixture) { f.prs[0].CI = domain.CIPending }},
		{"reviewer pending", "prs[0].review", FinalizeProtected, func(f *finalizeReadFixture) { f.prs[0].Review = domain.ReviewRequired }},
		{"owner pending", "session.activity", FinalizeProtected, func(f *finalizeReadFixture) { f.records[0].Activity.State = domain.ActivityBlocked }},
		{"unregistered legacy workspace", "worktrees.complete", FinalizeMissing, func(f *finalizeReadFixture) { f.wts = nil; f.records[0].Metadata.WorkspacePath = "/legacy" }},
		{"mid-read revision", "source.session.revision", FinalizeRevisionMismatch, func(f *finalizeReadFixture) {
			f.hook = func(n string, c int) {
				if n == "session" && c == 2 {
					f.records[0].Revision++
				}
			}
		}},
		{"mid-read generation", "source.session.generation", FinalizeGenerationMismatch, func(f *finalizeReadFixture) {
			f.hook = func(n string, c int) {
				if n == "session" && c == 2 {
					f.records[0].CleanupGeneration++
				}
			}
		}},
		{"mid-read owner", "source.session.owner", FinalizeIdentityMismatch, func(f *finalizeReadFixture) {
			f.hook = func(n string, c int) {
				if n == "session" && c == 2 {
					f.records[0].Metadata.ControllerGeneration = "replacement"
				}
			}
		}},
		{"mid-read cleanup", "source.cleanup.readback", FinalizeRevisionMismatch, func(f *finalizeReadFixture) {
			f.hook = func(n string, c int) {
				if n == "cleanup" && c == 2 {
					f.cleanup.AttemptCount++
				}
			}
		}},
		{"mid-read PR", "source.prs.readback", FinalizeRevisionMismatch, func(f *finalizeReadFixture) {
			f.hook = func(n string, c int) {
				if n == "prs" && c == 2 {
					f.prs[0].CI = domain.CIFailing
				}
			}
		}},
		{"mid-read worktree", "source.worktrees.readback", FinalizeRevisionMismatch, func(f *finalizeReadFixture) {
			f.hook = func(n string, c int) {
				if n == "worktrees" && c == 2 {
					f.wts[0].Branch = "replacement"
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinalizeReadFixture()
			tc.change(f)
			assertFinalizeSourceHold(t, finalizeReadRow(t, f, finalizeGitFunc(finalizeReadGit)), tc.field, tc.code)
		})
	}
}

func TestFinalizeSourceTrustedGitBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		code        FinalizeHoldCode
		change      func(*FinalizeGitObservation)
	}{
		{"dirty", "worktrees[0].dirty", FinalizeProtected, func(o *FinalizeGitObservation) { o.Evidence.Dirty = FinalizeYes }},
		{"untracked", "worktrees[0].untracked", FinalizeProtected, func(o *FinalizeGitObservation) { o.Evidence.Untracked = FinalizeYes }},
		{"unique HEAD", "worktrees[0].unique_commits", FinalizeProtected, func(o *FinalizeGitObservation) { o.Evidence.UniqueCommits = FinalizeYes }},
		{"symlink alias", "worktrees[0].link_or_reparse", FinalizeProtected, func(o *FinalizeGitObservation) { o.Evidence.LinkOrReparse = FinalizeYes }},
		{"ancestor link unknown", "worktrees[0].link_or_reparse", FinalizeMissing, func(o *FinalizeGitObservation) { o.Evidence.LinkOrReparse = FinalizeUnknown }},
		{"HEAD changed", "worktrees[0].git_readback", FinalizeRevisionMismatch, func(o *FinalizeGitObservation) { o.Evidence.HeadReadback = "changed" }},
		{"foreign owner", "source.git.binding", FinalizeIdentityMismatch, func(o *FinalizeGitObservation) { o.Binding.SessionID = "other" }},
		{"stale generation", "source.git.binding", FinalizeIdentityMismatch, func(o *FinalizeGitObservation) { o.Binding.Generation-- }},
		{"stale revision", "source.git.binding", FinalizeIdentityMismatch, func(o *FinalizeGitObservation) { o.Binding.Revision-- }},
		{"substituted alias", "source.git.binding", FinalizeIdentityMismatch, func(o *FinalizeGitObservation) { o.Evidence.Record.WorktreePath = "/alias" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			git := finalizeGitFunc(func(ctx context.Context, b FinalizeGitBinding) (FinalizeGitObservation, error) {
				o, err := finalizeReadGit(ctx, b)
				tc.change(&o)
				return o, err
			})
			assertFinalizeSourceHold(t, finalizeReadRow(t, newFinalizeReadFixture(), git), tc.field, tc.code)
		})
	}
	assertFinalizeSourceHold(t, finalizeReadRow(t, newFinalizeReadFixture(), nil), "worktrees[0].dirty", FinalizeMissing)
	failed := finalizeGitFunc(func(context.Context, FinalizeGitBinding) (FinalizeGitObservation, error) {
		return FinalizeGitObservation{}, errors.New("unavailable")
	})
	assertFinalizeSourceHold(t, finalizeReadRow(t, newFinalizeReadFixture(), failed), "source.git", FinalizeMissing)
}

func TestFinalizeSourcePaginationAndBounds(t *testing.T) {
	f := newFinalizeReadFixture()
	f.records, f.wts, f.prs = nil, nil, nil
	for i := 1202; i >= 0; i-- {
		f.records = append(f.records, domain.SessionRecord{ID: domain.SessionID(fmt.Sprintf("exact-%04d", i)), Revision: 1, CleanupGeneration: 7})
	}
	p, err := NewFinalizePreview(context.Background(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The inventory is frozen, even if the source reorders and adds rows.
	slices.Reverse(f.records)
	f.records = append(f.records, domain.SessionRecord{ID: "new"})
	count := 0
	for offset := 0; offset != -1; {
		page, err := p.Page(context.Background(), offset)
		if err != nil || page.Total != 1203 || len(page.Rows) > 200 || len(page.Rows) == 0 {
			t.Fatalf("page: %+v %v", page, err)
		}
		for _, row := range page.Rows {
			if row.Plan.SessionID != domain.SessionID(fmt.Sprintf("exact-%04d", count)) {
				t.Fatal("gap or duplicate")
			}
			assertFinalizeSourceHold(t, row, "owner.release_qualification", FinalizeReleaseUnqualified)
			if row.Plan.State != FinalizeHeld || row.Plan.PhysicalRemovalEligible {
				t.Fatal("not held")
			}
			count++
		}
		offset = page.NextOffset
	}
	if count != 1203 || f.calls["inventory"] != 1 || f.calls["session"] != 2406 || f.calls["prs"] != 2406 || f.calls["worktrees"] != 2406 {
		t.Fatalf("bad pagination/counts: %d %+v", count, f.calls)
	}
	for _, offset := range []int{-1, 1204} {
		if _, err := p.Page(context.Background(), offset); err == nil {
			t.Fatal("invalid offset accepted")
		}
	}
	end, err := p.Page(context.Background(), 1203)
	if err != nil || len(end.Rows) != 0 || end.NextOffset != -1 {
		t.Fatal("invalid end page")
	}
	for _, ids := range [][]domain.SessionID{{""}, {" exact"}, {"exact\x00"}, {domain.SessionID(strings.Repeat("x", 257))}, {"duplicate", "duplicate"}} {
		f.records = nil
		for _, id := range ids {
			f.records = append(f.records, domain.SessionRecord{ID: id})
		}
		if _, err := NewFinalizePreview(context.Background(), f, nil); err == nil {
			t.Fatal("invalid inventory accepted")
		}
	}
	f.records = make([]domain.SessionRecord, FinalizeSourceMaxSessions+1)
	if _, err := NewFinalizePreview(context.Background(), f, nil); err == nil {
		t.Fatal("oversized inventory accepted")
	}
	f.records = nil
	p, err = NewFinalizePreview(context.Background(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := p.Page(context.Background(), 0)
	if err != nil || empty.Total != 0 || len(empty.Rows) != 0 {
		t.Fatal("empty inventory")
	}
}

func TestFinalizeSourceFailures(t *testing.T) {
	for _, name := range []string{"session", "cleanup", "worktrees", "prs"} {
		t.Run(name, func(t *testing.T) {
			f := newFinalizeReadFixture()
			f.fail = name
			finalizeReadRow(t, f, nil)
		})
	}
	for _, name := range []string{"prs", "worktrees"} {
		f := newFinalizeReadFixture()
		if name == "prs" {
			f.prs = make([]domain.PRFacts, FinalizeSourceMaxFacts+1)
		} else {
			f.wts = make([]domain.SessionWorktreeRecord, FinalizeSourceMaxFacts+1)
		}
		assertFinalizeSourceHold(t, finalizeReadRow(t, f, nil), "source."+name, FinalizeMissing)
	}
	f := newFinalizeReadFixture()
	f.fail = "inventory"
	if _, err := NewFinalizePreview(context.Background(), f, nil); err == nil {
		t.Fatal("inventory failure ignored")
	}
	if _, err := NewFinalizePreview(context.Background(), nil, nil); err == nil {
		t.Fatal("nil source accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewFinalizePreview(ctx, newFinalizeReadFixture(), nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored")
	}
}

func TestFinalizeSourceInventoryDriftAndReadbackFailures(t *testing.T) {
	for _, name := range []string{"session", "cleanup", "prs", "worktrees"} {
		t.Run(name+" readback unavailable", func(t *testing.T) {
			f := newFinalizeReadFixture()
			f.hook = func(n string, c int) {
				if n == name && c == 2 {
					f.fail = name
				}
			}
			assertFinalizeSourceHold(t, finalizeReadRow(t, f, nil), "source."+name+".readback", FinalizeMissing)
		})
	}
	f := newFinalizeReadFixture()
	p, err := NewFinalizePreview(context.Background(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.records[0].Revision++
	f.records[0].CleanupGeneration++
	page, err := p.Page(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	assertFinalizeSourceHold(t, page.Rows[0], "source.session.revision", FinalizeRevisionMismatch)
	assertFinalizeSourceHold(t, page.Rows[0], "source.session.generation", FinalizeGenerationMismatch)
	// Deleted rows remain in the frozen page as held diagnostics.
	f.records = nil
	page, err = p.Page(context.Background(), 0)
	if err != nil || len(page.Rows) != 1 {
		t.Fatal("missing session silently dropped")
	}
	assertFinalizeSourceHold(t, page.Rows[0], "source.session", FinalizeMissing)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reads := f.calls["session"]
	if _, err := p.Page(ctx, 0); !errors.Is(err, context.Canceled) || f.calls["session"] != reads {
		t.Fatal("canceled page performed reads")
	}
}

// A legacy source exposes scalar and unbounded methods but no bounded seam.
// Embedding only the scalar interface deliberately hides bounded fixture reads.
type finalizeLegacySource struct{ FinalizeSource }

func (*finalizeLegacySource) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	panic("unbounded inventory")
}
func (*finalizeLegacySource) ListPRFactsForSession(context.Context, domain.SessionID) ([]domain.PRFacts, error) {
	panic("unbounded PRs")
}
func (*finalizeLegacySource) ListSessionWorktrees(context.Context, domain.SessionID) ([]domain.SessionWorktreeRecord, error) {
	panic("unbounded worktrees")
}

func TestFinalizeSourceNativeStoreUnavailable(t *testing.T) {
	// Even a nil native store is rejected before accessing the DB or any reads.
	// No live database, fixture writes, runtime or filesystem effects are needed.
	f := newFinalizeReadFixture()
	for _, source := range []FinalizeSource{(*sqlite.Store)(nil), &finalizeLegacySource{f}} {
		p, err := NewFinalizePreview(context.Background(), source, nil)
		if p != nil || !errors.Is(err, ErrFinalizeSourceUnavailable) || err.Error() != "SOURCE_UNAVAILABLE" {
			t.Fatalf("unsupported native source: %v %v", p, err)
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("unsupported source performed reads: %v", f.calls)
	}
}

func TestFinalizeSourceReadbackBounds(t *testing.T) {
	for _, name := range []string{"prs", "worktrees"} {
		t.Run(name, func(t *testing.T) {
			f := newFinalizeReadFixture()
			f.hook = func(n string, count int) {
				if n == name && count == 2 {
					if name == "prs" {
						f.prs = make([]domain.PRFacts, FinalizeSourceMaxFacts+1)
					} else {
						f.wts = make([]domain.SessionWorktreeRecord, FinalizeSourceMaxFacts+1)
					}
				}
			}
			assertFinalizeSourceHold(t, finalizeReadRow(t, f, nil), "source."+name+".readback", FinalizeMissing)
			if f.calls[name] != 2 {
				t.Fatalf("missing bounded readback: %v", f.calls)
			}
		})
	}
}
