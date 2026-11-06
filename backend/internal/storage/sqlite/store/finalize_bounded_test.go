package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	storepkg "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// The PR #69 interface is a structural contract; production storage must not
// import the service package. Its existing point reads are also available.
var _ interface {
	ListFinalizeSessions(context.Context, int) ([]domain.SessionRecord, error)
	ListFinalizePRs(context.Context, domain.SessionID, int) ([]domain.PRFacts, error)
	ListFinalizeWorktrees(context.Context, domain.SessionID, int) ([]domain.SessionWorktreeRecord, error)
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
	GetSessionCleanupFacts(context.Context, domain.SessionID) (domain.SessionCleanupRecord, bool, error)
} = (*sqlite.Store)(nil)

func finalizeFixtureDB(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dir)
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	seedProject(t, s, "finalize")
	return s, db
}

func finalizeExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func seedFinalizeSessions(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	// Deliberately insert in reverse order, independent of num/creation order.
	for i := count; i > 0; i-- {
		_, err := tx.Exec(`INSERT INTO sessions
		(id, project_id, num, activity_last_at, created_at, updated_at)
		VALUES (?, 'finalize', ?, ?, ?, ?)`, fmt.Sprintf("worker-%04d", i), count-i+1,
			time.Now().UTC(), time.Now().UTC(), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func requireFinalizeUnavailable(t *testing.T, err error, source string) {
	t.Helper()
	var gap *storepkg.FinalizeSourceUnavailableError
	if !errors.Is(err, storepkg.ErrFinalizeSourceUnavailable) || !errors.As(err, &gap) || gap.Source != source {
		t.Fatalf("want typed unavailable %s, got %v", source, err)
	}
}

func TestFinalizeBoundedSessionsAndPagination(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	ctx := context.Background()
	empty, err := s.ListFinalizeSessions(ctx, 1)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty inventory: %v %v", empty, err)
	}
	seedFinalizeSessions(t, db, 1203)
	got, err := s.ListFinalizeSessions(ctx, 1203)
	if err != nil || len(got) != 1203 || cap(got) != 1203 {
		t.Fatalf("exact bound: len=%d cap=%d err=%v", len(got), cap(got), err)
	}
	if _, ok, err := s.GetSessionCleanupFacts(ctx, got[0].ID); err != nil || ok {
		t.Fatalf("missing cleanup excluded or synthesized: %v %v", ok, err)
	}
	var snapshots []session.FinalizeSnapshot
	for i, r := range got {
		if want := domain.SessionID(fmt.Sprintf("worker-%04d", i+1)); r.ID != want {
			t.Fatalf("identity order: %s want %s", r.ID, want)
		}
		point, ok, err := s.GetSession(ctx, r.ID)
		if err != nil || !ok || !reflect.DeepEqual(r, point) {
			t.Fatalf("session conversion mismatch for %s: %v", r.ID, err)
		}
		snapshots = append(snapshots, session.FinalizeSnapshot{SessionID: r.ID, Session: r})
	}
	seen := 0
	for offset := 0; offset >= 0; {
		page, err := session.PlanFinalizePage(snapshots, offset)
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range page.Plans {
			if plan.SessionID != got[seen].ID || plan.State != session.FinalizeHeld || plan.PhysicalRemovalEligible ||
				!slices.Contains(plan.Holds, session.FinalizeHold{Field: "owner.release_qualification", Code: session.FinalizeReleaseUnqualified}) {
				t.Fatalf("pagination identity/release fence: %+v", plan)
			}
			seen++
		}
		offset = page.NextOffset
	}
	if seen != 1203 {
		t.Fatalf("pagination count %d", seen)
	}
	for _, bound := range []int{1, 1202} {
		rows, err := s.ListFinalizeSessions(ctx, bound)
		if rows != nil || !errors.Is(err, storepkg.ErrFinalizeSourceOverflow) {
			t.Fatalf("overflow %d: len=%d err=%v", bound, len(rows), err)
		}
		requireFinalizeUnavailable(t, err, "sessions")
	}
	if rows, err := s.ListFinalizeSessions(ctx, 10000); err != nil || len(rows) != 1203 {
		t.Fatalf("maximum accepted bound: %d %v", len(rows), err)
	}
}

func TestFinalizeBoundedFactsLimitsAndParity(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	ctx := context.Background()
	seedFinalizeSessions(t, db, 2)
	id := domain.SessionID("worker-0001")
	for i := 200; i >= 1; i-- {
		url := fmt.Sprintf("https://github.com/acme/repo/pull/%03d", i)
		finalizeExec(t, db, `INSERT INTO pr (url, session_id, number, updated_at, source_branch, target_branch, head_sha)
		VALUES (?, ?, ?, ?, 'feat/x', 'main', 'head')`, url, id, i, time.Now().UTC())
		if err := s.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{
			SessionID: id, RepoName: fmt.Sprintf("repo-%03d", 201-i), Branch: "feat/x",
			WorktreePath: fmt.Sprintf("/fixture/%03d", i), BaseSHA: "base", BaseRef: "main", PreservedRef: "refs/saved",
		}); err != nil {
			t.Fatal(err)
		}
	}
	prs, err := s.ListFinalizePRs(ctx, id, 200)
	if err != nil || len(prs) != 200 || cap(prs) != 200 {
		t.Fatalf("200 PRs: %d %v", len(prs), err)
	}
	legacy, err := s.ListPRFactsForSession(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(legacy, func(a, b domain.PRFacts) int { return strings.Compare(a.URL, b.URL) })
	if !reflect.DeepEqual(prs, legacy) {
		t.Fatal("PR projection differs from native facts")
	}
	worktrees, err := s.ListFinalizeWorktrees(ctx, id, 200)
	if err != nil || len(worktrees) != 200 || cap(worktrees) != 200 {
		t.Fatalf("200 worktrees: %d %v", len(worktrees), err)
	}
	want, err := s.ListSessionWorktrees(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(want, func(a, b domain.SessionWorktreeRecord) int { return strings.Compare(a.WorktreePath, b.WorktreePath) })
	if !reflect.DeepEqual(worktrees, want) {
		t.Fatal("worktree conversion/order mismatch")
	}
	// Overflow must be discovered before reading even the first full fact.
	finalizeExec(t, db, `UPDATE pr SET updated_at = 'invalid timestamp'`)
	finalizeExec(t, db, `UPDATE session_worktrees SET branch = '', worktree_path = ''`)
	finalizeExec(t, db, `INSERT INTO pr (url, session_id, number, updated_at) VALUES ('overflow', ?, 201, ?)`, id, time.Now().UTC())
	finalizeExec(t, db, `INSERT INTO session_worktrees (session_id, repo_name, branch, base_sha, worktree_path)
	VALUES (?, 'overflow', 'branch', '', '/fixture/201')`, id)
	if rows, err := s.ListFinalizePRs(ctx, id, 200); rows != nil || !errors.Is(err, storepkg.ErrFinalizeSourceOverflow) {
		t.Fatalf("201 PR overflow: %d %v", len(rows), err)
	}
	if rows, err := s.ListFinalizeWorktrees(ctx, id, 200); rows != nil || !errors.Is(err, storepkg.ErrFinalizeSourceOverflow) {
		t.Fatalf("201 worktree overflow: %d %v", len(rows), err)
	}
	if rows, err := s.ListFinalizePRs(ctx, "worker-0002", 1); rows == nil || len(rows) != 0 || err != nil {
		t.Fatalf("empty PRs: %v %v", rows, err)
	}
	if rows, err := s.ListFinalizeWorktrees(ctx, "worker-0002", 1); rows == nil || len(rows) != 0 || err != nil {
		t.Fatalf("empty worktrees: %v %v", rows, err)
	}
}

func TestFinalizeBoundedMinimumAndReadOnly(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	seedFinalizeSessions(t, db, 1)
	ctx := context.Background()
	id := domain.SessionID("worker-0001")
	finalizeExec(t, db, `INSERT INTO pr (url, session_id, number, updated_at) VALUES ('pr/1', ?, 1, ?)`, id, time.Now().UTC())
	finalizeExec(t, db, `INSERT INTO session_worktrees (session_id, repo_name, branch, base_sha, worktree_path)
	VALUES (?, '__root__', 'feat/x', 'base', ?)`, id, `C:\fixture\worker-0001`)
	// Give the provider a query-only pool, with no writer capability at all.
	db.SetMaxOpenConns(1)
	finalizeExec(t, db, `PRAGMA query_only = ON`)
	readOnly := storepkg.NewStore(nil, db)
	before, err := s.LatestSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if rows, err := readOnly.ListFinalizeSessions(ctx, 1); err != nil || len(rows) != 1 {
			t.Fatalf("minimum sessions: %v", err)
		}
		if rows, err := readOnly.ListFinalizePRs(ctx, id, 1); err != nil || len(rows) != 1 {
			t.Fatalf("minimum PRs: %v", err)
		}
		if rows, err := readOnly.ListFinalizeWorktrees(ctx, id, 1); err != nil || len(rows) != 1 || rows[0].WorktreePath != `C:\fixture\worker-0001` {
			t.Fatalf("minimum/native worktree path: %v %v", rows, err)
		}
	}
	if after, err := s.LatestSeq(ctx); err != nil || before != after {
		t.Fatalf("read-only provider changed CDC: %d -> %d (%v)", before, after, err)
	}
	// Cancellation while waiting for the occupied reader also releases promptly.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	blocked, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	rows, err := readOnly.ListFinalizeSessions(blocked, 1)
	if rows != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked reader cancellation: %v", err)
	}
	requireFinalizeUnavailable(t, err, "sessions")
}

func TestFinalizeBoundedPRReviewParity(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "finalize")
	r, err := s.CreateSession(ctx, sampleRecord("finalize"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := s.UpsertReview(ctx, domain.Review{ID: "review", SessionID: r.ID, ProjectID: "finalize", Harness: "claude-code", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	for i, state := range []string{"open", "draft", "merged", "closed"} {
		url := "pr/" + state
		pr := domain.PullRequest{URL: url, SessionID: r.ID, Number: i + 1,
			Draft: state == "draft", Merged: state == "merged", Closed: state == "closed",
			CI: domain.CIPassing, Review: domain.ReviewApproved, HeadSHA: "head", SourceBranch: "feat/x", TargetBranch: "main",
			UpdatedAt: now, ObservedAt: now}
		reviews := []domain.PullRequestReview{
			{ID: "old-" + state, Author: "human", State: domain.ReviewChangesRequest, TargetSHA: "old", SubmittedAt: now},
			{ID: "new-" + state, Author: "human", State: domain.ReviewApproved, TargetSHA: "head", SubmittedAt: now.Add(time.Second)},
			{ID: "bot-" + state, Author: "bot", IsBot: true, State: domain.ReviewChangesRequest, SubmittedAt: now},
			{ID: "ao-" + state, Author: "ao", State: domain.ReviewChangesRequest, SubmittedAt: now},
		}
		comments := []domain.PullRequestComment{{ID: state, ThreadID: state, ReviewID: "ao-" + state, Author: "ao", Body: "handled", CreatedAt: now}}
		if state == "open" {
			comments = append(comments, domain.PullRequestComment{ID: "human", ThreadID: state, Author: "human", Body: "fix", CreatedAt: now})
		}
		if err := s.WriteSCMObservation(ctx, pr, nil, reviews, []domain.PullRequestReviewThread{{ThreadID: state, Path: "x.go", UpdatedAt: now}}, comments, ports.ReviewWriteReplace); err != nil {
			t.Fatal(err)
		}
		if err := s.InsertReviewRun(ctx, domain.ReviewRun{ID: state, ReviewID: "review", SessionID: r.ID, Harness: "claude-code", PRURL: url,
			TargetSHA: "head", Status: domain.ReviewRunComplete, Verdict: domain.VerdictChangesRequested, GithubReviewID: "ao-" + state, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListFinalizePRs(ctx, r.ID, 4)
	if err != nil || len(got) != 4 {
		t.Fatalf("bounded reviews: %v %v", got, err)
	}
	want, err := s.ListPRFactsForSession(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(want, func(a, b domain.PRFacts) int { return strings.Compare(a.URL, b.URL) })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("review projection mismatch: %+v want %+v", got, want)
	}
	for _, pr := range got {
		if !pr.ExternalApproved || pr.ExternalChangesRequested || !pr.ReviewComments || pr.ExternalComments != (pr.URL == "pr/open") {
			t.Fatalf("external review/comment semantics: %+v", pr)
		}
	}
}

func TestFinalizeBoundedReadbacks(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	seedFinalizeSessions(t, db, 1)
	ctx := context.Background()
	before, err := s.ListFinalizeSessions(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := before[0]
	r.IsTerminated = true
	r.CleanupGeneration++
	r.Metadata.Prompt = "updated"
	if err := s.UpdateSession(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertSessionCleanupFacts(ctx, domain.SessionCleanupRecord{SessionID: r.ID, SessionGeneration: r.CleanupGeneration}); err != nil {
		t.Fatal(err)
	}
	point, ok, err := s.GetSession(ctx, r.ID)
	if err != nil || !ok || point.Revision <= before[0].Revision || point.CleanupGeneration != r.CleanupGeneration {
		t.Fatalf("revision/generation readback: %+v %v", point, err)
	}
	after, err := s.ListFinalizeSessions(ctx, 1)
	if err != nil || !reflect.DeepEqual(after[0], point) {
		t.Fatalf("fresh enumeration readback: %v", err)
	}
	cleanup, ok, err := s.GetSessionCleanupFacts(ctx, r.ID)
	if err != nil || !ok || cleanup.SessionGeneration != point.CleanupGeneration {
		t.Fatalf("cleanup readback: %+v %v", cleanup, err)
	}
}

func TestFinalizeBoundedInvalidAndUnsupported(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	seedFinalizeSessions(t, db, 1)
	ctx := context.Background()
	for _, bound := range []int{-1, 0, 10001, int(^uint(0) >> 1)} {
		rows, err := s.ListFinalizeSessions(ctx, bound)
		if rows != nil {
			t.Fatal("invalid bound returned rows")
		}
		requireFinalizeUnavailable(t, err, "sessions")
	}
	for _, bound := range []int{-1, 0, 201, int(^uint(0) >> 1)} {
		_, err := s.ListFinalizePRs(ctx, "worker-0001", bound)
		requireFinalizeUnavailable(t, err, "prs")
		_, err = s.ListFinalizeWorktrees(ctx, "worker-0001", bound)
		requireFinalizeUnavailable(t, err, "worktrees")
	}
	for _, id := range []domain.SessionID{"", " worker-0001", "worker-0001\n", "missing", "worker-0001' OR 1=1 --", domain.SessionID(strings.Repeat("x", 257))} {
		_, err := s.ListFinalizePRs(ctx, id, 1)
		requireFinalizeUnavailable(t, err, "prs")
		_, err = s.ListFinalizeWorktrees(ctx, id, 1)
		requireFinalizeUnavailable(t, err, "worktrees")
	}
	for _, id := range []string{"", " worker", "worker\n", strings.Repeat("x", 257)} {
		finalizeExec(t, db, `UPDATE sessions SET id = ?`, id)
		rows, err := s.ListFinalizeSessions(ctx, 1)
		if rows != nil {
			t.Fatal("malformed identity accepted")
		}
		requireFinalizeUnavailable(t, err, "sessions")
	}
	finalizeExec(t, db, `UPDATE sessions SET id = 'worker-0001'`)
	finalizeExec(t, db, `DROP TABLE session_worktrees`)
	_, err := s.ListFinalizeWorktrees(ctx, "worker-0001", 1)
	requireFinalizeUnavailable(t, err, "worktrees")
	finalizeExec(t, db, `DROP TABLE pr`)
	_, err = s.ListFinalizePRs(ctx, "worker-0001", 1)
	requireFinalizeUnavailable(t, err, "prs")
}

func TestFinalizeBoundedMalformedFacts(t *testing.T) {
	for _, paths := range [][]string{{""}, {"relative"}, {"/x/../alias"}, {"/x/"}, {"/x//y"}, {" /x"}, {"/x", "/x"}, {`C:\X`, `c:/x`}, {"/valid", "/bad/./path"}} {
		t.Run(fmt.Sprint(paths), func(t *testing.T) {
			s, db := finalizeFixtureDB(t)
			seedFinalizeSessions(t, db, 1)
			for i, path := range paths {
				finalizeExec(t, db, `INSERT INTO session_worktrees (session_id, repo_name, branch, base_sha, worktree_path)
				VALUES ('worker-0001', ?, 'feat/x', '', ?)`, fmt.Sprint(i), path)
			}
			rows, err := s.ListFinalizeWorktrees(context.Background(), "worker-0001", 200)
			if rows != nil {
				t.Fatal("malformed/duplicate path returned records")
			}
			requireFinalizeUnavailable(t, err, "worktrees")
		})
	}
	for _, value := range []struct {
		url    string
		number int
	}{{"", 1}, {" alias", 1}, {"pr\n", 1}, {"pr", 0}} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			s, db := finalizeFixtureDB(t)
			seedFinalizeSessions(t, db, 1)
			finalizeExec(t, db, `INSERT INTO pr (url, session_id, number, updated_at) VALUES (?, 'worker-0001', ?, ?)`, value.url, value.number, time.Now().UTC())
			rows, err := s.ListFinalizePRs(context.Background(), "worker-0001", 1)
			if rows != nil {
				t.Fatal("malformed PR returned records")
			}
			requireFinalizeUnavailable(t, err, "prs")
		})
	}
}

func TestFinalizeBoundedCancellationAndSQLFailures(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	seedFinalizeSessions(t, db, 1)
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		ctx, cancel := context.WithCancel(context.Background())
		if errors.Is(cause, context.DeadlineExceeded) {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		} else {
			cancel()
		}
		_, err := s.ListFinalizeSessions(ctx, 1)
		if !errors.Is(err, cause) {
			t.Fatalf("context cause lost: %v", err)
		}
		requireFinalizeUnavailable(t, err, "sessions")
		_, err = s.ListFinalizePRs(ctx, "worker-0001", 1)
		if !errors.Is(err, cause) {
			t.Fatalf("PR context cause lost: %v", err)
		}
		_, err = s.ListFinalizeWorktrees(ctx, "worker-0001", 1)
		if !errors.Is(err, cause) {
			t.Fatalf("worktree context cause lost: %v", err)
		}
		cancel()
	}
	// A bad field scan must fail the whole inventory, then release its reader.
	finalizeExec(t, db, `UPDATE sessions SET created_at = 'invalid timestamp'`)
	if rows, err := s.ListFinalizeSessions(context.Background(), 1); rows != nil || err == nil {
		t.Fatal("scan failure accepted")
	}
	finalizeExec(t, db, `UPDATE sessions SET created_at = ?`, time.Now().UTC())
	if rows, err := s.ListFinalizeSessions(context.Background(), 1); err != nil || len(rows) != 1 {
		t.Fatalf("reader leaked after failure: %v", err)
	}
	missing, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "missing-schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = missing.Close() }()
	unavailable := storepkg.NewStore(nil, missing)
	_, err = unavailable.ListFinalizeSessions(context.Background(), 1)
	requireFinalizeUnavailable(t, err, "sessions")
	_ = missing.Close()
	_, err = unavailable.ListFinalizeSessions(context.Background(), 1)
	requireFinalizeUnavailable(t, err, "sessions")
	var absent *storepkg.Store
	_, err = absent.ListFinalizeSessions(context.Background(), 1)
	requireFinalizeUnavailable(t, err, "sessions")
}

func TestFinalizeBoundedOverflowBeforeHydrationAndMemory(t *testing.T) {
	s, db := finalizeFixtureDB(t)
	seedFinalizeSessions(t, db, 1203)
	// If hydration runs before overflow detection, this invalid timestamp would
	// produce a scan error and this 2 MiB field would be copied into Go memory.
	finalizeExec(t, db, `UPDATE sessions SET prompt = ?, created_at = 'invalid' WHERE id = 'worker-0001'`, strings.Repeat("x", 2<<20))
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < 3; i++ {
		rows, err := s.ListFinalizeSessions(context.Background(), 1)
		if rows != nil || !errors.Is(err, storepkg.ErrFinalizeSourceOverflow) {
			t.Fatalf("hydrated before overflow: %v", err)
		}
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("overflow retained/materialized payloads: allocated %d bytes", allocated)
	}
	t.Logf("1203-row overflow, bound=1, three reads allocated %d bytes; 2 MiB payload not hydrated", after.TotalAlloc-before.TotalAlloc)
}

func TestFinalizeBoundedNoUnboundedSourceCalls(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "finalize_bounded.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range f.Imports {
		if strings.Contains(imp.Path.Value, "/service/") {
			t.Fatal("storage imports service")
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			switch selector.Sel.Name {
			case "ListAllSessions", "ListPRFactsForSession", "ListSessionWorktrees", "Exec", "ExecContext", "Commit":
				t.Errorf("forbidden unbounded/writer call: %s", selector.Sel.Name)
			}
		}
		return true
	})
}
