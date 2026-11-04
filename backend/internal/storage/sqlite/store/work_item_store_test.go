package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func workItemRecord() store.WorkItemRecord {
	return store.WorkItemRecord{
		Identity: domain.WorkItemIdentity{
			ID: "work-1", AttemptID: "attempt-1", ProjectID: "project-1",
			Repository: domain.RootWorkspaceRepoName, ExecutionBaseSHA: strings.Repeat("a", 40),
			OriginalParentSessionID: "parent-1", OriginalWorkerSessionID: "worker-1",
			OwnerSessionID: "worker-1", OwnerGeneration: 1,
			WorktreePath: "C:/ao/worktrees/worker-1", Branch: "ao/worker-1/root",
		},
		OriginalNativeSessionID: "native-1",
	}
}

func requireWorkItem(t *testing.T, s *sqlite.Store, want store.WorkItemRecord) {
	t.Helper()
	got, found, err := s.GetWorkItem(context.Background(), want.Identity.ID)
	if err != nil || !found || got != want {
		t.Fatalf("get = %+v, %v, %v; want %+v", got, found, err, want)
	}
}

func TestWorkItemRestartAndStaleRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	original := workItemRecord()
	for range 2 {
		if got, err := s.CreateWorkItem(ctx, original); err != nil || got != original {
			t.Fatalf("create/retry = %+v, %v", got, err)
		}
	}
	next := original
	next.Identity.OwnerSessionID, next.Identity.OwnerGeneration = "worker-2", 2
	if got, err := s.ClaimWorkItemOwner(ctx, original, "worker-2"); err != nil || got != next {
		t.Fatalf("claim = %+v, %v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	requireWorkItem(t, s, next)
	if got, err := s.CreateWorkItem(ctx, original); !errors.Is(err, domain.ErrStaleWorkOwner) || got != next {
		t.Fatalf("stale create = %+v, %v", got, err)
	}
	for _, owner := range []domain.SessionID{"worker-2", "foreign-worker"} {
		if got, err := s.ClaimWorkItemOwner(ctx, original, owner); !errors.Is(err, domain.ErrStaleWorkOwner) || got != next {
			t.Fatalf("stale claim/retry = %+v, %v", got, err)
		}
	}
	// Returning to the original owner still advances the fence; old credentials
	// do not become current again (ABA).
	third := next
	third.Identity.OwnerSessionID, third.Identity.OwnerGeneration = "worker-1", 3
	if got, err := s.ClaimWorkItemOwner(ctx, next, "worker-1"); err != nil || got != third {
		t.Fatalf("second claim = %+v, %v", got, err)
	}
	if got, err := s.ClaimWorkItemOwner(ctx, original, "worker-3"); !errors.Is(err, domain.ErrStaleWorkOwner) || got != third {
		t.Fatalf("ABA claim = %+v, %v", got, err)
	}
	requireWorkItem(t, s, third)
}

func TestWorkItemClaimExactBinding(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	original := workItemRecord()
	if _, err := s.CreateWorkItem(ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*store.WorkItemRecord)
	}{
		{"attempt", func(r *store.WorkItemRecord) { r.Identity.AttemptID = "other" }},
		{"project", func(r *store.WorkItemRecord) { r.Identity.ProjectID = "other" }},
		{"repository", func(r *store.WorkItemRecord) { r.Identity.Repository = "other" }},
		{"candidate HEAD as base", func(r *store.WorkItemRecord) { r.Identity.ExecutionBaseSHA = strings.Repeat("b", 40) }},
		{"parent", func(r *store.WorkItemRecord) { r.Identity.OriginalParentSessionID = "other" }},
		{"worker", func(r *store.WorkItemRecord) { r.Identity.OriginalWorkerSessionID = "other" }},
		{"native session", func(r *store.WorkItemRecord) { r.OriginalNativeSessionID = "other" }},
		{"foreign owner", func(r *store.WorkItemRecord) { r.Identity.OwnerSessionID = "other" }},
		{"future generation", func(r *store.WorkItemRecord) { r.Identity.OwnerGeneration++ }},
		{"worktree", func(r *store.WorkItemRecord) { r.Identity.WorktreePath += "-other" }},
		{"branch", func(r *store.WorkItemRecord) { r.Identity.Branch += "-other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrong := original
			tc.edit(&wrong)
			if got, err := s.ClaimWorkItemOwner(ctx, wrong, "worker-2"); !errors.Is(err, domain.ErrWorkIdentityMismatch) || got != original {
				t.Fatalf("foreign claim = %+v, %v", got, err)
			}
			requireWorkItem(t, s, original)
		})
	}
	missing := original
	missing.Identity.ID = "missing"
	if _, err := s.ClaimWorkItemOwner(ctx, missing, "worker-2"); !errors.Is(err, store.ErrWorkItemNotFound) {
		t.Fatalf("missing claim = %v", err)
	}
	if _, found, err := s.GetWorkItem(ctx, "missing"); err != nil || found {
		t.Fatalf("missing read = %v, %v", found, err)
	}
	if _, err := s.ClaimWorkItemOwner(ctx, original, ""); !errors.Is(err, domain.ErrInvalidWorkIdentity) {
		t.Fatalf("unknown owner = %v", err)
	}
}

func TestWorkItemCreateConflictsAndValidation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	original := workItemRecord()
	if _, err := s.CreateWorkItem(ctx, original); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"attempt", "worktree", "worktree across scopes"} {
		t.Run(field, func(t *testing.T) {
			other := original
			other.Identity.ID = "work-2"
			if field == "attempt" {
				other.Identity.WorktreePath += "-2"
			} else {
				other.Identity.AttemptID = "attempt-2"
			}
			if field == "worktree across scopes" {
				other.Identity.ProjectID, other.Identity.Repository = "other", "child"
			}
			if _, err := s.CreateWorkItem(ctx, other); !errors.Is(err, domain.ErrDuplicateWorkIdentity) {
				t.Fatalf("duplicate %s = %v", field, err)
			}
			if _, found, err := s.GetWorkItem(ctx, other.Identity.ID); err != nil || found {
				t.Fatalf("conflicting row inserted: %v, %v", found, err)
			}
		})
	}
	wrong := original
	wrong.Identity.ExecutionBaseSHA = strings.Repeat("b", 40)
	if got, err := s.CreateWorkItem(ctx, wrong); !errors.Is(err, domain.ErrWorkIdentityMismatch) || got != original {
		t.Fatalf("changed authorization create = %+v, %v", got, err)
	}
	for _, change := range []func(*store.WorkItemRecord){
		func(r *store.WorkItemRecord) { r.Identity.OwnerGeneration = 2 },
		func(r *store.WorkItemRecord) { r.Identity.OwnerSessionID = "" },
		func(r *store.WorkItemRecord) { r.OriginalNativeSessionID = "native\nforeign" },
	} {
		invalid := original
		change(&invalid)
		if _, err := s.CreateWorkItem(ctx, invalid); !errors.Is(err, domain.ErrInvalidWorkIdentity) {
			t.Fatalf("invalid create = %v", err)
		}
	}
	requireWorkItem(t, s, original)
	// Unknown native identity is retained as unknown, never manufactured.
	unknown := original
	unknown.Identity.ID, unknown.Identity.AttemptID = "work-3", "attempt-3"
	unknown.Identity.WorktreePath += "-3"
	unknown.OriginalNativeSessionID = ""
	if got, err := s.CreateWorkItem(ctx, unknown); err != nil || got != unknown {
		t.Fatalf("unknown native = %+v, %v", got, err)
	}
}

func TestWorkItemConcurrentCreateAndClaims(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var stores []*sqlite.Store
	for range 2 {
		s, err := sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		stores = append(stores, s)
	}
	original := workItemRecord()
	const writers = 16
	type result struct {
		record store.WorkItemRecord
		err    error
	}
	run := func(claim bool) []result {
		results := make([]result, writers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for n := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if claim {
					results[n].record, results[n].err = stores[n%2].ClaimWorkItemOwner(ctx, original, domain.SessionID(fmt.Sprintf("owner-%d", n)))
				} else {
					results[n].record, results[n].err = stores[n%2].CreateWorkItem(ctx, original)
				}
			}()
		}
		close(start)
		wg.Wait()
		return results
	}
	for _, got := range run(false) {
		if got.err != nil || got.record != original {
			t.Fatalf("concurrent create = %+v, %v", got.record, got.err)
		}
	}
	results := run(true)
	current, found, err := stores[0].GetWorkItem(ctx, original.Identity.ID)
	if err != nil || !found || current.Identity.OwnerGeneration != 2 {
		t.Fatalf("concurrent readback = %+v, %v, %v", current, found, err)
	}
	winners := 0
	for _, got := range results {
		if got.err == nil {
			winners++
		} else if !errors.Is(got.err, domain.ErrStaleWorkOwner) {
			t.Fatalf("losing claim = %v", got.err)
		}
		if got.record != current {
			t.Fatalf("claim did not read back winner: %+v, want %+v", got.record, current)
		}
	}
	if winners != 1 {
		t.Fatalf("%d successful claims, want 1", winners)
	}
}

func TestWorkItemClaimRollbackAndCanceledContext(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	original := workItemRecord()
	if _, err := s.CreateWorkItem(ctx, original); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER work_item_test_abort AFTER UPDATE ON work_items
BEGIN SELECT RAISE(ABORT, 'injected claim failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimWorkItemOwner(ctx, original, "worker-2"); err == nil {
		t.Fatal("injected claim failure accepted")
	}
	requireWorkItem(t, s, original)
	if _, err := db.Exec(`DROP TRIGGER work_item_test_abort`); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ClaimWorkItemOwner(canceled, original, "worker-2"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled claim = %v", err)
	}
	requireWorkItem(t, s, original)
	if _, err := s.ClaimWorkItemOwner(ctx, original, "worker-2"); err != nil {
		t.Fatalf("retry after rollback = %v", err)
	}
}
