package store_test

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	storagesvc "github.com/aoagents/agent-orchestrator/backend/internal/service/storage"
)

func TestStorageStatusNativeStorePagination(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "storage")
	ctx := context.Background()
	for range 1005 {
		rec := sampleRecord("storage")
		rec.CleanupGeneration = 4
		if _, err := s.CreateSession(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	after := ""
	for {
		page, err := storagesvc.ReadStatus(ctx, s, t.TempDir(), after, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Rows {
			if seen[row.ID] || !row.Retain || row.PhysicalBytes != nil {
				t.Fatalf("unsafe row: %+v", row)
			}
			seen[row.ID] = true
		}
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
	}
	if len(seen) != 1008 {
		t.Fatalf("got %d rows", len(seen))
	}
	if _, err := s.ListStorageWorkspaces(ctx, "", 258); err == nil {
		t.Fatal("unbounded read accepted")
	}
}

func TestStorageStatusRegisteredReposAndLiveRevision(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "storage")
	ctx := context.Background()
	rec, err := s.CreateSession(ctx, sampleRecord("storage"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"__root__", "child"} {
		if err := s.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: rec.ID, RepoName: name, WorktreePath: "/unproven/" + name}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.ListStorageWorkspaces(ctx, "", 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("registered rows: %+v %v", rows, err)
	}
	before := rows[0].Revision
	if _, err := s.RenameSession(ctx, rec.ID, "new-name", rec.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	updated, err := s.ListStorageWorkspaces(ctx, "", 10)
	if err != nil || updated[0].Revision <= before || updated[0].ID != rows[0].ID {
		t.Fatalf("live revision/identity: %+v %v", updated, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ListStorageWorkspaces(canceled, "", 10); err == nil {
		t.Fatal("canceled read succeeded")
	}
}
