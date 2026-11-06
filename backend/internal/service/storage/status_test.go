package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"
)

type workspaceSourceFunc func(context.Context, string, int) ([]WorkspaceRecord, error)

func (f workspaceSourceFunc) ListStorageWorkspaces(ctx context.Context, after string, limit int) ([]WorkspaceRecord, error) {
	return f(ctx, after, limit)
}

func TestStatusThousandRowsPagination(t *testing.T) {
	records := make([]WorkspaceRecord, 1203)
	for i := range records {
		records[i] = WorkspaceRecord{ID: fmt.Sprintf("workspace:%06d", i), SessionID: "session", Path: "untrusted", Generation: 7, Revision: 9}
	}
	source := workspaceSourceFunc(func(_ context.Context, after string, limit int) ([]WorkspaceRecord, error) {
		if limit > MaxStatusPage+1 {
			t.Fatal("unbounded read")
		}
		start := 0
		for start < len(records) && records[start].ID <= after {
			start++
		}
		return records[start:min(start+limit, len(records))], nil
	})
	dataDir := t.TempDir()
	for _, limit := range []int{1, 100, 256} {
		after, previous, count := "", "", 0
		for page := 0; ; page++ {
			if page > 1300 {
				t.Fatal("pagination did not terminate")
			}
			got, err := ReadStatus(context.Background(), source, dataDir, after, limit)
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != "ao.storage.v1" || got.State != "SOURCE_GAP" || got.ExecutableCandidates != 0 || len(got.Rows) > limit {
				t.Fatalf("unsafe page: %+v", got)
			}
			for _, row := range got.Rows {
				if row.ID <= previous || !row.Retain || row.PhysicalBytes != nil || row.LogicalBytes != nil {
					t.Fatalf("unsafe row: %+v", row)
				}
				previous = row.ID
				count++
				if row.Category == "worktree" && (row.OwnerSession != "session" || *row.CleanupGeneration != 7 || *row.SourceRevision != 9 || !slices.Contains(row.ProtectionReasons, "unique_commits_or_unknown")) {
					t.Fatalf("lost provenance: %+v", row)
				}
			}
			if got.NextAfter == "" {
				break
			}
			after = got.NextAfter
		}
		if count != len(records)+3 {
			t.Fatalf("got %d rows", count)
		}
	}
}

func TestStatusLiveMutationAndPathAliasesStayUnknown(t *testing.T) {
	// The path may be a junction, symlink, overlapping alias, missing, denied,
	// dirty or replaced during a call. Durable records authorize no path probes.
	path := filepath.Join(t.TempDir(), "not-created")
	revision := int64(1)
	source := workspaceSourceFunc(func(_ context.Context, _ string, _ int) ([]WorkspaceRecord, error) {
		revision++
		return []WorkspaceRecord{{ID: "workspace:a", SessionID: "a", Path: path, Revision: revision},
			{ID: "workspace:b", SessionID: "b", Path: filepath.Join(path, "child"), Revision: revision, Pinned: true}}, nil
	})
	for range 2 {
		got, err := ReadStatus(context.Background(), source, path, "", 20)
		if err != nil {
			t.Fatal(err)
		}
		if got.Consistency != "live_keyset_not_snapshot" || got.ExecutableCandidates != 0 {
			t.Fatalf("unexpected assurance: %+v", got)
		}
		for _, row := range got.Rows {
			if !row.Retain || row.PhysicalBytes != nil || row.LogicalBytes != nil {
				t.Fatalf("path was trusted: %+v", row)
			}
			if row.Category == "worktree" {
				for _, reason := range []string{"dirty_or_unknown", "stable_or_unknown", "unique_commits_or_unknown", "cleanup_provenance_unresolved"} {
					if !slices.Contains(row.ProtectionReasons, reason) {
						t.Fatalf("missing %s: %+v", reason, row)
					}
				}
			}
		}
		if !slices.Contains(got.SourceGaps, "backup_ownership_consistency_and_restorability_unknown") {
			t.Fatal("invented backup qualification")
		}
	}
}

func TestStatusRejectsInvalidAndPropagatesDeniedSource(t *testing.T) {
	denied := workspaceSourceFunc(func(context.Context, string, int) ([]WorkspaceRecord, error) { return nil, fs.ErrPermission })
	if _, err := ReadStatus(context.Background(), denied, "", "", 10); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("masked denial: %v", err)
	}
	for _, limit := range []int{0, -1, 257} {
		if _, err := ReadStatus(context.Background(), nil, "", "", limit); err == nil {
			t.Fatal("invalid page accepted")
		}
	}
	for _, rows := range [][]WorkspaceRecord{
		{{ID: "workspace:a", SessionID: "a"}, {ID: "workspace:a", SessionID: "a"}},
		{{ID: "workspace:z", SessionID: "a"}, {ID: "workspace:a", SessionID: "a"}},
		{{ID: "workspace:a", SessionID: "a", Generation: -1}},
		make([]WorkspaceRecord, MaxStatusPage+2),
	} {
		source := workspaceSourceFunc(func(context.Context, string, int) ([]WorkspaceRecord, error) { return rows, nil })
		if _, err := ReadStatus(context.Background(), source, "", "", 256); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
}
