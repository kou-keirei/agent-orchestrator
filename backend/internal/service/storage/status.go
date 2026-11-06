package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

// MaxStatusPage bounds both source reads and response rows.
const MaxStatusPage = 256

// WorkspaceRecord is AO's durable ownership record, not a filesystem attestation.
// Revision describes the session row only; it does not version Git or the path.
type WorkspaceRecord struct {
	ID, SessionID, Path  string
	Generation, Revision int64
	Pinned               bool
}

// WorkspaceSource reads a bounded keyset page from AO's existing store.
type WorkspaceSource interface {
	ListStorageWorkspaces(context.Context, string, int) ([]WorkspaceRecord, error)
}

// StatusRow retains every object. Nil byte counts and provenance mean UNKNOWN.
type StatusRow struct {
	ID                string   `json:"id"`
	Category          string   `json:"category"`
	Path              string   `json:"path"`
	OwnerSession      string   `json:"ownerSession"`
	CleanupGeneration *int64   `json:"cleanupGeneration"`
	SourceRevision    *int64   `json:"sourceRevision" description:"Owning session row revision only; worktree/Git/cleanup revision remains unknown."`
	WorkspaceTier     string   `json:"workspaceTier"`
	LogicalBytes      *int64   `json:"logicalBytes" description:"Logical file length; null means UNKNOWN, never zero."`
	PhysicalBytes     *int64   `json:"physicalBytes" description:"Allocated physical bytes; null means UNKNOWN, never inferred from logical length."`
	Retain            bool     `json:"retain"`
	ProtectionReasons []string `json:"protectionReasons"`
}

// Status is versioned, read-only, and explicitly incomplete. NextAfter is a live
// keyset cursor, not a snapshot lease: concurrent inserts before it may be missed
// and row revisions may change between pages. No totals or deletion are inferred.
type Status struct {
	Version              string      `json:"version" enum:"ao.storage.v1"`
	State                string      `json:"state" enum:"SOURCE_GAP"`
	Consistency          string      `json:"consistency" enum:"live_keyset_not_snapshot"`
	Rows                 []StatusRow `json:"rows"`
	NextAfter            string      `json:"nextAfter"`
	SourceGaps           []string    `json:"sourceGaps"`
	ExecutableCandidates int         `json:"executableCandidates" minimum:"0" maximum:"0"`
}

// ReadStatus reads only durable records. In particular it never calls the
// metadata collector with fabricated quiescence/no-reparse attestations. Live
// paths, aliases, permissions and TOCTOU cannot establish a physical census.
func ReadStatus(ctx context.Context, source WorkspaceSource, dataDir, after string, limit int) (Status, error) {
	if limit < 1 || limit > MaxStatusPage || len(after) > 4096 || strings.ContainsRune(after, '\x00') {
		return Status{}, fmt.Errorf("invalid storage page")
	}
	result := Status{Version: "ao.storage.v1", State: "SOURCE_GAP", Consistency: "live_keyset_not_snapshot",
		Rows: make([]StatusRow, 0, limit), SourceGaps: []string{
			"physical_allocation_and_aliases_unproven", "live_logical_sizes_unproven",
			"backup_ownership_consistency_and_restorability_unknown", "rebuildable_cache_ownership_unknown",
			"workspace_tier_git_and_cleanup_revision_unknown", "recorded_paths_not_physical_census",
		}}
	for _, item := range []struct {
		id       string
		category Category
		name     string
	}{
		{"database:db", Database, "ao.db"}, {"database:shm", SHM, "ao.db-shm"}, {"database:wal", WAL, "ao.db-wal"},
	} {
		if item.id <= after {
			continue
		}
		path := ""
		if filepath.IsAbs(dataDir) {
			path = filepath.Join(dataDir, item.name)
		}
		row, err := statusRow(Observation{ID: item.id, Category: item.category, Size: Measurement{MeasurementID: item.id, Extent: []string{item.id}}}, path)
		if err != nil {
			return Status{}, err
		}
		result.Rows = append(result.Rows, row)
	}
	if source == nil {
		return Status{}, fmt.Errorf("storage ownership source unavailable")
	}
	// One lookahead detects continuation without loading the full result set.
	records, err := source.ListStorageWorkspaces(ctx, after, limit+1)
	if err != nil {
		return Status{}, err
	}
	if len(records) > limit+1 {
		return Status{}, fmt.Errorf("storage source exceeded page bound")
	}
	previous := after
	for _, rec := range records {
		if !strings.HasPrefix(rec.ID, "workspace:") || rec.ID <= previous || rec.SessionID == "" || rec.Generation < 0 || rec.Revision < 0 {
			return Status{}, fmt.Errorf("invalid storage source record")
		}
		previous = rec.ID
		pinned := No
		if rec.Pinned {
			pinned = Yes
		}
		row, err := statusRow(Observation{ID: rec.ID, Category: Worktree,
			Size:     Measurement{MeasurementID: rec.ID, Extent: []string{rec.ID}},
			Worktree: &WorktreeObservation{Pinned: pinned}}, rec.Path)
		if err != nil {
			return Status{}, err
		}
		row.OwnerSession = rec.SessionID
		row.CleanupGeneration = &rec.Generation
		row.SourceRevision = &rec.Revision
		result.Rows = append(result.Rows, row)
	}
	if len(result.Rows) > limit {
		result.Rows = result.Rows[:limit]
		result.NextAfter = result.Rows[limit-1].ID
	}
	return result, nil
}

func statusRow(observation Observation, path string) (StatusRow, error) {
	// Each row has a logical identity extent only. No physical alias uniqueness
	// is asserted or aggregated across these unknown measurements.
	plan, err := BuildPlan([]Observation{observation})
	if err != nil {
		return StatusRow{}, err
	}
	return StatusRow{ID: observation.ID, Category: string(observation.Category), Path: path,
		OwnerSession: "UNKNOWN", WorkspaceTier: "UNKNOWN", Retain: plan.Decisions[0].Retain,
		ProtectionReasons: append(plan.Decisions[0].Reasons, "physical_provenance_unknown")}, nil
}
