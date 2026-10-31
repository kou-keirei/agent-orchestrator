// Package storage derives inventory and retention reports from caller-supplied
// observations. It collects no bytes and grants no deletion authority.
package storage

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type Category string

const (
	Database Category = "database"
	WAL      Category = "wal"
	SHM      Category = "shm"
	Snapshot Category = "snapshot"
	Worktree Category = "worktree"
	Other    Category = "other"
)

// Fact is three-valued: absence never means a successful negative probe.
type Fact string

const (
	Unknown Fact = ""
	Yes     Fact = "yes"
	No      Fact = "no"
)

type FactState string

const (
	FactsUnknown FactState = ""
	FactsCurrent FactState = "current"
	FactsStale   FactState = "stale"
	FactsError   FactState = "error"
	FactsForeign FactState = "foreign"
)

// MeasurementID identifies one measurement, while Extent identifies its coverage.
// Extents are canonical, case-sensitive hierarchical keys, not filesystem paths.
// Parents cover all descendants. Callers must give aliases the same extent and
// split disjoint coverage into separate observations; this core cannot resolve
// physical aliases or verify the supplied measurement's provenance.
type Measurement struct {
	MeasurementID string
	Extent        []string
	Known         bool
	Bytes         int64
}

// CleanupObservation preserves a caller's cleanup row revision and durable
// record. RowRevision is caller-supplied evidence, not a new database column:
// the current domain cleanup record has no revision field. RevisionKnown=false
// represents unavailable revision evidence, never an inferred revision.
type CleanupObservation struct {
	RowRevision   int64
	RevisionKnown bool
	Record        domain.SessionCleanupRecord
}

type WorktreeObservation struct {
	SessionID         domain.SessionID
	CurrentGeneration int64
	GenerationKnown   bool
	Cleanup           *CleanupObservation
	Dirty             Fact
	Pinned            Fact
	Stable            Fact
	UniqueCommits     Fact
}

type Observation struct {
	ID       string
	Category Category
	Size     Measurement
	Facts    FactState
	// SnapshotResolved must be Yes to claim a resolved recovery snapshot.
	SnapshotResolved Fact
	Worktree         *WorktreeObservation
}

// ByteTotal is a subtotal unless Complete is true. Unknown measurements do not
// contribute zero-byte claims. Completeness covers only supplied observations,
// not whether a caller discovered all storage on a machine.
type ByteTotal struct {
	MeasuredBytes  int64
	UnknownEntries int
	Complete       bool
}

type Inventory struct {
	Entries []Observation
	Total   ByteTotal
}

// BuildInventory rejects contradictory identities, coverage and provenance.
// Results are sorted by observation ID and own all mutable input slices/pointers.
func BuildInventory(observations []Observation) (Inventory, error) {
	ordered := slices.Clone(observations)
	slices.SortFunc(ordered, func(a, b Observation) int { return strings.Compare(a.ID, b.ID) })
	result := Inventory{Entries: make([]Observation, 0, len(ordered)), Total: ByteTotal{Complete: true}}
	measurements := make(map[string]bool, len(ordered))
	for i, o := range ordered {
		if err := validateObservation(o); err != nil {
			return Inventory{}, fmt.Errorf("observation %q: %w", o.ID, err)
		}
		if i > 0 && ordered[i-1].ID == o.ID {
			return Inventory{}, fmt.Errorf("duplicate observation %q", o.ID)
		}
		if measurements[o.Size.MeasurementID] {
			return Inventory{}, fmt.Errorf("duplicate measurement %q", o.Size.MeasurementID)
		}
		measurements[o.Size.MeasurementID] = true
		for _, earlier := range ordered[:i] {
			if containsExtent(earlier.Size.Extent, o.Size.Extent) || containsExtent(o.Size.Extent, earlier.Size.Extent) {
				return Inventory{}, fmt.Errorf("overlapping extents %q and %q", earlier.ID, o.ID)
			}
		}
		if o.Size.Known {
			if o.Size.Bytes > math.MaxInt64-result.Total.MeasuredBytes {
				return Inventory{}, fmt.Errorf("measured subtotal overflows int64")
			}
			result.Total.MeasuredBytes += o.Size.Bytes
		} else {
			result.Total.UnknownEntries++
			result.Total.Complete = false
		}
		o.Size.Extent = slices.Clone(o.Size.Extent)
		if o.Worktree != nil {
			w := *o.Worktree
			if w.Cleanup != nil {
				c := *w.Cleanup
				w.Cleanup = &c
			}
			o.Worktree = &w
		}
		result.Entries = append(result.Entries, o)
	}
	return result, nil
}

func containsExtent(parent, child []string) bool {
	return len(parent) <= len(child) && slices.Equal(parent, child[:len(parent)])
}

func validKey(key string) bool {
	return key != "" && strings.TrimSpace(key) == key && !strings.ContainsRune(key, '\x00')
}

func validFact(f Fact) bool { return f == Unknown || f == Yes || f == No }

func validateObservation(o Observation) error {
	if !validKey(o.ID) || !validKey(o.Size.MeasurementID) || len(o.Size.Extent) == 0 {
		return fmt.Errorf("identity and explicit measurement coverage required")
	}
	for _, part := range o.Size.Extent {
		if !validKey(part) || part == "." || part == ".." || strings.ContainsAny(part, "/\\") {
			return fmt.Errorf("invalid extent component")
		}
	}
	if o.Size.Bytes < 0 || (!o.Size.Known && o.Size.Bytes != 0) {
		return fmt.Errorf("negative or contradictory measurement")
	}
	switch o.Category {
	case Database, WAL, SHM, Snapshot, Worktree, Other:
	default:
		return fmt.Errorf("invalid category")
	}
	switch o.Facts {
	case FactsUnknown, FactsCurrent, FactsStale, FactsError, FactsForeign:
	default:
		return fmt.Errorf("invalid fact state")
	}
	if !validFact(o.SnapshotResolved) || (o.Category != Snapshot && o.SnapshotResolved != Unknown) {
		return fmt.Errorf("contradictory snapshot provenance")
	}
	if o.Category != Worktree && o.Worktree != nil {
		return fmt.Errorf("worktree provenance on another category")
	}
	if o.Worktree == nil {
		return nil
	}
	w := o.Worktree
	if !validFact(w.Dirty) || !validFact(w.Pinned) || !validFact(w.Stable) || !validFact(w.UniqueCommits) {
		return fmt.Errorf("invalid worktree fact")
	}
	if w.CurrentGeneration < 0 || (!w.GenerationKnown && w.CurrentGeneration != 0) {
		return fmt.Errorf("contradictory current generation")
	}
	if w.Cleanup == nil {
		return nil
	}
	c := w.Cleanup
	if c.RowRevision < 0 || (!c.RevisionKnown && c.RowRevision != 0) || c.Record.SessionGeneration < 0 || c.Record.AttemptCount < 0 {
		return fmt.Errorf("negative or contradictory cleanup provenance")
	}
	if !validKey(string(w.SessionID)) || c.Record.SessionID != w.SessionID {
		return fmt.Errorf("cleanup session identity mismatch")
	}
	// Equality is essential: neither an older nor a newer row is the current
	// cleanup generation. No >= comparison or termination/age heuristic applies.
	if w.GenerationKnown && c.Record.SessionGeneration != w.CurrentGeneration {
		return fmt.Errorf("cleanup generation mismatch")
	}
	switch c.Record.WorkspaceDisposition {
	case "", domain.DispositionPending, domain.DispositionRemoved, domain.DispositionPreservedDirty, domain.DispositionFailed, domain.DispositionNotApplicable:
	default:
		return fmt.Errorf("invalid cleanup disposition")
	}
	if o.Facts == FactsCurrent && w.GenerationKnown && w.Dirty == No && c.Record.WorkspaceDisposition == domain.DispositionPreservedDirty {
		return fmt.Errorf("contradictory current dirty facts")
	}
	return nil
}
