package storage

import (
	"math"
	"reflect"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func measured(id string, category Category, size int64) Observation {
	return Observation{ID: id, Category: category, Facts: FactsCurrent,
		Size: Measurement{MeasurementID: "measure-" + id, Extent: []string{"root", id}, Known: true, Bytes: size}}
}

func syntheticInventory() []Observation {
	unknown := measured("unknown", Other, 0)
	unknown.Size.Known = false
	unknown.Facts = FactsUnknown
	return []Observation{
		measured("db", Database, 100), measured("wal", WAL, 20),
		measured("shm", SHM, 3), measured("snapshot", Snapshot, 40),
		measured("worktree-a", Worktree, 10), measured("worktree-b", Worktree, 11),
		measured("worktree-c", Worktree, 12), unknown,
	}
}

func cleanupWorktree() Observation {
	o := measured("worktree", Worktree, 10)
	o.Worktree = &WorktreeObservation{
		SessionID: "session", CurrentGeneration: 7, GenerationKnown: true,
		Dirty: No, Pinned: No, Stable: No, UniqueCommits: No,
		Cleanup: &CleanupObservation{RowRevision: 19, RevisionKnown: true,
			Record: domain.SessionCleanupRecord{SessionID: "session", SessionGeneration: 7,
				WorkspaceDisposition: domain.DispositionRemoved}},
	}
	return o
}

func TestInventorySubtotalAndOrder(t *testing.T) {
	input := syntheticInventory()
	got, err := BuildInventory(input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != (ByteTotal{MeasuredBytes: 196, UnknownEntries: 1, Complete: false}) || len(got.Entries) != 8 {
		t.Fatalf("unexpected inventory: %+v", got)
	}
	for i, j := 0, len(input)-1; i < j; i, j = i+1, j-1 {
		input[i], input[j] = input[j], input[i]
	}
	reversed, err := BuildInventory(input)
	if err != nil || !reflect.DeepEqual(got, reversed) {
		t.Fatalf("order affected inventory: %+v, %v", reversed, err)
	}
}

func TestInventoryRejectsInvalidObservations(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Observation)
	}{
		{"missing identity", func(o *Observation) { o.ID = "" }},
		{"missing measurement identity", func(o *Observation) { o.Size.MeasurementID = "" }},
		{"missing extent", func(o *Observation) { o.Size.Extent = nil }},
		{"path extent", func(o *Observation) { o.Size.Extent = []string{"a/b"} }},
		{"dot extent", func(o *Observation) { o.Size.Extent = []string{".."} }},
		{"negative size", func(o *Observation) { o.Size.Bytes = -1 }},
		{"unknown nonzero size", func(o *Observation) { o.Size.Known = false }},
		{"category", func(o *Observation) { o.Category = "invalid" }},
		{"facts", func(o *Observation) { o.Facts = "invalid" }},
		{"snapshot provenance", func(o *Observation) { o.SnapshotResolved = Yes }},
		{"worktree provenance", func(o *Observation) { o.Category = WAL }},
		{"worktree fact", func(o *Observation) { o.Worktree.Pinned = "invalid" }},
		{"older generation", func(o *Observation) { o.Worktree.Cleanup.Record.SessionGeneration-- }},
		{"newer generation", func(o *Observation) { o.Worktree.Cleanup.Record.SessionGeneration++ }},
		{"altered current generation", func(o *Observation) { o.Worktree.CurrentGeneration++ }},
		{"negative generation", func(o *Observation) { o.Worktree.CurrentGeneration = -1 }},
		{"unknown nonzero generation", func(o *Observation) { o.Worktree.GenerationKnown = false }},
		{"foreign session", func(o *Observation) { o.Worktree.Cleanup.Record.SessionID = "foreign" }},
		{"negative revision", func(o *Observation) { o.Worktree.Cleanup.RowRevision = -1 }},
		{"unknown nonzero revision", func(o *Observation) { o.Worktree.Cleanup.RevisionKnown = false }},
		{"negative attempts", func(o *Observation) { o.Worktree.Cleanup.Record.AttemptCount = -1 }},
		{"cleanup disposition", func(o *Observation) { o.Worktree.Cleanup.Record.WorkspaceDisposition = "invalid" }},
		{"contradictory dirty facts", func(o *Observation) {
			o.Worktree.Cleanup.Record.WorkspaceDisposition = domain.DispositionPreservedDirty
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := cleanupWorktree()
			tt.change(&o)
			if _, err := BuildInventory([]Observation{o}); err == nil {
				t.Fatal("invalid observation accepted")
			}
		})
	}
}

func TestInventoryRejectsDuplicateAndOverlappingCoverage(t *testing.T) {
	for _, kind := range []string{"id", "measurement", "equal", "parent", "child", "overflow"} {
		t.Run(kind, func(t *testing.T) {
			a, b := measured("a", Database, 1), measured("b", WAL, 2)
			switch kind {
			case "id":
				b.ID = a.ID
			case "measurement":
				b.Size.MeasurementID = a.Size.MeasurementID
			case "equal":
				b.Size.Extent = a.Size.Extent
			case "parent":
				b.Size.Extent = []string{"root"}
			case "child":
				b.Size.Extent = []string{"root", "a", "child"}
			case "overflow":
				a.Size.Bytes = math.MaxInt64
			}
			for _, input := range [][]Observation{{a, b}, {b, a}} {
				if _, err := BuildInventory(input); err == nil {
					t.Fatal("invalid coverage/total accepted")
				}
			}
		})
	}
}

func TestInventoryPreservesAndCopiesProvenance(t *testing.T) {
	o := cleanupWorktree()
	want := *o.Worktree.Cleanup
	got, err := BuildInventory([]Observation{o})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*got.Entries[0].Worktree.Cleanup, want) || got.Entries[0].Worktree.CurrentGeneration != 7 {
		t.Fatal("cleanup revision, generation or record lost")
	}
	o.Size.Extent[0] = "changed"
	o.Worktree.CurrentGeneration = 99
	o.Worktree.Cleanup.RowRevision = 99
	if got.Entries[0].Size.Extent[0] != "root" || got.Entries[0].Worktree.CurrentGeneration != 7 || got.Entries[0].Worktree.Cleanup.RowRevision != 19 {
		t.Fatal("output aliases input")
	}
	got.Entries[0].Worktree.Cleanup.Record.SessionID = "changed"
	if o.Worktree.Cleanup.Record.SessionID != "session" {
		t.Fatal("input aliases output")
	}
}

func TestInventoryKnownZeroEmptyAndMaximum(t *testing.T) {
	for _, input := range [][]Observation{nil, {measured("zero", Other, 0)}, {measured("max", Other, math.MaxInt64)}} {
		got, err := BuildInventory(input)
		if err != nil || !got.Total.Complete || got.Total.UnknownEntries != 0 {
			t.Fatalf("known inventory incomplete: %+v, %v", got.Total, err)
		}
	}
}
