package storage

import (
	"reflect"
	"slices"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestPlanSyntheticInventoryRetainsEverything(t *testing.T) {
	input := syntheticInventory()
	got, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Inventory.Total.MeasuredBytes != 196 || got.Inventory.Total.Complete || got.ExecutableCandidates != 0 || len(got.Decisions) != 8 {
		t.Fatalf("unexpected plan: %+v", got)
	}
	for i, decision := range got.Decisions {
		if !decision.Retain || decision.ID != got.Inventory.Entries[i].ID || len(decision.Reasons) == 0 {
			t.Fatalf("unsafe or unbound decision: %+v", decision)
		}
	}
	for i, j := 0, len(input)-1; i < j; i, j = i+1, j-1 {
		input[i], input[j] = input[j], input[i]
	}
	reversed, err := BuildPlan(input)
	if err != nil || !reflect.DeepEqual(got, reversed) {
		t.Fatal("plan depends on caller order")
	}
}

func TestPlanWorktreeProtections(t *testing.T) {
	for _, field := range []string{"dirty", "pinned", "stable", "unique"} {
		for _, state := range []Fact{Unknown, Yes} {
			t.Run(field+string(state), func(t *testing.T) {
				o := cleanupWorktree()
				var reason string
				switch field {
				case "dirty":
					o.Worktree.Dirty, reason = state, "dirty_or_unknown"
				case "pinned":
					o.Worktree.Pinned, reason = state, "pinned_or_unknown"
				case "stable":
					o.Worktree.Stable, reason = state, "stable_or_unknown"
				case "unique":
					o.Worktree.UniqueCommits, reason = state, "unique_commits_or_unknown"
				}
				got, err := BuildPlan([]Observation{o})
				if err != nil {
					t.Fatal(err)
				}
				if !got.Decisions[0].Retain || !slices.Contains(got.Decisions[0].Reasons, reason) || got.ExecutableCandidates != 0 {
					t.Fatalf("protection lost: %+v", got)
				}
			})
		}
	}
}

func TestPlanUnknownFactsAndSnapshots(t *testing.T) {
	for _, facts := range []FactState{FactsUnknown, FactsStale, FactsError, FactsForeign, FactsCurrent} {
		for _, resolved := range []Fact{Unknown, No, Yes} {
			o := measured("snapshot", Snapshot, 40)
			o.Facts, o.SnapshotResolved = facts, resolved
			got, err := BuildPlan([]Observation{o})
			if err != nil {
				t.Fatal(err)
			}
			reasons := got.Decisions[0].Reasons
			if !got.Decisions[0].Retain || got.ExecutableCandidates != 0 ||
				(facts != FactsCurrent && !slices.Contains(reasons, "facts_unknown_or_unusable")) ||
				(resolved != Yes && !slices.Contains(reasons, "snapshot_unresolved")) {
				t.Fatalf("snapshot protection lost: %+v", got)
			}
		}
	}
}

func TestPlanCleanupNeverAuthorizesDeletion(t *testing.T) {
	for _, disposition := range []domain.WorkspaceDisposition{"", domain.DispositionPending, domain.DispositionRemoved, domain.DispositionFailed, domain.DispositionNotApplicable} {
		o := cleanupWorktree()
		o.Worktree.Cleanup.Record.WorkspaceDisposition = disposition
		got, err := BuildPlan([]Observation{o})
		if err != nil || !got.Decisions[0].Retain || got.ExecutableCandidates != 0 {
			t.Fatalf("cleanup granted deletion: %+v, %v", got, err)
		}
	}
	for _, change := range []func(*Observation){
		func(o *Observation) { o.Worktree = nil },
		func(o *Observation) { o.Worktree.Cleanup = nil },
		func(o *Observation) { o.Worktree.CurrentGeneration = 0; o.Worktree.GenerationKnown = false },
		func(o *Observation) { o.Worktree.Cleanup.RowRevision = 0; o.Worktree.Cleanup.RevisionKnown = false },
	} {
		o := cleanupWorktree()
		change(&o)
		got, err := BuildPlan([]Observation{o})
		if err != nil || !got.Decisions[0].Retain || got.ExecutableCandidates != 0 {
			t.Fatalf("missing evidence granted deletion: %+v, %v", got, err)
		}
	}
}

func TestPlanRejectsAlteredGenerationAndOverlap(t *testing.T) {
	o := cleanupWorktree()
	o.Worktree.CurrentGeneration++
	if _, err := BuildPlan([]Observation{o}); err == nil {
		t.Fatal("altered generation accepted")
	}
	a, b := measured("a", Database, 100), measured("b", WAL, 20)
	b.Size.Extent = a.Size.Extent
	if _, err := BuildPlan([]Observation{a, b}); err == nil {
		t.Fatal("overlapping inventory accepted")
	}
}
