package storage

type Decision struct {
	ID      string
	Retain  bool
	Reasons []string
}

// Plan is a dry-run report only. It contains no action paths, execution
// candidates or deletion capability. ExecutableCandidates is always zero.
type Plan struct {
	Inventory            Inventory
	Decisions            []Decision
	ExecutableCandidates int
}

func BuildPlan(observations []Observation) (Plan, error) {
	inventory, err := BuildInventory(observations)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{Inventory: inventory, Decisions: make([]Decision, 0, len(inventory.Entries))}
	for _, o := range inventory.Entries {
		reasons := []string{"deletion_policy_unavailable"}
		if o.Facts != FactsCurrent {
			reasons = append(reasons, "facts_unknown_or_unusable")
		}
		if !o.Size.Known {
			reasons = append(reasons, "size_unknown")
		}
		switch o.Category {
		case Database, WAL, SHM:
			reasons = append(reasons, "database_storage_retained")
		case Snapshot:
			if o.SnapshotResolved != Yes {
				reasons = append(reasons, "snapshot_unresolved")
			}
		case Worktree:
			w := o.Worktree
			if w == nil {
				reasons = append(reasons, "worktree_facts_unknown")
				break
			}
			// Keep each independent protection visible, including when other
			// facts are unavailable. No boolean zero value is treated as safe.
			for _, fact := range []struct {
				value  Fact
				reason string
			}{
				{w.Dirty, "dirty_or_unknown"},
				{w.Pinned, "pinned_or_unknown"},
				{w.Stable, "stable_or_unknown"},
				{w.UniqueCommits, "unique_commits_or_unknown"},
			} {
				if fact.value != No {
					reasons = append(reasons, fact.reason)
				}
			}
			if !w.GenerationKnown || w.Cleanup == nil || !w.Cleanup.RevisionKnown || o.Facts != FactsCurrent {
				reasons = append(reasons, "cleanup_provenance_unresolved")
			}
		}
		plan.Decisions = append(plan.Decisions, Decision{ID: o.ID, Retain: true, Reasons: reasons})
	}
	return plan, nil
}
