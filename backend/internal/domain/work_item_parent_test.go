package domain

import (
	"errors"
	"reflect"
	"testing"
)

func parentTarget(work WorkItemIdentity) WorkItemParentTarget {
	return WorkItemParentTarget{
		WorkItemID: work.ID, AttemptID: work.AttemptID,
		OriginalParentSessionID: work.OriginalParentSessionID,
		NativeSessionID:         "parent-native", ControllerGeneration: "parent-controller",
		ProviderConversationID: "parent-conversation", ProviderScopeID: "parent-scope",
		IssuedBySessionID:            work.OriginalParentSessionID,
		IssuedByControllerGeneration: "parent-controller", Provenance: WorkItemParentLiveProvenance,
	}
}

func TestWorkItemParentTargetBinding(t *testing.T) {
	work := workIdentityFixture()
	target := parentTarget(work)
	if err := target.ValidateFor(work); err != nil {
		t.Fatal(err)
	}
	// Ownership and candidate changes cannot change the original destination.
	next := work
	next.OwnerSessionID, next.OwnerGeneration = "replacement-worker", work.OwnerGeneration+1
	if err := target.ValidateFor(next); err != nil {
		t.Fatalf("owner transfer retargeted parent: %v", err)
	}
	for _, edit := range []func(*WorkItemIdentity){
		func(w *WorkItemIdentity) { w.ID = "different" },
		func(w *WorkItemIdentity) { w.AttemptID = "different" },
		func(w *WorkItemIdentity) { w.OriginalParentSessionID = "new-coordinator" },
	} {
		wrong := work
		edit(&wrong)
		if err := target.ValidateFor(wrong); !errors.Is(err, ErrInvalidWorkItemParent) {
			t.Fatalf("foreign work accepted: %+v %v", wrong, err)
		}
	}
	// Every string binding must be present and byte-preserving. These are shape
	// checks only; even a well-shaped forged value cannot authorize issuance.
	for n := range reflect.TypeFor[WorkItemParentTarget]().NumField() {
		name := reflect.TypeFor[WorkItemParentTarget]().Field(n).Name
		for _, value := range []string{"", " unknown", "unknown\n", "unknown\x00", "\xff"} {
			wrong := target
			reflect.ValueOf(&wrong).Elem().Field(n).SetString(value)
			if err := wrong.ValidateFor(work); !errors.Is(err, ErrInvalidWorkItemParent) {
				t.Fatalf("%s=%q accepted: %v", name, value, err)
			}
		}
	}
	for _, edit := range []func(*WorkItemParentTarget){
		func(t *WorkItemParentTarget) { t.IssuedBySessionID = work.OwnerSessionID },
		func(t *WorkItemParentTarget) { t.IssuedByControllerGeneration = "replacement-controller" },
		func(t *WorkItemParentTarget) { t.Provenance = "restored_native_history" },
		func(t *WorkItemParentTarget) { t.Provenance = "worker_report" },
	} {
		wrong := target
		edit(&wrong)
		if err := wrong.ValidateFor(work); !errors.Is(err, ErrInvalidWorkItemParent) {
			t.Fatalf("foreign authority accepted: %+v %v", wrong, err)
		}
	}
}

func TestWorkItemParentTypedHolds(t *testing.T) {
	for _, reason := range []WorkItemParentHoldReason{WorkItemParentLegacyMissing, WorkItemParentIssuanceUnbound} {
		err := &WorkItemParentHold{Reason: reason}
		if !errors.Is(err, ErrWorkItemParentHeld) ||
			errors.Is(err, ErrWorkItemParentBlocksPR) != (reason == WorkItemParentIssuanceUnbound) {
			t.Fatalf("incorrect hold classification: %v", err)
		}
	}
}
