package sessionmanager

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestPrepareWorkItemSpawnMintsAndFreezes(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		t.Run(string(mode), func(t *testing.T) {
			f := newWorkSpawnFixture(t)
			grant := f.auth.grant
			grant.AllowedEffects = []string{"report"}
			a, err := prepareWorkItemSpawn(f.auth.project, f.parent.ID, workSpawnBase, grant, f.auth.issuer)
			if err != nil {
				t.Fatal(err)
			}
			other, err := prepareWorkItemSpawn(f.auth.project, f.parent.ID, workSpawnBase, grant, f.auth.issuer)
			if err != nil || a.work == other.work || a.attempt == other.attempt || a.work == "" || a.attempt == "" {
				t.Fatalf("identity minting: first=%+v other=%+v err=%v", a, other, err)
			}
			grant.AllowedEffects[0] = "changed"
			if a.grant.AllowedEffects[0] != "report" || a.issuer != f.auth.issuer || a.parent != f.parent.ID || a.base != workSpawnBase {
				t.Fatal("authorization was not captured")
			}
			if _, found, err := f.store.GetWorkItem(t.Context(), a.work); err != nil || found {
				t.Fatalf("preparation issued work: found=%v err=%v", found, err)
			}
			if f.births.Load() != 1 || f.runtime.created != 0 {
				t.Fatal("preparation launched a provider")
			}
			if _, err := f.manager.spawnWorkItem(t.Context(), f.config(mode), a); err != nil {
				t.Fatal(err)
			}
			if _, err := f.manager.spawnWorkItem(t.Context(), f.config(mode), a); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
				t.Fatalf("prepared authorization retry: %v", err)
			}
			lease, found, err := f.store.GetWorkItemLease(t.Context(), a.work)
			if err != nil || !found || !sameSpawnWorkGrant(lease.Grant, a.grant) {
				t.Fatalf("frozen grant: %+v found=%v err=%v", lease, found, err)
			}
		})
	}
}

func TestPrepareWorkItemSpawnRejectsIncompleteInputs(t *testing.T) {
	for _, kind := range []string{"nil-issuer", "bare-store", "project", "parent", "base", "grant"} {
		t.Run(kind, func(t *testing.T) {
			f := newWorkSpawnFixture(t)
			project, parent, base, grant, issuer := f.auth.project, f.auth.parent, f.auth.base, f.auth.grant, f.auth.issuer
			switch kind {
			case "nil-issuer":
				issuer = nil
			case "bare-store":
				issuer = f.store
			case "project":
				project = ""
			case "parent":
				parent = ""
			case "base":
				base = "main"
			case "grant":
				grant.Duration = 0
			}
			if a, err := prepareWorkItemSpawn(project, parent, base, grant, issuer); err == nil || a != nil {
				t.Fatalf("invalid preparation: %+v err=%v", a, err)
			}
		})
	}
}

func TestNativeWorkItemCommandFreezesInputsAndRejectsZeroValue(t *testing.T) {
	f := newWorkSpawnFixture(t)
	spec := NativeWorkItemSpec{Project: f.auth.project, Parent: f.parent.ID, Base: workSpawnBase,
		Harness: domain.HarnessCodex, Prompt: "authorized task", Grant: f.auth.grant}
	spec.Grant.AllowedEffects = []string{"report"}
	command, err := f.manager.PrepareNativeWorkItem(spec, f.auth.issuer)
	if err != nil {
		t.Fatal(err)
	}
	spec.Parent, spec.Base, spec.Prompt, spec.Harness = "foreign", "main", "changed", domain.HarnessFake
	spec.Grant.AllowedEffects[0] = "changed"
	if command.config.ParentSessionID != f.parent.ID || command.config.ExecutionBaseSHA != workSpawnBase ||
		command.config.Prompt != "authorized task" || command.config.Harness != domain.HarnessCodex ||
		command.authorization.grant.AllowedEffects[0] != "report" || command.WorkItemID() == "" {
		t.Fatal("caller mutation changed frozen native command")
	}
	for _, command := range []*NativeWorkItemCommand{nil, {}} {
		if rec, err := command.Spawn(t.Context()); !errors.Is(err, domain.ErrWorkAdmissionHeld) || rec.ID != "" || command.WorkItemID() != "" {
			t.Fatalf("zero command: %+v %v", rec, err)
		}
	}
	if command, err := f.manager.PrepareNativeWorkItem(NativeWorkItemSpec{Project: f.auth.project, Parent: f.parent.ID,
		Base: workSpawnBase, Grant: f.auth.grant}, f.auth.issuer); !errors.Is(err, ErrMissingHarness) || command != nil {
		t.Fatalf("missing native harness: %+v %v", command, err)
	}
}

func TestPreparedWorkItemCrashAfterIssuerCommitBeforeBirth(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		t.Run(string(mode), func(t *testing.T) {
			f := newWorkSpawnFixture(t)
			a, err := prepareWorkItemSpawn(f.auth.project, f.auth.parent, f.auth.base, f.auth.grant, f.auth.issuer)
			if err != nil {
				t.Fatal(err)
			}
			var identity domain.WorkItemIdentity
			var receipt ports.WorkItemSpawnReceipt
			a.issuer = workSpawnIssuer{WorkItemParentIssuer: f.auth.issuer, issue: func(ctx context.Context, i domain.WorkItemIdentity, native string, grant domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
				got, err := f.auth.issuer.(ports.WorkItemSpawnIssuer).IssueWorkItemSpawn(ctx, i, native, grant)
				if err != nil {
					return got, err
				}
				identity, receipt = i, got
				panic("crash after issuer commit")
			}}
			func() {
				defer func() {
					if got := recover(); got != "crash after issuer commit" {
						t.Fatalf("crash boundary: %v", got)
					}
				}()
				_, _ = f.manager.spawnWorkItem(t.Context(), f.config(mode), a)
			}()
			f.requireAtomicReceipt(t, identity, receipt)
			other, err := sqlite.Open(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			rt := &fakeRuntime{}
			cold := f.newManager(other, rt, &fakeWorkspace{})
			a.issuer = f.auth.issuer // A usable live issuer cannot bypass durable admission.
			if _, err := cold.spawnWorkItem(t.Context(), f.config(mode), a); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
				t.Fatalf("restart retry: %v", err)
			}
			worker, found, err := other.GetSession(t.Context(), identity.OriginalWorkerSessionID)
			if err != nil || !found || worker.IsTerminated {
				t.Fatalf("lost held seed: %+v found=%v err=%v", worker, found, err)
			}
			if err := cold.reconcileLive(t.Context(), worker); err != nil {
				t.Fatal(err)
			}
			f.requireAtomicReceipt(t, identity, receipt)
			if f.births.Load() != 1 || f.runtime.created != 0 || rt.created != 0 || f.ws.destroyed != 0 {
				t.Fatal("restart launched work or destroyed held resources")
			}
		})
	}
}
