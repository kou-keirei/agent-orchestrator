package sessionmanager

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
)

func TestNativeReportToolSetupFollowsAdmissionAndNeverOrdinarySpawn(t *testing.T) {
	calls := 0
	f := newWorkSpawnFixture(t, func(f *workSpawnFixture, opts *chatsvc.Options) {
		opts.PrepareWorkerReportTool = func(ctx context.Context, id domain.WorkItemID, worker domain.SessionID) (ports.WorkerReportToolAdmission, error) {
			calls++
			if id != f.auth.work || f.births.Load() != 1 {
				t.Fatal("tool setup bypassed trusted admission or followed worker birth")
			}
			b, found, err := (workBridgeBindings{f.store}).ReadWorkerReportBinding(ctx, id)
			if err != nil || !found || b.Identity.OwnerSessionID != worker {
				t.Fatalf("unbound setup: %+v %v", b, err)
			}
			if _, err := f.store.GetWorkItemOriginalParent(ctx, id, b.Identity.AttemptID); err != nil {
				t.Fatal(err)
			}
			return ports.WorkerReportToolAdmission{Binding: b, Bindings: workBridgeBindings{f.store}, Acceptance: func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
				return reportactor.NewAcceptance(actor, f.store)
			}}, nil
		}
	})
	if _, err := f.manager.spawnWorkItem(t.Context(), f.config(domain.SessionModeChat), &f.auth); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || f.births.Load() != 2 {
		t.Fatalf("native setup calls=%d births=%d", calls, f.births.Load())
	}
	if _, _, _, err := f.manager.Spawn(t.Context(), f.config(domain.SessionModeChat)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("ordinary spawn invoked report setup")
	}
}
