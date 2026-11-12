package chat

import (
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestWorkerReportNativeBridgeRetainsBindingAndGuard(t *testing.T) {
	f := newReportActorFixture(t)
	f.start(t)
	binding := f.b
	bridge, err := f.svc.BindWorkerReportNativeBridge(t.Context(), binding, f.leases,
		func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
			if _, ok := actor.(ports.WorkerReportActorGuard); !ok {
				t.Fatal("unguarded native actor")
			}
			return reportactor.NewAcceptance(actor, f.writes)
		})
	if err != nil {
		t.Fatal(err)
	}
	binding.Identity.OwnerSessionID = "foreign"
	binding.Lease.Revision++
	content := NativeWorkerReportContent{ID: f.r.ID, Type: f.r.Type, Version: f.r.Version, Content: f.r.Content, Digest: f.r.Digest}
	receipt, err := bridge.Submit(t.Context(), content)
	if err != nil || receipt.Report != f.r {
		t.Fatalf("native envelope: %+v %v", receipt, err)
	}
	f.c.mu.Lock()
	f.c.reportTurn = nil
	f.c.mu.Unlock()
	reads, writes := f.leases.reads, f.writes.writes
	if got, err := bridge.Submit(t.Context(), content); !errors.Is(err, domain.ErrWorkerReportActorMismatch) || got.EventID != "" {
		t.Fatalf("retired invocation: %+v %v", got, err)
	}
	if reads != f.leases.reads || writes != f.writes.writes {
		t.Fatal("retired invocation accessed receipt storage")
	}
	f.assertEvents(t, 1)
}

func TestWorkerReportNativeBridgeUnavailableNeverSubmits(t *testing.T) {
	for _, kind := range []string{"nil-factory", "uncommitted-turn", "factory-failure", "nil-submission"} {
		t.Run(kind, func(t *testing.T) {
			f := newReportActorFixture(t)
			if kind != "uncommitted-turn" {
				f.start(t)
			}
			failure := errors.New("acceptance unavailable")
			called := false
			factory := func(ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
				called = true
				if kind == "factory-failure" {
					return nil, failure
				}
				return nil, nil
			}
			if kind == "nil-factory" {
				factory = nil
			}
			bridge, err := f.svc.BindWorkerReportNativeBridge(t.Context(), f.b, f.leases, factory)
			if bridge != nil || err == nil {
				t.Fatalf("unavailable bridge: %+v %v", bridge, err)
			}
			if kind == "factory-failure" && !errors.Is(err, failure) {
				t.Fatalf("lost factory failure: %v", err)
			}
			if kind == "uncommitted-turn" && called {
				t.Fatal("uncommitted turn reached acceptance factory")
			}
			if f.writes.writes != 0 {
				t.Fatal("unavailable bridge wrote report")
			}
			f.assertEvents(t, 0)
		})
	}
	for _, bridge := range []*WorkerReportNativeBridge{nil, {}} {
		if got, err := bridge.Submit(t.Context(), NativeWorkerReportContent{}); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got.EventID != "" {
			t.Fatalf("zero bridge: %+v %v", got, err)
		}
	}
}
