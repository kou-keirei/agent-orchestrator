package orchestrationevents

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestReportNeverUsesTransportAdmissionAsSemanticACK(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		store := &fakeStore{events: []domain.OrchestrationEvent{{ID: "report", Kind: domain.OrchestrationWorkerReport}}}
		if mixed {
			store.events = append([]domain.OrchestrationEvent{{ID: "legacy", Kind: domain.OrchestrationWorkerBlocked}}, store.events...)
			for i := range store.events {
				store.events[i].LeaseToken = "stable-client-message"
				store.events[i].DestinationSessionID = "original-parent"
			}
		}
		transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
		err := (&Dispatcher{Store: store, Transport: transport}).DispatchProject(context.Background(), "p")
		if !errors.Is(err, ErrReportDeliveryHeld) || transport.calls != 0 || len(store.leased)+len(store.submitted)+len(store.acked) != 0 || store.sessionLists != 0 {
			t.Fatalf("mixed=%v err=%v transport=%+v store=%+v", mixed, err, transport, store)
		}
	}
}

func TestReportHoldAllowsIndependentLegacyWithoutResolvingReportAttention(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		reportsHeld: true,
		sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator,
			Mode: domain.SessionModeChat, FirstSignalAt: now, Activity: domain.Activity{State: domain.ActivityIdle}}},
		events: []domain.OrchestrationEvent{{ID: "legacy", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked}},
		attentionEvents: []domain.OrchestrationEvent{{ID: "report", ProjectID: "p", WorkerID: "w",
			Kind: domain.OrchestrationWorkerReport, State: domain.OrchestrationPending, AttentionRequiredAt: now}},
	}
	transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
	notifier := &fakeNotifier{}
	d := &Dispatcher{Store: store, Transport: transport, Notifier: notifier}
	if err := d.DispatchProject(context.Background(), "p"); !errors.Is(err, ErrReportDeliveryHeld) {
		t.Fatalf("err=%v", err)
	}
	if transport.calls != 1 || len(store.acked) != 1 || store.acked[0] != "legacy" || len(notifier.created) != 1 || len(notifier.resolved)+len(store.cleared) != 0 {
		t.Fatalf("transport=%+v store=%+v notifier=%+v", transport, store, notifier)
	}
	// A real legacy failure must not be suppressed as a safe report hold.
	want := errors.New("legacy persistence failure")
	store.ackErr = want
	if err := d.DispatchProject(context.Background(), "p"); !errors.Is(err, want) || errors.Is(err, ErrReportDeliveryHeld) {
		t.Fatalf("masked failure: %v", err)
	}
}

func TestRecoverToleratesReportHoldAndContinuesOtherProjects(t *testing.T) {
	store := &fakeStore{reportsHeld: true}
	transport := &fakeTransport{}
	recovery := &recoveryStoreFake{projects: []domain.ProjectRecord{{ID: "report-project"}, {ID: "other-project"}}}
	if err := Recover(context.Background(), recovery, &Dispatcher{Store: store, Transport: transport}); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 0 || store.reportHoldCalls != 2 || !recovery.reclaimed || !recovery.maintained {
		t.Fatalf("transport=%+v recovery=%+v", transport, recovery)
	}
}

func TestReportLegacyTransportACKDoesNotResolveSemanticAttention(t *testing.T) {
	notifier := &fakeNotifier{}
	store := &fakeStore{attentionEvents: []domain.OrchestrationEvent{{ID: "report", ProjectID: "p", WorkerID: "w",
		Kind: domain.OrchestrationWorkerReport, State: domain.OrchestrationAcknowledged, AttentionRequiredAt: time.Now()}}}
	if err := (&Dispatcher{Store: store, Notifier: notifier}).ReconcileAttention(context.Background(), "p", time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(notifier.created) != 1 || len(notifier.resolved)+len(store.cleared) != 0 {
		t.Fatalf("transport ACK resolved semantic attention: notifier=%+v store=%+v", notifier, store)
	}
}

func TestReportHoldPrecedesParentDiscoveryForUnavailableHistory(t *testing.T) {
	for _, scenario := range []string{"busy", "exited", "replaced", "foreign", "history_unknown"} {
		t.Run(scenario, func(t *testing.T) {
			parent := domain.SessionRecord{ID: "replacement", ProjectID: "p", Kind: domain.KindOrchestrator,
				Mode: domain.SessionModeChat, FirstSignalAt: time.Now(), Activity: domain.Activity{State: domain.ActivityIdle}}
			switch scenario {
			case "busy":
				parent.Activity.State = domain.ActivityActive
			case "exited":
				parent.IsTerminated = true
			case "foreign":
				parent.ProjectID = "foreign"
			}
			store := &fakeStore{reportsHeld: true, sessions: []domain.SessionRecord{parent}}
			transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
			if err := (&Dispatcher{Store: store, Transport: transport}).DispatchProject(context.Background(), "p"); !errors.Is(err, ErrReportDeliveryHeld) {
				t.Fatalf("err=%v", err)
			}
			if transport.calls != 0 || store.sessionLists != 0 || len(store.leased)+len(store.acked) != 0 {
				t.Fatalf("parent substitution: store=%+v transport=%+v", store, transport)
			}
		})
	}
}
