package store_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type reportDeliveryTransport struct{ batches []orchestrationevents.Batch }

func (t *reportDeliveryTransport) Submit(_ context.Context, _ domain.SessionRecord, b orchestrationevents.Batch) (orchestrationevents.Submission, error) {
	t.batches = append(t.batches, b)
	return orchestrationevents.Submission{Submitted: true, Acknowledged: true}, nil
}

func TestReportDeliveryHoldRestartAndIndependentLegacy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := seedWorkerReport(t, s)
	receipt, err := s.AcceptWorkerReport(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	parent := sampleRecord("project-1")
	parent.Kind, parent.Mode, parent.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
	parent.Activity.State = domain.ActivityIdle
	if _, err := s.CreateSession(ctx, parent); err != nil {
		t.Fatal(err)
	}
	legacy := domain.OrchestrationEvent{ID: "legacy", ProjectID: r.Identity.ProjectID, WorkerID: r.WorkerSessionID,
		Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r1", EnqueuedAt: now, NextAttemptAt: now}
	if _, err := s.EnqueueOrchestrationEvent(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	transport := &reportDeliveryTransport{}
	for range 2 {
		d := &orchestrationevents.Dispatcher{Store: s, Transport: transport}
		if err := d.DispatchProject(ctx, r.Identity.ProjectID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
			t.Fatalf("dispatch: %v", err)
		}
		events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
		if err != nil || len(events) != 2 {
			t.Fatalf("events=%+v err=%v", events, err)
		}
		for _, e := range events {
			if e.ID == receipt.EventID {
				if e.State != domain.OrchestrationPending || e.LeaseToken != "" || e.DestinationSessionID != "" || !e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() || e.AttemptCount != 0 || e.AttentionRequiredAt.IsZero() || e.LastError != ports.WorkerReportDeliveryHoldReason {
					t.Fatalf("report advanced: %+v", e)
				}
			} else if e.State != domain.OrchestrationAcknowledged {
				t.Fatalf("legacy starved: %+v", e)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.AcceptWorkerReport(ctx, r)
		if err != nil || got != receipt {
			t.Fatalf("receipt changed: %+v %v", got, err)
		}
	}
	if len(transport.batches) != 1 || !reflect.DeepEqual(transport.batches[0].EventIDs, []string{legacy.ID}) {
		t.Fatalf("batches=%+v", transport.batches)
	}
}

func TestReportDeliveryRejectsGenericPathsAndPartialMixedBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := seedWorkerReport(t, s)
	receipt, err := s.AcceptWorkerReport(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	parent := sampleRecord("project-1")
	parent.Kind = domain.KindOrchestrator
	parent, err = s.CreateSession(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ids := []string{receipt.EventID}
	if err := s.LeaseOrchestrationEvents(ctx, ids, "new-client-id", parent.ID, now.Add(time.Minute)); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatalf("lease: %v", err)
	}
	// Disposable historical pre-upgrade batch: provider admission or a lost
	// lifecycle ACK must not complete any subset or select another destination.
	legacy := domain.OrchestrationEvent{ID: "mixed-legacy", ProjectID: r.Identity.ProjectID, WorkerID: r.WorkerSessionID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "mixed-r", EnqueuedAt: now, NextAttemptAt: now}
	if _, err := s.EnqueueOrchestrationEvent(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	ids = append(ids, legacy.ID)
	db := reportDB(t, dir)
	if _, err := db.Exec(`UPDATE orchestration_events SET state='submitted',lease_token='original-client-id',destination_session_id=?,submitted_at=?,lease_expires_at=?`, parent.ID, now, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, members := range [][]string{ids, ids[:1], ids[1:], {receipt.EventID, receipt.EventID}} {
		for _, apply := range []func() error{
			func() error { return s.AcknowledgeOrchestrationEvents(ctx, members, "original-client-id", now) },
			func() error { return s.MarkOrchestrationEventsSubmitted(ctx, members, "original-client-id", now) },
			func() error {
				return s.LeaseOrchestrationEvents(ctx, members, "original-client-id", parent.ID, now.Add(time.Minute))
			},
		} {
			if err := apply(); err == nil {
				t.Fatalf("generic mutation admitted members=%v", members)
			}
		}
	}
	if n, err := s.AcknowledgeOrchestrationBatchAccepted(ctx, parent.ID, "original-client-id", now); err != nil || n != 0 {
		t.Fatalf("provider admission ACKed report: %d %v", n, err)
	}
	if _, err := s.ReclaimOrchestrationEventLeases(ctx, now); err != nil {
		t.Fatal(err)
	}
	if held, err := s.HoldWorkerReportDeliveries(ctx, r.Identity.ProjectID, now); err != nil || !held {
		t.Fatalf("hold=%v %v", held, err)
	}
	before, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range before {
		if e.State == domain.OrchestrationAcknowledged || !e.AcknowledgedAt.IsZero() || e.LeaseToken != "original-client-id" || e.DestinationSessionID != parent.ID || e.SubmittedAt.IsZero() || e.AttentionRequiredAt.IsZero() {
			t.Fatalf("binding lost: %+v", e)
		}
	}
	if due, err := s.ListDueOrchestrationEvents(ctx, r.Identity.ProjectID, now.Add(time.Hour), 50); err != nil || len(due) != 0 {
		t.Fatalf("partial retry: %+v %v", due, err)
	}
	if err := s.ClearOrchestrationAttention(ctx, ids[:1]); err != nil {
		t.Fatal(err)
	}
	after, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("report attention cleared: before=%+v after=%+v", before, after)
	}
}

func TestReportDeliveryHoldRespectsProjectHoldAcrossIndependentOpens(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := seedWorkerReport(t, s)
	if _, err := s.AcceptWorkerReport(ctx, r); err != nil {
		t.Fatal(err)
	}
	other, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := s.SetOrchestrationDispatchHold(ctx, r.Identity.ProjectID, true); err != nil {
		t.Fatal(err)
	}
	before, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.HoldWorkerReportDeliveries(ctx, r.Identity.ProjectID, time.Now()); !errors.Is(err, orchestrationevents.ErrDispatchHeld) {
		t.Fatalf("project hold: %v", err)
	}
	after, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("held project mutated")
	}
	if err := s.SetOrchestrationDispatchHold(ctx, r.Identity.ProjectID, false); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, db := range []*sqlite.Store{s, other} {
		wg.Go(func() {
			for range 3 {
				if held, err := db.HoldWorkerReportDeliveries(ctx, r.Identity.ProjectID, time.Now()); err != nil || !held {
					t.Errorf("concurrent hold: %v %v", held, err)
				}
				if _, err := db.AcceptWorkerReport(ctx, r); err != nil {
					t.Errorf("duplicate receipt: %v", err)
				}
			}
		})
	}
	wg.Wait()
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationPending || events[0].AttentionRequiredAt.IsZero() {
		t.Fatalf("concurrent result: %+v %v", events, err)
	}
}
