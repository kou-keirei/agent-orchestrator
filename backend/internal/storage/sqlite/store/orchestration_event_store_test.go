package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type synchronousAckTransport struct {
	store interface {
		AcknowledgeOrchestrationBatchAccepted(context.Context, domain.SessionID, string, time.Time) (int64, error)
	}
}

func (t synchronousAckTransport) Submit(ctx context.Context, target domain.SessionRecord, batch orchestrationevents.Batch) (orchestrationevents.Submission, error) {
	_, err := t.store.AcknowledgeOrchestrationBatchAccepted(ctx, target.ID, batch.ID, time.Now().UTC())
	return orchestrationevents.Submission{Submitted: true}, err
}

func TestOrchestrationEventStoreDedupeRearmLeaseAndAcknowledge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	worker, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := sampleRecord("p")
	orchestrator.Kind = domain.KindOrchestrator
	orchestrator, err = s.CreateSession(ctx, orchestrator)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	event := domain.OrchestrationEvent{ID: "e1", ProjectID: "p", WorkerID: worker.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "revision-1", EnqueuedAt: now, NextAttemptAt: now}
	inserted, err := s.EnqueueOrchestrationEvent(ctx, event)
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	event.ID = "duplicate"
	inserted, err = s.EnqueueOrchestrationEvent(ctx, event)
	if err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	event.ID = "e2"
	event.SourceRevision = "revision-2"
	inserted, err = s.EnqueueOrchestrationEvent(ctx, event)
	if err != nil || !inserted {
		t.Fatalf("rearm inserted=%v err=%v", inserted, err)
	}
	due, err := s.ListDueOrchestrationEvents(ctx, "p", now, 50)
	if err != nil || len(due) != 2 {
		t.Fatalf("due=%v err=%v", due, err)
	}
	ids := []string{due[0].ID, due[1].ID}
	if err := s.LeaseOrchestrationEvents(ctx, ids, "batch", orchestrator.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkOrchestrationEventsSubmitted(ctx, ids, "batch", now); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeOrchestrationEvents(ctx, ids, "wrong", now); err == nil {
		t.Fatal("wrong lease token acknowledged")
	}
	if err := s.AcknowledgeOrchestrationEvents(ctx, ids, "batch", now); err != nil {
		t.Fatal(err)
	}
	if due, err = s.ListDueOrchestrationEvents(ctx, "p", now.Add(time.Hour), 50); err != nil || len(due) != 0 {
		t.Fatalf("acknowledged due=%v err=%v", due, err)
	}
}

func TestOrchestrationEventStoreRestartReclaimsExpiredLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	e := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
	if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{"e"}, "batch", o.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if count, err := s.ReclaimOrchestrationEventLeases(ctx, now.Add(2*time.Second)); err != nil || count != 1 {
		t.Fatalf("reclaimed=%d err=%v", count, err)
	}
	due, err := s.ListDueOrchestrationEvents(ctx, "p", now.Add(2*time.Second), 50)
	if err != nil || len(due) != 1 {
		t.Fatalf("due=%v err=%v", due, err)
	}
	if due[0].LeaseToken != "batch" || due[0].DestinationSessionID != o.ID {
		t.Fatalf("reclaimed binding=%+v, want stable batch and destination", due[0])
	}
}

func TestOrchestrationRetryPreservesPendingBatchIdentity(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	e := domain.OrchestrationEvent{ID: "retry", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
	if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
		t.Fatalf("enqueue=%v err=%v", ok, err)
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{e.ID}, "batch", o.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	e.LeaseToken = "batch"
	if err := s.RetryOrchestrationEvents(ctx, []domain.OrchestrationEvent{e}, "batch", "transport_error", now, domain.SessionModeChat); err != nil {
		t.Fatal(err)
	}
	due, err := s.ListDueOrchestrationEvents(ctx, "p", now.Add(time.Minute), 50)
	if err != nil || len(due) != 1 || due[0].LeaseToken != "batch" || due[0].DestinationSessionID != o.ID {
		t.Fatalf("due=%v err=%v, want pending batch identity", due, err)
	}
}

func TestOrchestrationRetryUsesOneBackoffForEveryBatchMember(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	events := []domain.OrchestrationEvent{
		{ID: "batch-a", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "a", EnqueuedAt: now, NextAttemptAt: now},
		{ID: "batch-b", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "b", EnqueuedAt: now.Add(time.Second), NextAttemptAt: now},
	}
	for _, event := range events {
		if ok, err := s.EnqueueOrchestrationEvent(ctx, event); err != nil || !ok {
			t.Fatalf("enqueue %s=%v err=%v", event.ID, ok, err)
		}
	}
	ids := []string{events[0].ID, events[1].ID}
	if err := s.LeaseOrchestrationEvents(ctx, ids, "batch", o.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		events[i].LeaseToken = "batch"
	}
	if err := s.RetryOrchestrationEvents(ctx, events, "batch", "transport_error", now, domain.SessionModeChat); err != nil {
		t.Fatal(err)
	}
	due, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(due) != 2 {
		t.Fatalf("events=%+v err=%v", due, err)
	}
	if !due[0].NextAttemptAt.Equal(due[1].NextAttemptAt) {
		t.Fatalf("batch retry times differ: %v and %v", due[0].NextAttemptAt, due[1].NextAttemptAt)
	}
}

func TestOrchestrationRetryUsesConservativeTUIBudgetAndChatBudget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       domain.SessionMode
		attempts   int
		deadLetter bool
	}{
		{name: "tui", mode: domain.SessionModeTUI, attempts: 2, deadLetter: true},
		{name: "chat", mode: domain.SessionModeChat, attempts: 2, deadLetter: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			seedProject(t, s, "p")
			w, err := s.CreateSession(ctx, sampleRecord("p"))
			if err != nil {
				t.Fatal(err)
			}
			o := sampleRecord("p")
			o.Kind, o.Mode = domain.KindOrchestrator, tc.mode
			o, err = s.CreateSession(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			e := domain.OrchestrationEvent{ID: "retry-" + tc.name, ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
			if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
				t.Fatalf("enqueue=%v err=%v", ok, err)
			}
			for i := 0; i < tc.attempts; i++ {
				if err := s.LeaseOrchestrationEvents(ctx, []string{e.ID}, "batch", o.ID, now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				e.LeaseToken = "batch"
				e.AttemptCount = i
				if err := s.RetryOrchestrationEvents(ctx, []domain.OrchestrationEvent{e}, "batch", "transport_error", now, tc.mode); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(got) != 1 {
				t.Fatalf("events=%+v err=%v", got, err)
			}
			if (got[0].State == domain.OrchestrationDeadLetter) != tc.deadLetter {
				t.Fatalf("state=%q, want dead_letter=%v", got[0].State, tc.deadLetter)
			}
		})
	}
}

func TestOrchestrationDeadLetterRearmRestoresTheWholeBatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	events := []domain.OrchestrationEvent{
		{ID: "dead-a", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "a", EnqueuedAt: now.Add(-16 * time.Minute), NextAttemptAt: now},
		{ID: "dead-b", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "b", EnqueuedAt: now.Add(-16 * time.Minute), NextAttemptAt: now},
	}
	for _, event := range events {
		if ok, err := s.EnqueueOrchestrationEvent(ctx, event); err != nil || !ok {
			t.Fatalf("enqueue %s=%v err=%v", event.ID, ok, err)
		}
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{"dead-a", "dead-b"}, "dead-batch", o.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		events[i].LeaseToken = "dead-batch"
	}
	if err := s.RetryOrchestrationEvents(ctx, events, "dead-batch", "transport_error", now, domain.SessionModeChat); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.RetryDeadLetterOrchestrationEvent(ctx, "p", events[0].ID, now.Add(time.Second)); err != nil || !changed {
		t.Fatalf("rearm changed=%v err=%v", changed, err)
	}
	got, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("events=%+v err=%v", got, err)
	}
	for _, event := range got {
		if event.State != domain.OrchestrationPending || event.AttemptCount != 0 || event.LeaseToken != "dead-batch" || event.DestinationSessionID != o.ID {
			t.Fatalf("rearmed event=%+v, want whole batch pending with identity", event)
		}
	}
}

func TestOrchestrationEventStoreLoadsCompleteReclaimedBatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	events := []domain.OrchestrationEvent{
		{ID: "batch-a", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "a", EnqueuedAt: now, NextAttemptAt: now},
		{ID: "new-event", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "new", EnqueuedAt: now.Add(time.Second), NextAttemptAt: now},
		{ID: "batch-c", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerReadyMerge, SourceRevision: "c", EnqueuedAt: now.Add(2 * time.Second), NextAttemptAt: now},
	}
	for _, event := range events {
		if ok, err := s.EnqueueOrchestrationEvent(ctx, event); err != nil || !ok {
			t.Fatalf("enqueue %s=%v err=%v", event.ID, ok, err)
		}
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{"batch-a", "batch-c"}, "reclaimed-batch", o.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if count, err := s.ReclaimOrchestrationEventLeases(ctx, now.Add(2*time.Second)); err != nil || count != 2 {
		t.Fatalf("reclaimed=%d err=%v", count, err)
	}
	due, err := s.ListDueOrchestrationEvents(ctx, "p", now.Add(2*time.Second), 50)
	if err != nil || len(due) != 2 {
		t.Fatalf("due=%v err=%v, want complete reclaimed batch", due, err)
	}
	if due[0].ID != "batch-a" || due[1].ID != "batch-c" {
		t.Fatalf("due ids=%v, want [batch-a batch-c]", []string{due[0].ID, due[1].ID})
	}
}

func TestOrchestrationEventStoreReclaimUsesDestinationModeBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      domain.SessionMode
		wantState domain.OrchestrationDeliveryState
	}{
		{name: "chat", mode: domain.SessionModeChat, wantState: domain.OrchestrationPending},
		{name: "tui", mode: domain.SessionModeTUI, wantState: domain.OrchestrationDeadLetter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			seedProject(t, s, "p")
			w, err := s.CreateSession(ctx, sampleRecord("p"))
			if err != nil {
				t.Fatal(err)
			}
			o := sampleRecord("p")
			o.Kind, o.Mode = domain.KindOrchestrator, tc.mode
			o, err = s.CreateSession(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			e := domain.OrchestrationEvent{ID: "reclaim-" + tc.name, ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
			if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
				t.Fatalf("enqueue=%v err=%v", ok, err)
			}
			for i := 0; i < 2; i++ {
				if err := s.LeaseOrchestrationEvents(ctx, []string{e.ID}, "batch", o.ID, now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ReclaimOrchestrationEventLeases(ctx, now.Add(time.Duration(i+1)*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(got) != 1 || got[0].State != tc.wantState {
				t.Fatalf("events=%+v err=%v, want state %q", got, err, tc.wantState)
			}
		})
	}
}

func TestOrchestrationEventStoreTUIAcknowledgesOnlyExactDestinationAndBatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	e := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
	if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{"e"}, "batch", o.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkOrchestrationEventsSubmitted(ctx, []string{"e"}, "batch", now); err != nil {
		t.Fatal(err)
	}
	if n, err := s.AcknowledgeOrchestrationBatchAccepted(ctx, o.ID, "wrong", now); err != nil || n != 0 {
		t.Fatalf("wrong token n=%d err=%v", n, err)
	}
	if n, err := s.AcknowledgeOrchestrationBatchAccepted(ctx, "other", "batch", now); err != nil || n != 0 {
		t.Fatalf("wrong destination n=%d err=%v", n, err)
	}
	if n, err := s.AcknowledgeOrchestrationBatchAccepted(ctx, o.ID, "batch", now); err != nil || n != 1 {
		t.Fatalf("exact acknowledgement n=%d err=%v", n, err)
	}
	if err := s.MarkOrchestrationEventsSubmitted(ctx, []string{"e"}, "batch", now); err != nil {
		t.Fatalf("early acknowledgement must make submitted marking idempotent: %v", err)
	}
}

func TestOrchestrationSourceStateDedupesAndRearmsSCMTransition(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	created, err := s.RecordOrchestrationSourceState(ctx, "p", w.ID, domain.OrchestrationWorkerReadyMerge, "https://example.invalid/pr/1", true, now)
	if err != nil || !created {
		t.Fatalf("first created=%v err=%v", created, err)
	}
	for i := 0; i < 100; i++ {
		created, err = s.RecordOrchestrationSourceState(ctx, "p", w.ID, domain.OrchestrationWorkerReadyMerge, "https://example.invalid/pr/1", true, now.Add(time.Duration(i)*time.Second))
		if err != nil || created {
			t.Fatalf("repeat %d created=%v err=%v", i, created, err)
		}
	}
	if created, err = s.RecordOrchestrationSourceState(ctx, "p", w.ID, domain.OrchestrationWorkerReadyMerge, "https://example.invalid/pr/1", false, now.Add(time.Minute)); err != nil || created {
		t.Fatalf("clear created=%v err=%v", created, err)
	}
	if created, err = s.RecordOrchestrationSourceState(ctx, "p", w.ID, domain.OrchestrationWorkerReadyMerge, "https://example.invalid/pr/1", true, now.Add(2*time.Minute)); err != nil || !created {
		t.Fatalf("rearm created=%v err=%v", created, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 2 || events[0].SourceRevision == events[1].SourceRevision {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestStartupReconcilesPRFactsThatPrecededOutboxEnqueue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	prURL := "https://example.invalid/pull/42"
	if err := s.WritePR(ctx, domain.PullRequest{URL: prURL, SessionID: w.ID, Number: 42, Merged: true, UpdatedAt: now}, nil, nil); err != nil {
		t.Fatal(err)
	}
	created, err := s.ReconcileTerminatedOrchestrationEvents(ctx, now)
	if err != nil || created != 1 {
		t.Fatalf("created=%d err=%v", created, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 1 || events[0].Kind != domain.OrchestrationPRMerged {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestActivityAndOutboxCommitRollsBackOnInjectedInsertFailure(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	before, ok, err := s.GetSession(ctx, w.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	next := before
	now := time.Now().UTC().Truncate(time.Second)
	next.Activity = domain.Activity{State: domain.ActivityIdle, LastActivityAt: now}
	next.UpdatedAt = now
	event := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: w.ID, Kind: "invalid_kind", SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
	if _, err := s.CommitActivityAndOrchestrationEvent(ctx, next, event); err == nil {
		t.Fatal("invalid event insert succeeded")
	}
	after, ok, err := s.GetSession(ctx, w.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if after.Activity.State != before.Activity.State || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("source fact escaped rollback: before=%+v after=%+v", before.Activity, after.Activity)
	}
}

func TestOrchestrationRetryDeadLettersAndRequiresMatchingProjectForRearm(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind = domain.KindOrchestrator
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	e := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now.Add(-16 * time.Minute), NextAttemptAt: now}
	if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
		t.Fatalf("enqueue=%v err=%v", ok, err)
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{"e"}, "batch", o.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.RetryOrchestrationEvents(ctx, []domain.OrchestrationEvent{e}, "batch", "transient secret\nerror", now, domain.SessionModeChat); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationDeadLetter || events[0].AttentionRequiredAt.IsZero() {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	if changed, err := s.RetryDeadLetterOrchestrationEvent(ctx, "other", "e", now); err != nil || changed {
		t.Fatalf("cross-project changed=%v err=%v", changed, err)
	}
	if changed, err := s.RetryDeadLetterOrchestrationEvent(ctx, "p", "e", now); err != nil || !changed {
		t.Fatalf("matching project changed=%v err=%v", changed, err)
	}
	events, err = s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || events[0].State != domain.OrchestrationPending || events[0].AttemptCount != 0 || !events[0].AttentionRequiredAt.IsZero() {
		t.Fatalf("rearmed events=%+v err=%v", events, err)
	}
}

func TestOrchestrationMissingDestinationAttentionAndRetentionAreDurableAndDeduplicated(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for _, e := range []domain.OrchestrationEvent{
		{ID: "attention", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "a", EnqueuedAt: now.Add(-16 * time.Minute), NextAttemptAt: now},
		{ID: "expired", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "b", EnqueuedAt: now.Add(-31 * 24 * time.Hour), NextAttemptAt: now},
	} {
		if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
			t.Fatalf("enqueue %s=%v err=%v", e.ID, ok, err)
		}
	}
	if n, err := s.MarkProjectNoDestinationAttention(ctx, "p", now); err != nil || n != 2 {
		t.Fatalf("first attention=%d err=%v", n, err)
	}
	if n, err := s.MarkProjectNoDestinationAttention(ctx, "p", now.Add(time.Minute)); err != nil || n != 0 {
		t.Fatalf("deduplicated attention=%d err=%v", n, err)
	}
	if n, err := s.MarkOrchestrationRetentionOverflow(ctx, now); err != nil || n != 1 {
		t.Fatalf("retention=%d err=%v", n, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]domain.OrchestrationEvent{}
	for _, event := range events {
		byID[event.ID] = event
	}
	if byID["expired"].State != domain.OrchestrationDeadLetter || byID["expired"].LastError != "retention limit exceeded" {
		t.Fatalf("expired=%+v", byID["expired"])
	}
	if byID["attention"].State != domain.OrchestrationPending || byID["attention"].AttemptCount != 0 || byID["attention"].AttentionRequiredAt.IsZero() {
		t.Fatalf("attention=%+v", byID["attention"])
	}
	recoverable, err := s.ListOrchestrationEventsRequiringAttention(ctx, "p")
	if err != nil || len(recoverable) != 2 {
		t.Fatalf("recoverable attention=%+v err=%v", recoverable, err)
	}
}

func TestTerminatedEventReconciliationClosesCrashWindowAndRearmsPerLaunch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w := sampleRecord("p")
	w.Metadata.RuntimeLaunchID = "launch-1"
	w, err := s.CreateSession(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	w.IsTerminated = true
	w.Activity.State = domain.ActivityExited
	w.UpdatedAt = time.Now().UTC()
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, w.UpdatedAt); err != nil || n != 1 {
		t.Fatalf("first reconciliation=%d err=%v", n, err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, w.UpdatedAt.Add(time.Second)); err != nil || n != 0 {
		t.Fatalf("repeat reconciliation=%d err=%v", n, err)
	}
	w.IsTerminated = false
	w.Metadata.RuntimeLaunchID = "launch-2"
	w.UpdatedAt = w.UpdatedAt.Add(2 * time.Second)
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.IsTerminated = true
	w.UpdatedAt = w.UpdatedAt.Add(time.Second)
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, w.UpdatedAt); err != nil || n != 1 {
		t.Fatalf("second launch reconciliation=%d err=%v", n, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestTerminatedEventReconciliationUsesStableRevisionWithoutRuntimeIdentity(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	w.IsTerminated = true
	w.Activity.State = domain.ActivityExited
	w.Metadata.RuntimeLaunchID = ""
	w.Metadata.ControllerGeneration = ""
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	w, _, err = s.GetSession(ctx, w.ID)
	if err != nil || w.TerminalRevision != w.Revision {
		t.Fatalf("terminal revision=%d row revision=%d err=%v", w.TerminalRevision, w.Revision, err)
	}
	terminalRevision := w.TerminalRevision
	// Simulate an unrelated observation in the crash window, before any event.
	w.UpdatedAt = w.UpdatedAt.Add(time.Hour)
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, now); err != nil || n != 1 {
		t.Fatalf("first reconciliation=%d err=%v", n, err)
	}
	if inserted, err := s.RecordOrchestrationSourceState(ctx, w.ProjectID, w.ID, domain.OrchestrationWorkerTerminated, "terminal-revision:"+strconv.FormatInt(terminalRevision, 10), true, now); err != nil || inserted {
		t.Fatalf("canonical terminal identity inserted=%v err=%v", inserted, err)
	}
	w.UpdatedAt = w.UpdatedAt.Add(time.Hour)
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, now.Add(time.Hour)); err != nil || n != 0 {
		t.Fatalf("repeat reconciliation=%d err=%v", n, err)
	}
	// A new terminal episode without runtime identity must still rearm.
	w.IsTerminated = false
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.IsTerminated = true
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, now.Add(2*time.Hour)); err != nil || n != 1 {
		t.Fatalf("new terminal episode reconciliation=%d err=%v", n, err)
	}
}

func TestTerminalLifecycleAndReconciliationShareCanonicalRevision(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	m := lifecycle.New(s, nil)
	if err := m.MarkTerminated(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	w, _, err = s.GetSession(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	terminalRevision := w.TerminalRevision
	w.UpdatedAt = w.UpdatedAt.Add(24 * time.Hour)
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkTerminated(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, time.Now().UTC()); err != nil || n != 0 {
		t.Fatalf("reconciliation after lifecycle=%d err=%v", n, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("terminal events=%v err=%v", events, err)
	}
	w, _, err = s.GetSession(ctx, w.ID)
	if err != nil || w.TerminalRevision != terminalRevision || w.Revision <= terminalRevision {
		t.Fatalf("terminal=%d row=%d original=%d err=%v", w.TerminalRevision, w.Revision, terminalRevision, err)
	}
}

type capturedOrchestrationTransport struct {
	batches []orchestrationevents.Batch
	targets []domain.SessionID
	result  orchestrationevents.Submission
}

// Keep the held project first so startup must pass it to reach unrelated work.
// missing models an exact native lookup returning not-found, without deleting
// the recorded destination (the schema's delete action clears that binding).
type startupHoldStore struct {
	*sqlite.Store
	missing domain.SessionID
}

func (s startupHoldStore) ListProjects(context.Context) ([]domain.ProjectRecord, error) {
	return []domain.ProjectRecord{{ID: "p"}, {ID: "q"}}, nil
}

func (s startupHoldStore) GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	if id == s.missing {
		return domain.SessionRecord{}, false, nil
	}
	return s.Store.GetSession(ctx, id)
}

type startupAttentionRecorder struct{ intents []ports.NotificationIntent }

func (n *startupAttentionRecorder) Notify(_ context.Context, intent ports.NotificationIntent) error {
	n.intents = append(n.intents, intent)
	return nil
}

func (*startupAttentionRecorder) Resolve(context.Context, ports.NotificationResolution) error {
	return nil
}

func TestOrchestrationStartupHoldsLegacyLostDestination(t *testing.T) {
	for _, recovery := range []string{"reclaimed_lease", "reclaimed_submission", "rearmed_dead_letter"} {
		t.Run(recovery, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			s := sqlitetest.MustOpenAt(t, dataDir)
			for _, project := range []string{"p", "q"} {
				seedProject(t, s, project)
			}
			now := time.Now().UTC().Truncate(time.Second)
			worker, err := s.CreateSession(ctx, sampleRecord("p"))
			if err != nil {
				t.Fatal(err)
			}
			replacement := sampleRecord("p")
			replacement.Kind, replacement.Mode, replacement.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
			replacement.Activity.State = domain.ActivityIdle
			if _, err := s.CreateSession(ctx, replacement); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "ao.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			attempts := 1
			var submitted any
			if recovery == "reclaimed_submission" {
				submitted = now.Add(-time.Minute)
			}
			if recovery == "rearmed_dead_letter" {
				attempts = 0
			}
			// Seed the state produced by BASE_SHA reclaim/rearm directly. Using
			// the repaired lease/reclaim APIs would miss already-persisted rows:
			// pending + retained token + NULL destination and lease expiry;
			// reclaim increments attempts and retains submitted_at, rearm resets both.
			for _, id := range []string{"legacy-a", "legacy-b"} {
				if _, err := db.ExecContext(ctx, `INSERT INTO orchestration_events
					(id,project_id,worker_id,kind,source_revision,state,attempt_count,enqueued_at,next_attempt_at,lease_token,destination_session_id,lease_expires_at,submitted_at,acknowledged_at,attention_required_at,last_error)
					VALUES(?,'p',?,? ,?,'pending',?,?,?,'legacy-batch',NULL,NULL,?,NULL,NULL,'')`,
					id, worker.ID, domain.OrchestrationWorkerBlocked, id, attempts, now.Add(-time.Minute), now.Add(-time.Second), submitted); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(before) != 2 {
				t.Fatalf("legacy fixture=%v err=%v", before, err)
			}
			transport := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
			notifier := &startupAttentionRecorder{}
			wrapped := startupHoldStore{Store: s} // p must be visited before q.
			d := &orchestrationevents.Dispatcher{Store: wrapped, Transport: transport, Notifier: notifier, Now: func() time.Time { return now }, NewID: func() string { return "fresh-token" }}
			if err := orchestrationevents.Recover(ctx, wrapped, d); err != nil {
				t.Fatalf("startup aborted on legacy lost destination: %v", err)
			}
			if err := d.DispatchProject(ctx, "p"); err != nil {
				t.Fatalf("direct dispatch aborted on legacy lost destination: %v", err)
			}
			if len(transport.batches) != 0 || len(notifier.intents) != 0 {
				t.Fatalf("legacy hold sent or alerted prematurely: sends=%v attention=%v", transport.targets, notifier.intents)
			}
			// A missing destination cannot be repaired by a stale lifecycle ACK
			// or by directly trying to lease the batch to a replacement parent.
			parents, err := s.ListSessions(ctx, "p")
			if err != nil {
				t.Fatal(err)
			}
			for _, parent := range parents {
				if parent.Kind != domain.KindOrchestrator {
					continue
				}
				if n, err := s.AcknowledgeOrchestrationBatchAccepted(ctx, parent.ID, "legacy-batch", now); err != nil || n != 0 {
					t.Fatalf("replacement ACK=%d err=%v", n, err)
				}
				if err := s.LeaseOrchestrationEvents(ctx, []string{"legacy-a", "legacy-b"}, "legacy-batch", parent.ID, now.Add(time.Minute)); err == nil {
					t.Fatal("legacy batch was rebound to replacement")
				}
			}
			otherWorker, err := s.CreateSession(ctx, sampleRecord("q"))
			if err != nil {
				t.Fatal(err)
			}
			otherParent := replacement
			otherParent.ProjectID = "q"
			otherParent, err = s.CreateSession(ctx, otherParent)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := s.EnqueueOrchestrationEvent(ctx, domain.OrchestrationEvent{ID: "unrelated", ProjectID: "q", WorkerID: otherWorker.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "q", EnqueuedAt: now, NextAttemptAt: now}); err != nil || !ok {
				t.Fatalf("enqueue unrelated=%v err=%v", ok, err)
			}
			now = now.Add(16 * time.Minute)
			// Repeated recovery must keep holding p, expose durable attention,
			// deliver q once, and never replay its acknowledged batch.
			for range 2 {
				if err := orchestrationevents.Recover(ctx, wrapped, d); err != nil {
					t.Fatal(err)
				}
			}
			if len(transport.batches) != 1 || transport.targets[0] != otherParent.ID {
				t.Fatalf("replacement sent, unrelated stalled or replayed: targets=%v", transport.targets)
			}
			otherEvents, err := s.ListOrchestrationEvents(ctx, "q", 10)
			if err != nil || len(otherEvents) != 1 || otherEvents[0].State != domain.OrchestrationAcknowledged {
				t.Fatalf("unrelated events=%v err=%v", otherEvents, err)
			}
			held, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(held) != len(before) {
				t.Fatalf("legacy held=%v err=%v", held, err)
			}
			for i, event := range held {
				if event.AttentionRequiredAt.IsZero() {
					t.Fatalf("legacy hold lost delayed attention: %+v", event)
				}
				event.AttentionRequiredAt = before[i].AttentionRequiredAt
				if event != before[i] {
					t.Fatalf("legacy hold changed durable facts: got=%+v before=%+v", event, before[i])
				}
			}
			if len(notifier.intents) == 0 {
				t.Fatal("legacy hold attention never became visible")
			}
			for _, intent := range notifier.intents {
				if intent.SessionID != worker.ID || intent.ProjectID != "p" {
					t.Fatalf("wrong legacy hold attention: %+v", intent)
				}
			}
		})
	}
}

func TestOrchestrationStartupHoldsLegacyMixedAgeBatch(t *testing.T) {
	for _, recovery := range []string{"reclaimed_lease", "reclaimed_submission"} {
		t.Run(recovery, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			s := sqlitetest.MustOpenAt(t, dataDir)
			for _, project := range []string{"p", "q"} {
				seedProject(t, s, project)
			}
			now := time.Now().UTC().Truncate(time.Second)
			worker, err := s.CreateSession(ctx, sampleRecord("p"))
			if err != nil {
				t.Fatal(err)
			}
			replacement := sampleRecord("p")
			replacement.Kind, replacement.Mode, replacement.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
			replacement.Activity.State = domain.ActivityIdle
			replacement, err = s.CreateSession(ctx, replacement)
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "ao.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			var submitted any
			if recovery == "reclaimed_submission" {
				submitted = now.Add(-30 * time.Second)
			}
			// Persist BASE_SHA reclaim's result, not the repaired reclaim API's
			// result. One expired token spans ages 15m01s and 14m59s: reclaim
			// increments both attempts and erases both destinations, but only
			// the older member becomes dead_letter and requires attention.
			for i, state := range []domain.OrchestrationDeliveryState{domain.OrchestrationDeadLetter, domain.OrchestrationPending} {
				var attention any
				if state == domain.OrchestrationDeadLetter {
					attention = now
				}
				id := "legacy-" + strconv.Itoa(i)
				if _, err := db.ExecContext(ctx, `INSERT INTO orchestration_events
					(id,project_id,worker_id,kind,source_revision,state,attempt_count,enqueued_at,next_attempt_at,lease_token,destination_session_id,lease_expires_at,submitted_at,acknowledged_at,attention_required_at,last_error)
					VALUES(?,'p',?,?,?,?,1,?,?,'legacy-mixed',NULL,NULL,?,NULL,?,'')`,
					id, worker.ID, domain.OrchestrationWorkerBlocked, id, state, now.Add(-15*time.Minute-time.Second+time.Duration(i)*2*time.Second), now, submitted, attention); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(before) != 2 || before[0].State != domain.OrchestrationPending || before[1].State != domain.OrchestrationDeadLetter || before[1].AttentionRequiredAt.IsZero() {
				t.Fatalf("mixed fixture=%v err=%v", before, err)
			}
			otherWorker, err := s.CreateSession(ctx, sampleRecord("q"))
			if err != nil {
				t.Fatal(err)
			}
			otherParent := replacement
			otherParent.ProjectID = "q"
			otherParent, err = s.CreateSession(ctx, otherParent)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := s.EnqueueOrchestrationEvent(ctx, domain.OrchestrationEvent{ID: "unrelated", ProjectID: "q", WorkerID: otherWorker.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "q", EnqueuedAt: now, NextAttemptAt: now}); err != nil || !ok {
				t.Fatalf("enqueue unrelated=%v err=%v", ok, err)
			}
			transport := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
			notifier := &startupAttentionRecorder{}
			wrapped := startupHoldStore{Store: s} // Held p must precede deliverable q.
			d := &orchestrationevents.Dispatcher{Store: wrapped, Transport: transport, Notifier: notifier, Now: func() time.Time { return now }}
			for range 2 {
				if err := orchestrationevents.Recover(ctx, wrapped, d); err != nil {
					t.Fatalf("mixed legacy batch aborted startup: %v", err)
				}
				if err := d.DispatchProject(ctx, "p"); err != nil {
					t.Fatalf("mixed legacy batch aborted direct dispatch: %v", err)
				}
			}
			due, err := s.ListDueOrchestrationEvents(ctx, "p", now, orchestrationevents.MaxBatchEvents)
			if err != nil || len(due) != 2 || due[0].ID != "legacy-0" || due[1].ID != "legacy-1" {
				t.Fatalf("complete legacy mixed batch unavailable for HOLD: due=%v err=%v", due, err)
			}
			if len(transport.batches) != 1 || transport.targets[0] != otherParent.ID {
				t.Fatalf("replacement sent, later project stalled or replayed: targets=%v", transport.targets)
			}
			other, err := s.ListOrchestrationEvents(ctx, "q", 10)
			if err != nil || len(other) != 1 || other[0].State != domain.OrchestrationAcknowledged {
				t.Fatalf("later project events=%v err=%v", other, err)
			}
			if n, err := s.AcknowledgeOrchestrationBatchAccepted(ctx, replacement.ID, "legacy-mixed", now); err != nil || n != 0 {
				t.Fatalf("replacement ACK=%d err=%v", n, err)
			}
			if err := s.LeaseOrchestrationEvents(ctx, []string{"legacy-0", "legacy-1"}, "legacy-mixed", replacement.ID, now.Add(time.Minute)); err == nil {
				t.Fatal("mixed legacy batch rebound to replacement")
			}
			held, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(held) != len(before) {
				t.Fatalf("held=%v err=%v", held, err)
			}
			for i, event := range held {
				if event != before[i] {
					t.Fatalf("HOLD changed durable facts: got=%+v before=%+v", event, before[i])
				}
			}
			// The younger pending member also acquires attention after aging;
			// the original dead-letter attention timestamp remains untouched.
			now = now.Add(2 * time.Second)
			for range 2 {
				if err := orchestrationevents.Recover(ctx, wrapped, d); err != nil {
					t.Fatal(err)
				}
			}
			held, err = s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(held) != 2 || held[0].AttentionRequiredAt.IsZero() {
				t.Fatalf("delayed attention lost: held=%v err=%v", held, err)
			}
			held[0].AttentionRequiredAt = before[0].AttentionRequiredAt
			if held[0] != before[0] || held[1] != before[1] || len(transport.batches) != 1 || len(notifier.intents) == 0 {
				t.Fatalf("HOLD lost facts/attention or sent again: held=%v sends=%v alerts=%v", held, transport.targets, notifier.intents)
			}
			for _, intent := range notifier.intents {
				if intent.ProjectID != "p" || intent.SessionID != worker.ID {
					t.Fatalf("wrong HOLD attention: %+v", intent)
				}
			}
			// The exception admits only destination-less pending/dead_letter
			// membership. Other states and bound mixed batches keep their fence.
			for _, state := range []domain.OrchestrationDeliveryState{domain.OrchestrationLeased, domain.OrchestrationSubmitted, domain.OrchestrationAcknowledged} {
				if _, err := db.ExecContext(ctx, `UPDATE orchestration_events SET state=? WHERE id='legacy-0'`, state); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ListDueOrchestrationEvents(ctx, "p", now, orchestrationevents.MaxBatchEvents); err == nil {
					t.Fatalf("lost-destination batch admitted %s member", state)
				}
			}
			if _, err := db.ExecContext(ctx, `UPDATE orchestration_events SET destination_session_id=?,state=CASE WHEN id='legacy-0' THEN 'dead_letter' ELSE 'pending' END WHERE project_id='p'`, replacement.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ListDueOrchestrationEvents(ctx, "p", now, orchestrationevents.MaxBatchEvents); err == nil {
				t.Fatal("bound batch admitted dead_letter member")
			}
		})
	}
}

func TestOrchestrationStartupHoldsUnavailableRecordedDestination(t *testing.T) {
	for _, recovery := range []string{"leased", "submitted", "rearmed"} {
		for _, unavailable := range []string{"retired", "blocked", "waiting_input", "exited", "startup", "busy_tui", "missing"} {
			t.Run(recovery+"/"+unavailable, func(t *testing.T) {
				ctx := context.Background()
				s := newTestStore(t)
				for _, project := range []string{"p", "q"} {
					seedProject(t, s, project)
				}
				now := time.Now().UTC().Truncate(time.Second)
				worker, err := s.CreateSession(ctx, sampleRecord("p"))
				if err != nil {
					t.Fatal(err)
				}
				parent := sampleRecord("p")
				parent.Kind, parent.Mode, parent.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
				if unavailable == "busy_tui" {
					parent.Mode = domain.SessionModeTUI
				}
				parent.Activity.State = domain.ActivityIdle
				parent, err = s.CreateSession(ctx, parent)
				if err != nil {
					t.Fatal(err)
				}
				enqueued := now.Add(-time.Minute)
				if recovery == "rearmed" {
					enqueued = now.Add(-16 * time.Minute)
				}
				for _, id := range []string{"held-a", "held-b"} {
					ok, err := s.EnqueueOrchestrationEvent(ctx, domain.OrchestrationEvent{ID: id, ProjectID: "p", WorkerID: worker.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: id, EnqueuedAt: enqueued, NextAttemptAt: enqueued})
					if err != nil || !ok {
						t.Fatalf("enqueue=%v err=%v", ok, err)
					}
				}
				ids := []string{"held-a", "held-b"}
				if err := s.LeaseOrchestrationEvents(ctx, ids, "original-token", parent.ID, now.Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
				if recovery == "submitted" {
					if err := s.MarkOrchestrationEventsSubmitted(ctx, ids, "original-token", enqueued); err != nil {
						t.Fatal(err)
					}
				}
				if recovery == "rearmed" {
					if n, err := s.ReclaimOrchestrationEventLeases(ctx, now); err != nil || n != 2 {
						t.Fatalf("reclaim=%d err=%v", n, err)
					}
					if ok, err := s.RetryDeadLetterOrchestrationEvent(ctx, "p", ids[0], now); err != nil || !ok {
						t.Fatalf("rearm=%v err=%v", ok, err)
					}
				}
				wrapped := startupHoldStore{Store: s}
				switch unavailable {
				case "retired":
					parent.IsTerminated = true
				case "blocked":
					parent.Activity.State = domain.ActivityBlocked
				case "waiting_input":
					parent.Activity.State = domain.ActivityWaitingInput
				case "exited":
					parent.Activity.State = domain.ActivityExited
				case "startup":
					parent.FirstSignalAt = time.Time{}
				case "busy_tui":
					parent.Activity.State = domain.ActivityActive
				case "missing":
					wrapped.missing = parent.ID
				}
				if err := s.UpdateSession(ctx, parent); err != nil {
					t.Fatal(err)
				}
				replacement := sampleRecord("p")
				replacement.Kind, replacement.Mode, replacement.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
				replacement.Activity.State = domain.ActivityIdle
				if _, err := s.CreateSession(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				transport := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
				notifier := &startupAttentionRecorder{}
				// Recover uses wall time for reclaim's next_attempt_at. Dispatch
				// just beyond that instant so this first scan exercises the hold.
				now = now.Add(time.Minute)
				d := &orchestrationevents.Dispatcher{Store: wrapped, Transport: transport, Notifier: notifier, Now: func() time.Time { return now }, NewID: func() string { return "fresh-token" }}
				if err := orchestrationevents.Recover(ctx, wrapped, d); err != nil {
					t.Fatalf("startup aborted on held destination: %v", err)
				}
				if len(transport.batches) != 0 {
					t.Fatalf("held startup sent: %v", transport.targets)
				}
				wantAttention := recovery == "rearmed"
				if (len(notifier.intents) != 0) != wantAttention {
					t.Fatalf("premature/missing attention: %v", notifier.intents)
				}
				for _, intent := range notifier.intents {
					if intent.SessionID != worker.ID || intent.ProjectID != "p" {
						t.Fatalf("wrong attention: %+v", intent)
					}
				}
				// The next startup scan must still deliver work from a later project.
				otherWorker, err := s.CreateSession(ctx, sampleRecord("q"))
				if err != nil {
					t.Fatal(err)
				}
				otherParent := replacement
				otherParent.ID, otherParent.ProjectID = "", "q"
				otherParent, err = s.CreateSession(ctx, otherParent)
				if err != nil {
					t.Fatal(err)
				}
				if ok, err := s.EnqueueOrchestrationEvent(ctx, domain.OrchestrationEvent{ID: "unrelated", ProjectID: "q", WorkerID: otherWorker.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "q", EnqueuedAt: enqueued, NextAttemptAt: enqueued}); err != nil || !ok {
					t.Fatalf("enqueue unrelated=%v err=%v", ok, err)
				}
				now = now.Add(16 * time.Minute)
				if err := orchestrationevents.Recover(ctx, wrapped, d); err != nil {
					t.Fatal(err)
				}
				if len(transport.batches) != 1 || transport.targets[0] != otherParent.ID {
					t.Fatalf("unrelated project stalled or replacement sent: %v", transport.targets)
				}
				otherEvents, err := s.ListOrchestrationEvents(ctx, "q", 10)
				if err != nil || len(otherEvents) != 1 || otherEvents[0].State != domain.OrchestrationAcknowledged {
					t.Fatalf("unrelated events=%+v err=%v", otherEvents, err)
				}
				held, err := s.ListOrchestrationEvents(ctx, "p", 10)
				if err != nil || len(held) != 2 {
					t.Fatalf("held=%v err=%v", held, err)
				}
				for _, event := range held {
					wantAttempts := 1
					if recovery == "rearmed" {
						wantAttempts = 0
					}
					if event.State != domain.OrchestrationPending || event.LeaseToken != "original-token" || event.DestinationSessionID != parent.ID || event.AttemptCount != wantAttempts || !event.LeaseExpiresAt.IsZero() || !event.AcknowledgedAt.IsZero() || event.AttentionRequiredAt.IsZero() {
						t.Fatalf("hold changed binding/delivery facts or lost delayed attention: %+v", event)
					}
				}
				if len(notifier.intents) == 0 {
					t.Fatal("delayed attention missing")
				}
			})
		}
	}
}

func TestPersistedBatchOnePastEventLimitFailsClosed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord("p")
	o.Kind, o.Mode = domain.KindOrchestrator, domain.SessionModeChat
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ids := make([]string, orchestrationevents.MaxBatchEvents+1)
	for i := range ids {
		ids[i] = "event-" + strconv.Itoa(i)
		event := domain.OrchestrationEvent{ID: ids[i], ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: ids[i], EnqueuedAt: now, NextAttemptAt: now}
		if inserted, err := s.EnqueueOrchestrationEvent(ctx, event); err != nil || !inserted {
			t.Fatalf("enqueue=%v err=%v", inserted, err)
		}
	}
	// Fresh work is bounded normally; persisted membership must never be cut.
	if events, err := s.ListDueOrchestrationEvents(ctx, "p", now, orchestrationevents.MaxBatchEvents); err != nil || len(events) != orchestrationevents.MaxBatchEvents {
		t.Fatalf("fresh events=%d err=%v", len(events), err)
	}
	if err := s.LeaseOrchestrationEvents(ctx, ids, "oversized-parent-batch", o.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReclaimOrchestrationEventLeases(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListDueOrchestrationEvents(ctx, "p", now.Add(2*time.Second), orchestrationevents.MaxBatchEvents); err == nil {
		t.Fatal("persisted batch above event limit was silently truncated")
	}
}

func TestUnknownLegacyTerminalRevisionFailsClosedWithoutEvent(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dataDir)
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	w.IsTerminated = true
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `UPDATE sessions SET terminal_revision=-1,revision=revision+1 WHERE id=?`, w.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReconcileTerminatedOrchestrationEvents(ctx, time.Now().UTC()); err == nil {
		t.Fatal("unknown legacy terminal revision was accepted")
	}
	if err := lifecycle.New(s, nil).MarkTerminated(ctx, w.ID); err == nil {
		t.Fatal("lifecycle accepted unknown legacy terminal revision")
	}
	if events, err := s.ListOrchestrationEvents(ctx, "p", 10); err != nil || len(events) != 0 {
		t.Fatalf("unproven terminal events=%v err=%v", events, err)
	}
	// An actual controller identity does not need the legacy revision fallback.
	w, _, err = s.GetSession(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	w.Metadata.ControllerGeneration = "controller-legacy"
	if err := s.UpdateSession(ctx, w); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReconcileTerminatedOrchestrationEvents(ctx, time.Now().UTC()); err != nil || n != 1 {
		t.Fatalf("controller-identified reconciliation=%d err=%v", n, err)
	}
}

func (t *capturedOrchestrationTransport) Submit(_ context.Context, target domain.SessionRecord, batch orchestrationevents.Batch) (orchestrationevents.Submission, error) {
	t.batches = append(t.batches, batch)
	t.targets = append(t.targets, target.ID)
	return t.result, nil
}

func TestParentEraBatchRecoveryPreservesCompletePayload(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dataDir)
	seedProject(t, s, "p")
	w, err := s.CreateSession(ctx, sampleRecord("p"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	o := sampleRecord("p")
	o.Kind, o.Mode, o.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
	o.Activity.State = domain.ActivityIdle
	if _, err := s.CreateSession(ctx, o); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"batch-a", "batch-c"} {
		event := domain.OrchestrationEvent{ID: id, ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: id, EnqueuedAt: now.Add(time.Duration(i) * time.Second), NextAttemptAt: now}
		if inserted, err := s.EnqueueOrchestrationEvent(ctx, event); err != nil || !inserted {
			t.Fatalf("enqueue=%v err=%v", inserted, err)
		}
	}
	transport := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true}}
	dispatcher := orchestrationevents.Dispatcher{Store: s, Transport: transport, Now: func() time.Time { return now }, NewID: func() string { return "parent-era-batch" }}
	if err := dispatcher.DispatchProject(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if len(transport.batches) != 1 || len(transport.batches[0].EventIDs) != 2 {
		t.Fatalf("original batches=%v", transport.batches)
	}
	now = now.Add(time.Minute)
	if count, err := s.ReclaimOrchestrationEventLeases(ctx, now); err != nil || count != 2 {
		t.Fatalf("reclaimed=%d err=%v", count, err)
	}
	// Persist parent-era per-member backoff; one member is not currently due.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `UPDATE orchestration_events SET next_attempt_at=? WHERE id='batch-c'`, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	seedProject(t, s, "foreign")
	foreignWorker, err := s.CreateSession(ctx, sampleRecord("foreign"))
	if err != nil {
		t.Fatal(err)
	}
	foreign := domain.OrchestrationEvent{ID: "foreign-event", ProjectID: "foreign", WorkerID: foreignWorker.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "foreign", EnqueuedAt: now, NextAttemptAt: now}
	if inserted, err := s.EnqueueOrchestrationEvent(ctx, foreign); err != nil || !inserted {
		t.Fatalf("enqueue foreign=%v err=%v", inserted, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE orchestration_events SET lease_token='parent-era-batch' WHERE id='foreign-event'`); err != nil {
		t.Fatal(err)
	}
	newEvent := domain.OrchestrationEvent{ID: "new-event", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "new", EnqueuedAt: now.Add(-time.Second), NextAttemptAt: now}
	if inserted, err := s.EnqueueOrchestrationEvent(ctx, newEvent); err != nil || !inserted {
		t.Fatalf("enqueue new=%v err=%v", inserted, err)
	}
	if _, err := s.ListDueOrchestrationEvents(ctx, "p", now, 1); err == nil {
		t.Fatal("undersized limit silently split a persisted batch")
	}
	for _, limit := range []int{0, -1} {
		if events, err := s.ListDueOrchestrationEvents(ctx, "p", now, limit); err != nil || len(events) != 0 {
			t.Fatalf("limit=%d events=%v err=%v", limit, events, err)
		}
	}
	if events, err := s.ListDueOrchestrationEvents(ctx, "p", now.Add(-time.Hour), 50); err != nil || len(events) != 0 {
		t.Fatalf("not-yet-due events=%v err=%v", events, err)
	}
	transport.result.Acknowledged = true
	if err := dispatcher.DispatchProject(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if len(transport.batches) != 2 || transport.batches[1].ID != transport.batches[0].ID || transport.batches[1].Payload != transport.batches[0].Payload || strings.Join(transport.batches[1].EventIDs, ",") != "batch-a,batch-c" {
		t.Fatalf("recovered batches=%v", transport.batches)
	}
	foreignEvents, err := s.ListOrchestrationEvents(ctx, "foreign", 50)
	if err != nil || len(foreignEvents) != 1 || foreignEvents[0].State != domain.OrchestrationPending {
		t.Fatalf("foreign batch changed: events=%v err=%v", foreignEvents, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE orchestration_events SET state='pending',next_attempt_at=? WHERE id='batch-a'`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListDueOrchestrationEvents(ctx, "p", now, 50); err == nil {
		t.Fatal("partially acknowledged batch was accepted for replay")
	}
}

func TestDispatcherAcceptsSynchronousTUIHookAcknowledgement(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "p")
	w, _ := s.CreateSession(ctx, sampleRecord("p"))
	o := sampleRecord("p")
	o.Kind, o.Mode, o.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeTUI, time.Now().UTC()
	o.Activity.State = domain.ActivityIdle
	o, _ = s.CreateSession(ctx, o)
	now := time.Now().UTC()
	e := domain.OrchestrationEvent{ID: "e-sync", ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now, NextAttemptAt: now}
	if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
		t.Fatalf("enqueue=%v err=%v", ok, err)
	}
	dispatcher := orchestrationevents.Dispatcher{Store: s, Transport: synchronousAckTransport{store: s}, Now: func() time.Time { return now }, NewID: func() string { return "batch-sync" }}
	if err := dispatcher.DispatchProject(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationAcknowledged {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestOrchestrationDestinationFenceAcrossRecovery(t *testing.T) {
	for _, recovery := range []string{"crash_before_send", "submitted_without_ack", "dead_letter_rearm"} {
		for _, retired := range []bool{false, true} {
			t.Run(recovery+"/retired="+strconv.FormatBool(retired), func(t *testing.T) {
				ctx := context.Background()
				s := newTestStore(t)
				seedProject(t, s, "p")
				w, err := s.CreateSession(ctx, sampleRecord("p"))
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Truncate(time.Second)
				o := sampleRecord("p")
				o.Kind, o.Mode, o.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
				o.Activity.State = domain.ActivityIdle
				o, err = s.CreateSession(ctx, o)
				if err != nil {
					t.Fatal(err)
				}
				enqueued := now
				if recovery == "dead_letter_rearm" {
					enqueued = now.Add(-16 * time.Minute)
				}
				for _, id := range []string{"a", "b"} {
					e := domain.OrchestrationEvent{ID: id, ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: id, EnqueuedAt: enqueued, NextAttemptAt: now}
					if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
						t.Fatalf("enqueue=%v err=%v", ok, err)
					}
				}
				ids := []string{"a", "b"}
				if err := s.LeaseOrchestrationEvents(ctx, ids, "retained-batch", o.ID, now.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if recovery == "submitted_without_ack" {
					if err := s.MarkOrchestrationEventsSubmitted(ctx, ids, "retained-batch", now); err != nil {
						t.Fatal(err)
					}
				}
				now = now.Add(time.Minute)
				if n, err := s.ReclaimOrchestrationEventLeases(ctx, now); err != nil || n != 2 {
					t.Fatalf("reclaim=%d err=%v", n, err)
				}
				if recovery == "dead_letter_rearm" {
					if changed, err := s.RetryDeadLetterOrchestrationEvent(ctx, "p", "a", now); err != nil || !changed {
						t.Fatalf("rearm=%v err=%v", changed, err)
					}
				}
				due, err := s.ListDueOrchestrationEvents(ctx, "p", now, 50)
				if err != nil || len(due) != 2 {
					t.Fatalf("due=%v err=%v", due, err)
				}
				for _, e := range due {
					if e.DestinationSessionID != o.ID || e.LeaseToken != "retained-batch" {
						t.Fatalf("lost binding: %+v", e)
					}
				}
				payload, _ := orchestrationevents.Prompt("retained-batch", due)
				if retired {
					o.IsTerminated = true
					if err := s.UpdateSession(ctx, o); err != nil {
						t.Fatal(err)
					}
					replacement := sampleRecord("p")
					replacement.Kind, replacement.Mode, replacement.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
					replacement.Activity.State = domain.ActivityIdle
					if _, err := s.CreateSession(ctx, replacement); err != nil {
						t.Fatal(err)
					}
				}
				transport := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
				d := orchestrationevents.Dispatcher{Store: s, Transport: transport, Now: func() time.Time { return now }, NewID: func() string { t.Fatal("retry replaced batch token"); return "" }}
				err = d.DispatchProject(ctx, "p")
				if retired {
					if err != nil || len(transport.batches) != 0 {
						t.Fatalf("retired err=%v batches=%v", err, transport.batches)
					}
				} else if err != nil || len(transport.batches) != 1 || transport.targets[0] != o.ID || transport.batches[0].ID != "retained-batch" || transport.batches[0].Payload != payload {
					t.Fatalf("err=%v batches=%v targets=%v", err, transport.batches, transport.targets)
				}
				got, err := s.ListOrchestrationEvents(ctx, "p", 10)
				if err != nil || len(got) != 2 {
					t.Fatalf("events=%v err=%v", got, err)
				}
				for _, e := range got {
					want := domain.OrchestrationAcknowledged
					if retired {
						want = domain.OrchestrationPending
					}
					if e.State != want || e.DestinationSessionID != o.ID || e.LeaseToken != "retained-batch" || (retired && !e.AcknowledgedAt.IsZero()) {
						t.Fatalf("event=%+v want=%s", e, want)
					}
				}
				if !retired {
					if err := d.DispatchProject(ctx, "p"); err != nil || len(transport.batches) != 1 {
						t.Fatalf("ack replay err=%v batches=%v", err, transport.batches)
					}
				}
			})
		}
	}
}

// Mutate the fixture after the dispatcher's due read, at the lease boundary.
type orchestrationLeaseRaceStore struct {
	orchestrationevents.Store
	beforeLease func()
}

func (s orchestrationLeaseRaceStore) LeaseOrchestrationEvents(ctx context.Context, ids []string, token string, destination domain.SessionID, expires time.Time) error {
	s.beforeLease()
	return s.Store.LeaseOrchestrationEvents(ctx, ids, token, destination, expires)
}

func TestOrchestrationLeaseDestinationFenceRejectsStaleReads(t *testing.T) {
	for _, mutation := range []string{"changed_destination", "changed_token", "missing_destination", "retired_destination", "fresh_read_then_claimed_and_reclaimed"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			s := sqlitetest.MustOpenAt(t, dataDir)
			seedProject(t, s, "p")
			w, err := s.CreateSession(ctx, sampleRecord("p"))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			o := sampleRecord("p")
			o.Kind, o.Mode, o.FirstSignalAt = domain.KindOrchestrator, domain.SessionModeChat, now
			o.Activity.State = domain.ActivityIdle
			o, err = s.CreateSession(ctx, o)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"a", "b"} {
				e := domain.OrchestrationEvent{ID: id, ProjectID: "p", WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: id, EnqueuedAt: now, NextAttemptAt: now}
				if ok, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil || !ok {
					t.Fatalf("enqueue=%v err=%v", ok, err)
				}
			}
			ids := []string{"a", "b"}
			if mutation != "fresh_read_then_claimed_and_reclaimed" {
				if err := s.LeaseOrchestrationEvents(ctx, ids, "original-batch", o.ID, now.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ReclaimOrchestrationEventLeases(ctx, now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Minute)
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "ao.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			raceStore := orchestrationLeaseRaceStore{Store: s, beforeLease: func() {
				var err error
				switch mutation {
				case "changed_destination":
					_, err = db.ExecContext(ctx, `UPDATE orchestration_events SET destination_session_id=? WHERE id='b'`, w.ID)
				case "changed_token":
					_, err = db.ExecContext(ctx, `UPDATE orchestration_events SET lease_token='changed-batch' WHERE id='b'`)
				case "missing_destination":
					_, err = db.ExecContext(ctx, `UPDATE orchestration_events SET destination_session_id=NULL WHERE id='b'`)
				case "retired_destination":
					o.IsTerminated = true
					err = s.UpdateSession(ctx, o)
				case "fresh_read_then_claimed_and_reclaimed":
					err = s.LeaseOrchestrationEvents(ctx, ids, "competing-batch", o.ID, now.Add(time.Second))
					if err == nil {
						_, err = s.ReclaimOrchestrationEventLeases(ctx, now.Add(time.Minute))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}}
			transport := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
			d := orchestrationevents.Dispatcher{Store: raceStore, Transport: transport, Now: func() time.Time { return now }, NewID: func() string { return "fresh-batch" }}
			if err := d.DispatchProject(ctx, "p"); err == nil || len(transport.batches) != 0 {
				t.Fatalf("stale lease err=%v batches=%v", err, transport.batches)
			}
			if mutation == "changed_destination" || mutation == "missing_destination" {
				// The now-inconsistent persisted batch must also fail before leasing
				// on the next read, rather than selecting either member's target.
				d.Store = s
				if err := d.DispatchProject(ctx, "p"); err == nil || len(transport.batches) != 0 {
					t.Fatalf("inconsistent batch err=%v batches=%v", err, transport.batches)
				}
			}
			got, err := s.ListOrchestrationEvents(ctx, "p", 10)
			if err != nil || len(got) != 2 {
				t.Fatalf("events=%v err=%v", got, err)
			}
			for _, e := range got {
				if e.State != domain.OrchestrationPending || !e.AcknowledgedAt.IsZero() || !e.LeaseExpiresAt.IsZero() {
					t.Fatalf("failed lease escaped rollback: %+v", e)
				}
			}
		})
	}
}
