package store_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
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
	if due[0].LeaseToken != "batch" {
		t.Fatalf("reclaimed lease token=%q, want stable batch token", due[0].LeaseToken)
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
	if err != nil || len(due) != 1 || due[0].LeaseToken != "batch" {
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
		if event.State != domain.OrchestrationPending || event.AttemptCount != 0 || event.LeaseToken != "dead-batch" {
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
	result  orchestrationevents.Submission
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

func (t *capturedOrchestrationTransport) Submit(_ context.Context, _ domain.SessionRecord, batch orchestrationevents.Batch) (orchestrationevents.Submission, error) {
	t.batches = append(t.batches, batch)
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

func seedDispatchHoldProject(t *testing.T, s *sqlite.Store, project string) (domain.SessionRecord, domain.OrchestrationEvent) {
	t.Helper()
	ctx := context.Background()
	seedProject(t, s, project)
	w, err := s.CreateSession(ctx, sampleRecord(project))
	if err != nil {
		t.Fatal(err)
	}
	o := sampleRecord(project)
	o.Kind = domain.KindOrchestrator
	o.Mode = domain.SessionModeChat
	o.Activity.State = domain.ActivityIdle
	o.FirstSignalAt = time.Now()
	o, err = s.CreateSession(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	e := domain.OrchestrationEvent{ID: project + "-event", ProjectID: domain.ProjectID(project), WorkerID: w.ID, Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "original", EnqueuedAt: now, NextAttemptAt: now}
	if _, err := s.EnqueueOrchestrationEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	return o, e
}
func TestDispatchHoldPersistenceScopeAndExplicitRelease(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	o, e := seedDispatchHoldProject(t, s, "ecp")
	seedDispatchHoldProject(t, s, "other")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "ao.db")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE orchestration_events SET attempt_count=2,attention_required_at=?,last_error='original retry' WHERE id=?`, time.Now().UTC(), e.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := s.ListOrchestrationEvents(ctx, "ecp", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetOrchestrationDispatchHold(ctx, "ecp", true); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{e.ID}, "forbidden", o.ID, time.Now().Add(time.Minute)); !errors.Is(err, orchestrationevents.ErrDispatchHeld) {
		t.Fatalf("lease err=%v", err)
	}
	tr := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
	d := &orchestrationevents.Dispatcher{Store: s, Transport: tr}
	if err := d.DispatchProject(ctx, "ecp"); !errors.Is(err, orchestrationevents.ErrDispatchHeld) {
		t.Fatalf("dispatch err=%v", err)
	}
	if len(tr.batches) != 0 {
		t.Fatal("held project submitted")
	}
	if err := d.DispatchProject(ctx, "other"); err != nil {
		t.Fatal(err)
	}
	if len(tr.batches) != 1 {
		t.Fatal("other project did not submit")
	}
	if _, err := s.ReclaimOrchestrationEventLeases(ctx, time.Now().Add(40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkOrchestrationRetentionOverflow(ctx, time.Now().Add(40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	after, err := s.ListOrchestrationEvents(ctx, "ecp", 10)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("held backlog changed: %v %v", after, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	held, n, err := s.ReadOrchestrationDispatchHold(ctx, "ecp")
	if err != nil || !held || n != 0 {
		t.Fatalf("restart held=%v outstanding=%d err=%v", held, n, err)
	}
	d.Store = s
	if err := d.DispatchProject(ctx, "ecp"); !errors.Is(err, orchestrationevents.ErrDispatchHeld) {
		t.Fatalf("restart dispatch=%v", err)
	}
	if err := s.SetOrchestrationDispatchHold(ctx, "ecp", false); err != nil {
		t.Fatal(err)
	}
	if len(tr.batches) != 1 {
		t.Fatal("release itself submitted")
	}
	if err := d.DispatchProject(ctx, "ecp"); err != nil {
		t.Fatal(err)
	}
	if len(tr.batches) != 2 {
		t.Fatal("explicit release did not permit dispatch")
	}
	if _, err := s.OrchestrationDispatchHeld(ctx, "missing"); err == nil {
		t.Fatal("unknown project admitted")
	}
	if err := s.SetOrchestrationDispatchHold(ctx, "missing", false); err == nil {
		t.Fatal("unknown project released")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if held, err := s.OrchestrationDispatchHeld(ctx, "ecp"); err == nil || !held {
		t.Fatal("storage failure did not fail closed")
	}
}

type holdAfterLeaseStore struct{ *sqlite.Store }

func (s holdAfterLeaseStore) LeaseOrchestrationEvents(ctx context.Context, ids []string, token string, dest domain.SessionID, expiry time.Time) error {
	if err := s.Store.LeaseOrchestrationEvents(ctx, ids, token, dest, expiry); err != nil {
		return err
	}
	return s.SetOrchestrationDispatchHold(ctx, "ecp", true)
}
func TestDispatchHoldWinsAfterLeasePreservesOutstanding(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	_, e := seedDispatchHoldProject(t, s, "ecp")
	tr := &capturedOrchestrationTransport{}
	err := (&orchestrationevents.Dispatcher{Store: holdAfterLeaseStore{s}, Transport: tr}).DispatchProject(ctx, "ecp")
	if !errors.Is(err, orchestrationevents.ErrDispatchHeld) || len(tr.batches) != 0 {
		t.Fatalf("err=%v sends=%d", err, len(tr.batches))
	}
	held, n, err := s.ReadOrchestrationDispatchHold(ctx, "ecp")
	if err != nil || !held || n != 1 {
		t.Fatalf("held=%v n=%d err=%v", held, n, err)
	}
	before, err := s.ListOrchestrationEvents(ctx, "ecp", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || before[0].ID != e.ID || before[0].AttemptCount != 0 || before[0].State != domain.OrchestrationLeased {
		t.Fatalf("%+v", before)
	}
	if _, err := s.ReclaimOrchestrationEventLeases(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	after, err := s.ListOrchestrationEvents(ctx, "ecp", 10)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("lease changed while held: %v %v", after, err)
	}
}

type blockedHoldTransport struct{ entered, finish chan struct{} }

func (b blockedHoldTransport) Submit(ctx context.Context, _ domain.SessionRecord, _ orchestrationevents.Batch) (orchestrationevents.Submission, error) {
	close(b.entered)
	select {
	case <-ctx.Done():
		return orchestrationevents.Submission{}, ctx.Err()
	case <-b.finish:
		return orchestrationevents.Submission{Submitted: true}, nil
	}
}
func TestDispatchHoldWaitsForInFlightSubmissionAndLeavesOtherProjectOperational(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := newTestStore(t)
	seedDispatchHoldProject(t, s, "ecp")
	seedDispatchHoldProject(t, s, "other")
	tr := blockedHoldTransport{make(chan struct{}), make(chan struct{})}
	sent := make(chan error, 1)
	go func() { sent <- (&orchestrationevents.Dispatcher{Store: s, Transport: tr}).DispatchProject(ctx, "ecp") }()
	select {
	case <-tr.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	holdCtx, holdCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	if err := s.SetOrchestrationDispatchHold(holdCtx, "ecp", true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("in-flight hold claimed success: %v", err)
	}
	holdCancel()
	other := &capturedOrchestrationTransport{result: orchestrationevents.Submission{Submitted: true, Acknowledged: true}}
	if err := (&orchestrationevents.Dispatcher{Store: s, Transport: other}).DispatchProject(ctx, "other"); err != nil || len(other.batches) != 1 {
		t.Fatalf("other project blocked: %v", err)
	}
	heldDone := make(chan error, 1)
	go func() { heldDone <- s.SetOrchestrationDispatchHold(ctx, "ecp", true) }()
	close(tr.finish)
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-heldDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	held, n, err := s.ReadOrchestrationDispatchHold(ctx, "ecp")
	if err != nil || !held || n != 1 {
		t.Fatalf("lost submitted truth: %v %d %v", held, n, err)
	}
	if err := (&orchestrationevents.Dispatcher{Store: s, Transport: other}).DispatchProject(ctx, "ecp"); !errors.Is(err, orchestrationevents.ErrDispatchHeld) {
		t.Fatalf("held err=%v", err)
	}
}

func TestDispatchHoldForeign169AdoptionAndCollision(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(strconv.FormatBool(collision), func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			seedProject(t, s, "ecp")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "ao.db")))
			if err != nil {
				t.Fatal(err)
			}
			for v := 158; v <= 169; v++ {
				if _, err := db.Exec(`INSERT INTO goose_db_version(version_id,is_applied) VALUES (?,1)`, v); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`DROP TABLE project_dispatch_hold`); err != nil {
				t.Fatal(err)
			}
			if collision {
				if _, err := db.Exec(`CREATE TABLE project_dispatch_hold(project_id TEXT, foreign_data TEXT)`); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = sqlite.Open(dir)
			if collision {
				if err == nil {
					_ = s.Close()
					t.Fatal("foreign table admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.SetOrchestrationDispatchHold(context.Background(), "ecp", true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDispatchHoldUnknownStateAndOutstandingReadback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := sqlitetest.MustOpenAt(t, dir)
	o, e := seedDispatchHoldProject(t, s, "ecp")
	other, _ := seedDispatchHoldProject(t, s, "other")
	if err := s.LeaseOrchestrationEvents(ctx, []string{e.ID}, "wrong-project", other.ID, time.Now()); err == nil {
		t.Fatal("wrong-project destination leased event")
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "ao.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := s.SetOrchestrationDispatchHold(ctx, "ecp", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE project_dispatch_hold SET held=2 WHERE project_id='ecp'`); err != nil {
		t.Fatal(err)
	}
	if held, err := s.OrchestrationDispatchHeld(ctx, "ecp"); err == nil || !held {
		t.Fatal("invalid state admitted")
	}
	if err := s.LeaseOrchestrationEvents(ctx, []string{e.ID}, "invalid-state", o.ID, time.Now()); err == nil {
		t.Fatal("invalid state leased")
	}
	if err := s.SetOrchestrationDispatchHold(ctx, "ecp", true); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		state, token string
		submitted    bool
		count        int
	}{
		{"pending", "", false, 0}, {"leased", "batch", false, 1}, {"submitted", "batch", true, 1},
		{"pending", "batch", true, 1}, {"dead_letter", "batch", true, 1}, {"acknowledged", "batch", true, 0},
	} {
		var at any
		if tc.submitted {
			at = time.Now().UTC()
		}
		if _, err := db.Exec(`UPDATE orchestration_events SET state=?,lease_token=?,submitted_at=? WHERE id=?`, tc.state, tc.token, at, e.ID); err != nil {
			t.Fatal(err)
		}
		held, n, err := s.ReadOrchestrationDispatchHold(ctx, "ecp")
		if err != nil || !held || n != tc.count {
			t.Fatalf("%s: held=%v outstanding=%d err=%v", tc.state, held, n, err)
		}
	}
}
