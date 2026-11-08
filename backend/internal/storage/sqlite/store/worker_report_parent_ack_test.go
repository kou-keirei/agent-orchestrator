package store_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// Only the selected database stage is intercepted. The actual writer and its
// mutex/transaction still run; no synthetic store result supplies the verdict.
type contendedReportStore struct {
	ports.ExactReportStore
	stage string
	enter func(context.Context) context.Context
}

func (s contendedReportStore) WithExactParentReport(ctx context.Context, p domain.ProjectID, id string, fn func(ports.ExactReportWriter) error) error {
	return s.ExactReportStore.WithExactParentReport(ctx, p, id, func(w ports.ExactReportWriter) error {
		return fn(contendedReportWriter{ExactReportWriter: w, stage: s.stage, enter: s.enter})
	})
}

type contendedReportWriter struct {
	ports.ExactReportWriter
	stage string
	enter func(context.Context) context.Context
}

func (w contendedReportWriter) Prepare(ctx context.Context, m ports.ExactReportMessage, e domain.OrchestrationEvent, now func() time.Time) error {
	if w.stage == "prepare" {
		ctx = w.enter(ctx)
	}
	return w.ExactReportWriter.Prepare(ctx, m, e, now)
}

func (w contendedReportWriter) Admit(ctx context.Context, m ports.ExactReportMessage, now func() time.Time) error {
	if w.stage == "admit" {
		ctx = w.enter(ctx)
	}
	return w.ExactReportWriter.Admit(ctx, m, now)
}

func (w contendedReportWriter) Resolve(ctx context.Context, m ports.ExactReportMessage, now func() time.Time) error {
	if w.stage == "resolve" {
		ctx = w.enter(ctx)
	}
	return w.ExactReportWriter.Resolve(ctx, m, now)
}

type reportWriterWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *reportWriterWaitContext) Err() error {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Err()
}

func TestParentReportExpiryDuringWriterContention(t *testing.T) {
	for _, stage := range []string{"prepare", "admit"} {
		t.Run(stage, func(t *testing.T) {
			s := newTestStore(t)
			r, chat := seedExactParentDelivery(t, s)
			lease, _, err := s.GetWorkItemLease(t.Context(), r.Report.Identity.ID)
			if err != nil {
				t.Fatal(err)
			}
			initial := time.Now().UTC()
			expiry := lease.ExpiresAt
			if stage == "admit" {
				expiry = initial.Add(orchestrationevents.LeaseDuration)
			}
			var clock atomic.Int64
			clock.Store(initial.UnixNano())
			waiting := make(chan struct{})
			locked := make(chan func(), 1)
			d := &orchestrationevents.ExactParentReports{Parents: chat, Native: chat, Now: func() time.Time { return time.Unix(0, clock.Load()).UTC() }}
			d.Store = contendedReportStore{ExactReportStore: s, stage: stage, enter: func(ctx context.Context) context.Context {
				locked <- store.LockReportWriterForTest(s)
				return &reportWriterWaitContext{Context: ctx, waiting: waiting}
			}}
			result := make(chan error, 1)
			go func() { result <- d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID) }()
			release := <-locked
			var once sync.Once
			unlock := func() { once.Do(release) }
			defer unlock()
			<-waiting // LockContext has entered with the writer unavailable.
			clock.Store(expiry.UnixNano())
			unlock()
			if err := <-result; !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || (stage == "prepare" && !errors.Is(err, domain.ErrWorkLeaseExpired)) {
				t.Fatalf("expiry during %s: %v", stage, err)
			}
			e := exactReportEvent(t, s, r)
			if !e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() || e.AttentionRequiredAt.IsZero() {
				t.Fatalf("expired writer advanced delivery: %+v", e)
			}
			if stage == "prepare" {
				if chat.sendCalls != 0 || e.AttemptCount != 0 || e.LeaseToken != "" {
					t.Fatalf("expired work sent: %+v", e)
				}
				return
			}
			if chat.sendCalls != 1 || e.State != domain.OrchestrationLeased {
				t.Fatalf("expired admission: %+v sends=%d", e, chat.sendCalls)
			}
			// A refused late transport receipt may retry only the frozen bytes/key.
			d.Store = s
			if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			after := exactReportEvent(t, s, r)
			if after.LeaseToken != e.LeaseToken || after.DestinationSessionID != e.DestinationSessionID || after.AttemptCount != 2 ||
				after.State != domain.OrchestrationSubmitted || chat.sendCalls != 2 || chat.providerWrites != 1 || !after.AcknowledgedAt.IsZero() {
				t.Fatalf("retry changed binding or semantics: %+v", after)
			}
		})
	}
}

func TestParentReportAttemptDeadlineBoundsContendedCallbackAndHold(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel"} {
		for _, stage := range []string{"prepare", "admit", "resolve"} {
			t.Run(mode+"/"+stage, func(t *testing.T) {
				s := newTestStore(t)
				r, chat := seedExactParentDelivery(t, s)
				d := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat}
				if stage == "resolve" {
					_ = d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID)
					e := exactReportEvent(t, s, r)
					m, err := ports.ParseExactReportMessage(e.LeaseToken)
					if err != nil {
						t.Fatal(err)
					}
					chat.authenticated = true
					chat.semantic = ports.ExactReportReconciliation{NativeEventID: "contended-semantic-event", ClientMessageID: m.ClientMessageID(), Message: m}
				}
				waiting := make(chan struct{})
				locked := make(chan func(), 1)
				d.Store = contendedReportStore{ExactReportStore: s, stage: stage, enter: func(ctx context.Context) context.Context {
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) > orchestrationevents.AttemptTimeout {
						t.Errorf("%s database call escaped attempt deadline", stage)
					}
					locked <- store.LockReportWriterForTest(s)
					return &reportWriterWaitContext{Context: ctx, waiting: waiting}
				}}
				result := make(chan error, 1)
				outer, cancel := context.WithCancel(context.Background())
				defer cancel()
				// Outer context intentionally has no deadline. Neither the writer nor
				// the follow-up HOLD may wait for its cancellation or lock release.
				go func() {
					if stage == "resolve" {
						result <- d.Reconcile(outer, r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID)
					} else {
						result <- d.Deliver(outer, r.Report.Identity.ProjectID, r.Report.ID)
					}
				}()
				release := <-locked
				var once sync.Once
				unlock := func() { once.Do(release) }
				defer unlock()
				<-waiting
				before := exactReportEvent(t, s, r)
				wantErr := context.DeadlineExceeded
				bound := orchestrationevents.AttemptTimeout + time.Second
				if mode == "cancel" {
					cancel()
					wantErr, bound = context.Canceled, time.Second
				}
				select {
				case err := <-result:
					if !errors.Is(err, wantErr) || !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
						t.Fatalf("contended %s: %v", stage, err)
					}
				case <-time.After(bound):
					unlock()
					<-result // release an old, broken implementation before failing
					t.Fatal("callback or failure HOLD outlived the attempt deadline")
				}
				if after := exactReportEvent(t, s, r); !reflect.DeepEqual(before, after) {
					t.Fatalf("timed-out writer changed outbox: before=%+v after=%+v", before, after)
				}
				if stage == "prepare" && chat.sendCalls != 0 {
					t.Fatal("timed-out preparation sent a report")
				}
				unlock()
			})
		}
	}
}

func TestParentReportRejectsStaleWorkLeaseBeforeSend(t *testing.T) {
	for _, state := range []string{"expired", "released", "renewed", "foreign_owner"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, chat := seedExactParentDelivery(t, s)
			lease, _, err := s.GetWorkItemLease(t.Context(), r.Report.Identity.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "expired":
				_, err = reportDB(t, dir).Exec(`UPDATE work_item_leases SET expires_at_ms=?`, time.Now().Add(-time.Second).UnixMilli())
			case "released":
				_, err = s.ReleaseWorkItemLease(t.Context(), r.Report.Identity, lease)
			case "renewed":
				_, err = s.RenewWorkItemLease(t.Context(), r.Report.Identity, lease, lease.ExpiresAt.Add(time.Hour))
			case "foreign_owner":
				work, _, readErr := s.GetWorkItem(t.Context(), r.Report.Identity.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				_, err = s.ClaimWorkItemOwner(t.Context(), work, "foreign-owner")
			}
			if err != nil {
				t.Fatal(err)
			}
			d := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat}
			if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatalf("stale lease: %v", err)
			}
			e := exactReportEvent(t, s, r)
			if chat.sendCalls != 0 || e.AttemptCount != 0 || e.LeaseToken != "" || e.State != domain.OrchestrationPending || e.AttentionRequiredAt.IsZero() {
				t.Fatalf("stale source admitted: %+v chat=%+v", e, chat)
			}
			got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID)
			if err != nil || !found || got != r {
				t.Fatalf("historical receipt changed: %+v %v", got, err)
			}
		})
	}
}

func TestParentReportOwnershipChangesDuringWriterContention(t *testing.T) {
	for _, change := range []string{"released", "renewed", "foreign_owner"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, chat := seedExactParentDelivery(t, s)
			waiting := make(chan struct{})
			locked := make(chan func(), 1)
			d := &orchestrationevents.ExactParentReports{Parents: chat, Native: chat}
			d.Store = contendedReportStore{ExactReportStore: s, stage: "prepare", enter: func(ctx context.Context) context.Context {
				locked <- store.LockReportWriterForTest(s)
				return &reportWriterWaitContext{Context: ctx, waiting: waiting}
			}}
			result := make(chan error, 1)
			go func() { result <- d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID) }()
			release := <-locked
			var once sync.Once
			unlock := func() { once.Do(release) }
			defer unlock()
			<-waiting
			// Another writer's committed state is injected only into this
			// disposable database while the in-process writer is excluded.
			query := "UPDATE work_item_leases SET state='released'"
			switch change {
			case "renewed":
				query = "UPDATE work_item_leases SET revision=revision+1"
			case "foreign_owner":
				query = "UPDATE work_items SET owner_generation=owner_generation+1,owner_session_id='foreign-owner'"
			}
			if _, err := reportDB(t, dir).Exec(query); err != nil {
				t.Fatal(err)
			}
			unlock()
			if err := <-result; !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			e := exactReportEvent(t, s, r)
			if chat.sendCalls != 0 || e.AttemptCount != 0 || e.LeaseToken != "" || e.DestinationSessionID != "" ||
				e.State != domain.OrchestrationPending || e.AttentionRequiredAt.IsZero() {
				t.Fatalf("changed ownership admitted: %+v", e)
			}
			if got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID); err != nil || !found || got != r {
				t.Fatalf("immutable receipt changed: %+v %v", got, err)
			}
		})
	}
}

func TestParentReportAdmissionRequiresInvocationLiveLease(t *testing.T) {
	for _, state := range []string{"expired", "reclaimed", "other_invocation"} {
		t.Run(state, func(t *testing.T) {
			s := newTestStore(t)
			r, chat := seedExactParentDelivery(t, s)
			now := time.Now().UTC()
			e := exactReportEvent(t, s, r)
			m := ports.ExactReportMessage{ReportID: r.Report.ID, EventID: r.EventID, SourceRevision: e.SourceRevision,
				LeaseID: r.Report.LeaseID, LeaseRevision: r.Report.LeaseRevision, Parent: chat.parent, Payload: "disposable bounded payload"}
			apply := func(w ports.ExactReportWriter) error {
				before := exactReportEvent(t, s, r)
				at := now
				if state != "other_invocation" {
					at = now.Add(orchestrationevents.LeaseDuration)
				}
				if state == "reclaimed" {
					if n, err := s.ReclaimOrchestrationEventLeases(t.Context(), at); err != nil || n != 1 {
						t.Fatalf("reclaim: n=%d err=%v", n, err)
					}
					before = exactReportEvent(t, s, r)
				}
				if err := w.Admit(t.Context(), m, func() time.Time { return at }); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatalf("stale admission accepted: %v", err)
				}
				if after := exactReportEvent(t, s, r); !reflect.DeepEqual(before, after) {
					t.Fatalf("stale admission mutated outbox: before=%+v after=%+v", before, after)
				}
				return nil
			}
			if err := s.WithExactParentReport(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, func(w ports.ExactReportWriter) error {
				if err := w.Prepare(t.Context(), m, e, func() time.Time { return now }); err != nil {
					return err
				}
				if state == "other_invocation" {
					return nil
				}
				return apply(w)
			}); err != nil {
				t.Fatal(err)
			}
			if state == "other_invocation" {
				if err := s.WithExactParentReport(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, apply); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type parentResumeFixture struct {
	*exactParentChatFixture
	resumes int
}

func (f *parentResumeFixture) WithResumedReportParent(_ context.Context, expected ports.ExactReportParent, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	f.resumes++
	if expected != f.parent {
		return errors.New("fixture foreign resume")
	}
	return fn(f.observed, f)
}

func TestParentReportResumeLostACKAndNotifierRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, chat := seedExactParentDelivery(t, s)
	chat.liveErr = ports.ErrExactReportParentExited
	resume := &parentResumeFixture{exactParentChatFixture: chat}
	d := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat, Resume: resume}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || resume.resumes != 1 {
		t.Fatalf("resume: %v calls=%d", err, resume.resumes)
	}
	prepared := exactReportEvent(t, s, r)
	if _, _, err := s.CreateNotification(t.Context(), domain.NotificationRecord{ID: "parent-report-alert", SessionID: r.Report.WorkerSessionID,
		ProjectID: r.Report.Identity.ProjectID, Type: domain.NotificationOrchestrationAttention, Title: "Reconcile report",
		Status: domain.NotificationUnread, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	m, err := ports.ParseExactReportMessage(prepared.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	chat.liveErr, chat.authenticated = nil, true
	chat.semantic = ports.ExactReportReconciliation{NativeEventID: "parent-durable-reconciliation", ClientMessageID: m.ClientMessageID(), Message: m}
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	resolved := exactReportEvent(t, s, r)
	if resolved.State != domain.OrchestrationAcknowledged || resolved.AttentionRequiredAt.IsZero() || resolved.LeaseToken != prepared.LeaseToken {
		t.Fatalf("lost notification retry evidence: %+v", resolved)
	}
	// Lose the semantic API response, restart, then replay the same reference.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.Store = s
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	if after := exactReportEvent(t, s, r); !reflect.DeepEqual(after, resolved) {
		t.Fatalf("semantic replay advanced state: %+v", after)
	}
	n := &reportAttentionFixture{store: s, resolveErr: errors.New("fixture notifier unavailable")}
	legacy := &orchestrationevents.Dispatcher{Store: s, Notifier: n}
	if err := legacy.DispatchProject(t.Context(), r.Report.Identity.ProjectID); !errors.Is(err, n.resolveErr) {
		t.Fatalf("notification failure: %v", err)
	}
	if after := exactReportEvent(t, s, r); !reflect.DeepEqual(after, resolved) {
		t.Fatal("notifier failure erased retry evidence")
	}
	n.resolveErr = nil
	if err := legacy.DispatchProject(t.Context(), r.Report.Identity.ProjectID); err != nil {
		t.Fatal(err)
	}
	if after := exactReportEvent(t, s, r); !after.AttentionRequiredAt.IsZero() || after.State != domain.OrchestrationAcknowledged ||
		after.LeaseToken != prepared.LeaseToken || chat.providerWrites != 1 || chat.sendCalls != 1 || resume.resumes != 1 || n.resolved != 1 {
		t.Fatalf("duplicate wake or unresolved attention: %+v resume=%+v notifier=%+v", after, resume, n)
	}
	alerts, err := s.ListNotifications(t.Context(), domain.NotificationListAll, time.Time{}, "", 10)
	if err != nil || len(alerts) != 1 || !alerts[0].Resolved() {
		t.Fatalf("durable alert not resolved: %+v %v", alerts, err)
	}
}

func TestParentReportFinalAttemptReclaimStaysWithinSchemaBudget(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, chat := seedExactParentDelivery(t, s)
	chat.firstSendErr = errors.New("fixture lost final admission")
	chat.repeatSendErr = chat.firstSendErr
	now := time.Now().UTC()
	d := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat, Now: func() time.Time { return now }}
	for attempt := 1; attempt <= 8; attempt++ {
		// The fixture has a stable provider ledger; lose each transport response.
		if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		e := exactReportEvent(t, s, r)
		if e.AttemptCount != attempt || e.State != domain.OrchestrationLeased {
			t.Fatalf("attempt %d: %+v", attempt, e)
		}
		now = now.Add(orchestrationevents.LeaseDuration + time.Second)
	}
	before := exactReportEvent(t, s, r)
	if n, err := s.ReclaimOrchestrationEventLeases(t.Context(), now); err != nil || n != 1 {
		t.Fatalf("final reclaim: n=%d err=%v", n, err)
	}
	after := exactReportEvent(t, s, r)
	if after.AttemptCount != 8 || after.State != domain.OrchestrationDeadLetter || after.LeaseToken != before.LeaseToken ||
		after.DestinationSessionID != before.DestinationSessionID || after.AttentionRequiredAt.IsZero() {
		t.Fatalf("final reclaim changed binding or budget: %+v", after)
	}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || chat.sendCalls != 8 || chat.providerWrites != 1 {
		t.Fatalf("dead letter resent: %v sends=%d", err, chat.sendCalls)
	}
}
