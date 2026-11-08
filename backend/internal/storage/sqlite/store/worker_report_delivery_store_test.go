package store_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
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

// Disposable adapters below exercise the new Store/Chat boundary only. They do
// NOT implement W557 readback, genuine native continuity or authentication.
type exactParentChatFixture struct {
	parent, observed                                                 ports.ExactReportParent
	parentErr, liveErr, firstSendErr, repeatSendErr                  error
	parentReads, liveCalls, sendCalls, providerWrites, semanticReads int
	acceptedID, acceptedPayload                                      string
	semantic                                                         ports.ExactReportReconciliation
	authenticated                                                    bool
}

func (f *exactParentChatFixture) ReadReportOriginalParent(_ context.Context, work domain.WorkItemID, attempt domain.WorkAttemptID) (ports.ExactReportParent, error) {
	f.parentReads++
	if work != f.parent.WorkItemID || attempt != f.parent.AttemptID {
		return ports.ExactReportParent{}, errors.New("fixture read key mismatch")
	}
	return f.parent, f.parentErr
}
func (f *exactParentChatFixture) WithLiveReportParent(_ context.Context, expected ports.ExactReportParent, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	f.liveCalls++
	if f.liveErr != nil {
		return f.liveErr
	}
	if expected != f.parent {
		return errors.New("fixture foreign target")
	}
	return fn(f.observed, f)
}
func (f *exactParentChatFixture) SendReport(_ context.Context, id, payload string) (bool, error) {
	f.sendCalls++
	if f.acceptedID == "" {
		f.providerWrites++
		f.acceptedID, f.acceptedPayload = id, payload
		return f.firstSendErr == nil, f.firstSendErr
	}
	if id != f.acceptedID || payload != f.acceptedPayload {
		return false, errors.New("fixture changed client ID or payload")
	}
	return f.repeatSendErr == nil, f.repeatSendErr
}
func (f *exactParentChatFixture) ReadReportReconciliation(_ context.Context, id string) (ports.ExactReportReconciliation, error) {
	f.semanticReads++
	if !f.authenticated || id != f.semantic.NativeEventID {
		return ports.ExactReportReconciliation{}, errors.New("fixture unauthenticated semantic event")
	}
	return f.semantic, nil
}

type exactReportWriteCounts struct{ prepares, admissions, resolves, holds int }
type countedExactReportStore struct {
	ports.ExactReportStore
	counts *exactReportWriteCounts
}
type countedExactReportWriter struct {
	ports.ExactReportWriter
	counts *exactReportWriteCounts
}

func (s countedExactReportStore) WithExactParentReport(ctx context.Context, p domain.ProjectID, id string, fn func(ports.ExactReportWriter) error) error {
	return s.ExactReportStore.WithExactParentReport(ctx, p, id, func(w ports.ExactReportWriter) error {
		return fn(countedExactReportWriter{ExactReportWriter: w, counts: s.counts})
	})
}
func (w countedExactReportWriter) Prepare(ctx context.Context, m ports.ExactReportMessage, e domain.OrchestrationEvent, now func() time.Time) error {
	w.counts.prepares++
	return w.ExactReportWriter.Prepare(ctx, m, e, now)
}
func (w countedExactReportWriter) Admit(ctx context.Context, m ports.ExactReportMessage, now func() time.Time) error {
	w.counts.admissions++
	return w.ExactReportWriter.Admit(ctx, m, now)
}
func (w countedExactReportWriter) Resolve(ctx context.Context, m ports.ExactReportMessage, now func() time.Time) error {
	w.counts.resolves++
	return w.ExactReportWriter.Resolve(ctx, m, now)
}
func (w countedExactReportWriter) Hold(ctx context.Context, at time.Time) error {
	w.counts.holds++
	return w.ExactReportWriter.Hold(ctx, at)
}

func seedExactParentDelivery(t *testing.T, s *sqlite.Store) (domain.WorkerReportReceipt, *exactParentChatFixture) {
	t.Helper()
	seedProject(t, s, "project-1")
	parent := sampleRecord("project-1")
	parent.Kind, parent.Mode = domain.KindOrchestrator, domain.SessionModeChat
	parent.Metadata.AgentSessionID = "original-parent-native"
	parent, err := s.CreateSession(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := s.CreateSession(t.Context(), sampleRecord("project-1"))
	if err != nil {
		t.Fatal(err)
	}
	work := workLeaseRecord()
	work.Identity.OriginalParentSessionID = parent.ID
	work.Identity.OriginalWorkerSessionID, work.Identity.OwnerSessionID = worker.ID, worker.ID
	if _, err := s.CreateWorkItem(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireWorkItemLease(t.Context(), work.Identity, leaseGrant("lease-1"))
	if err != nil {
		t.Fatal(err)
	}
	r := domain.WorkerReport{ID: "report-1", Type: "checkpoint", Version: 1, Identity: work.Identity,
		LeaseID: lease.Grant.ID, LeaseRevision: lease.Revision, WorkerSessionID: worker.ID,
		NativeSessionID: work.OriginalNativeSessionID, NativeTurnID: "turn-1", OriginalNativeSessionID: work.OriginalNativeSessionID,
		Content: "explicit worker checkpoint", Digest: reportDigest("explicit worker checkpoint")}
	receipt, err := s.AcceptWorkerReport(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	f := &exactParentChatFixture{parent: ports.ExactReportParent{WorkItemID: r.Identity.ID, AttemptID: r.Identity.AttemptID,
		SessionID: parent.ID, NativeSessionID: parent.Metadata.AgentSessionID, ControllerGeneration: "original-generation",
		ProviderConversationID: "original-conversation", ProviderScopeID: "original-scope"}}
	f.observed = f.parent
	return receipt, f
}

func exactReportEvent(t *testing.T, s *sqlite.Store, r domain.WorkerReportReceipt) domain.OrchestrationEvent {
	t.Helper()
	rows, err := s.ListOrchestrationEvents(t.Context(), r.Report.Identity.ProjectID, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rows {
		if e.ID == r.EventID {
			return e
		}
	}
	t.Fatal("report event missing")
	return domain.OrchestrationEvent{}
}

// Exercise the real transaction boundary deterministically: now is sampled
// after the source reads, while an independent connection tries to write.
// A deferred reader would allow that write and then fail its own upgrade.
func TestExactParentReportReservesSQLiteWriterBeforeSourceReads(t *testing.T) {
	for _, operation := range []string{"prepare", "admit", "resolve"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			r, chat := seedExactParentDelivery(t, s)
			e := exactReportEvent(t, s, r)
			message := ports.ExactReportMessage{ReportID: r.Report.ID, EventID: r.EventID,
				SourceRevision: e.SourceRevision, LeaseID: r.Report.LeaseID, LeaseRevision: r.Report.LeaseRevision,
				Parent: chat.parent, Payload: "fixture exact binding"}
			conn, err := reportDB(t, dir).Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(t.Context(), `PRAGMA busy_timeout=0`); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			checked := false
			checkWriter := func() time.Time {
				checked = true
				_, err := conn.ExecContext(t.Context(), `UPDATE orchestration_events SET last_error='competing writer' WHERE id=?`, e.ID)
				var coded interface{ Code() int }
				if !errors.As(err, &coded) || coded.Code()&255 != 5 { // SQLITE_BUSY
					t.Fatalf("independent write entered source-read transaction: %v", err)
				}
				return now
			}
			err = s.WithExactParentReport(t.Context(), e.ProjectID, r.Report.ID, func(w ports.ExactReportWriter) error {
				if operation == "prepare" {
					return w.Prepare(t.Context(), message, e, checkWriter)
				}
				if err := w.Prepare(t.Context(), message, e, func() time.Time { return now }); err != nil {
					return err
				}
				if operation == "admit" {
					return w.Admit(t.Context(), message, checkWriter)
				}
				return w.Resolve(t.Context(), message, checkWriter)
			})
			if err != nil || !checked {
				t.Fatalf("writer reservation: checked=%v err=%v", checked, err)
			}
			if _, err := conn.ExecContext(t.Context(), `UPDATE orchestration_events SET last_error=last_error WHERE id=?`, e.ID); err != nil {
				t.Fatalf("committed report retained SQLite writer: %v", err)
			}
			if got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID); err != nil || !found || got != r {
				t.Fatalf("writer reservation changed immutable receipt: %+v %v", got, err)
			}
		})
	}
}

func TestExactParentReportStoreColdReplayAndSemanticResolution(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, chat := seedExactParentDelivery(t, s)
	now := time.Now().UTC()
	counts := &exactReportWriteCounts{}
	d := &orchestrationevents.ExactParentReports{Store: countedExactReportStore{s, counts}, Parents: chat, Native: chat, Now: func() time.Time { return now }}
	uncertain := errors.New("fixture accepted native write, response lost")
	chat.firstSendErr = uncertain
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, uncertain) || !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatalf("uncertain send: %v", err)
	}
	first := exactReportEvent(t, s, r)
	if first.State != domain.OrchestrationLeased || first.LeaseToken == "" || first.DestinationSessionID != chat.parent.SessionID ||
		first.AttemptCount != 1 || first.AttentionRequiredAt.IsZero() || !first.SubmittedAt.IsZero() || !first.AcknowledgedAt.IsZero() ||
		counts.prepares != 1 || counts.admissions != 0 || counts.resolves != 0 || chat.sendCalls != 1 || chat.providerWrites != 1 {
		t.Fatalf("first=%+v counts=%+v chat=%+v", first, counts, chat)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(orchestrationevents.LeaseDuration + time.Second)
	// New service/store objects consume persisted bytes. The disposable native
	// fixture retains its accepted-message ledger, like a native Chat controller.
	d = &orchestrationevents.ExactParentReports{Store: countedExactReportStore{s, counts}, Parents: chat, Native: chat, Now: func() time.Time { return now }}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatalf("replay: %v", err)
	}
	second := exactReportEvent(t, s, r)
	m, err := ports.ParseExactReportMessage(second.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseToken != first.LeaseToken || second.DestinationSessionID != first.DestinationSessionID || second.State != domain.OrchestrationSubmitted ||
		second.AttemptCount != 2 || second.SubmittedAt.IsZero() || !second.AcknowledgedAt.IsZero() || second.AttentionRequiredAt.IsZero() ||
		chat.sendCalls != 2 || chat.providerWrites != 1 || chat.acceptedID != m.ClientMessageID() || chat.acceptedPayload != m.Payload ||
		counts.prepares != 2 || counts.admissions != 1 || counts.resolves != 0 {
		t.Fatalf("replay changed snapshot or semantic state: %+v counts=%+v chat=%+v", second, counts, chat)
	}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || chat.sendCalls != 2 {
		t.Fatalf("repeat wake after admission: %v", err)
	}
	// A transport admission cannot use the existing generic ACK endpoint.
	if n, err := s.AcknowledgeOrchestrationBatchAccepted(t.Context(), chat.parent.SessionID, second.LeaseToken, now); err != nil || n != 0 {
		t.Fatalf("generic ACK admitted: %d %v", n, err)
	}
	chat.semantic = ports.ExactReportReconciliation{NativeEventID: "fixture-parent-semantic", ClientMessageID: m.ClientMessageID(), Message: m}
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || counts.resolves != 0 {
		t.Fatalf("unauthenticated ACK: %v", err)
	}
	chat.authenticated = true
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	resolved := exactReportEvent(t, s, r)
	if resolved.State != domain.OrchestrationAcknowledged || resolved.AcknowledgedAt.IsZero() || resolved.AttentionRequiredAt.IsZero() ||
		resolved.LeaseToken != first.LeaseToken || counts.resolves != 1 || chat.providerWrites != 1 || chat.semanticReads != 2 {
		t.Fatalf("semantic state=%+v counts=%+v", resolved, counts)
	}
	got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID)
	if err != nil || !found || got != r {
		t.Fatalf("immutable receipt changed: %+v %v %v", got, found, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.Store = countedExactReportStore{s, counts}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); err != nil || chat.sendCalls != 2 || counts.resolves != 1 {
		t.Fatalf("resolved restart: %v", err)
	}
}

func TestExactParentReportStoreNegativeBoundaries(t *testing.T) {
	for _, name := range []string{"unbound", "missing", "ambiguous", "terminated", "replaced", "project_hold", "mixed", "retarget_retry", "changed_payload", "lost_admission_receipt"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, chat := seedExactParentDelivery(t, s)
			now := time.Now().UTC()
			counts := &exactReportWriteCounts{}
			d := &orchestrationevents.ExactParentReports{Store: countedExactReportStore{s, counts}, Parents: chat, Native: chat, Now: func() time.Time { return now }}
			var original domain.OrchestrationEvent
			if name == "retarget_retry" || name == "changed_payload" || name == "lost_admission_receipt" {
				chat.firstSendErr = errors.New("fixture uncertain delivery")
				if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatal(err)
				}
				original = exactReportEvent(t, s, r)
				now = now.Add(orchestrationevents.LeaseDuration + time.Second)
			}
			switch name {
			case "unbound":
				d.Parents = nil
			case "missing", "ambiguous":
				chat.parentErr = errors.New(name + " fixture parent history")
			case "terminated":
				parent, ok, err := s.GetSession(t.Context(), chat.parent.SessionID)
				if err != nil || !ok {
					t.Fatalf("parent: %v %v", ok, err)
				}
				parent.IsTerminated = true
				if err := s.UpdateSession(t.Context(), parent); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				chat.observed.ControllerGeneration = "replacement"
			case "project_hold":
				if err := s.SetOrchestrationDispatchHold(t.Context(), r.Report.Identity.ProjectID, true); err != nil {
					t.Fatal(err)
				}
			case "mixed":
				legacy := domain.OrchestrationEvent{ID: "fixture-legacy", ProjectID: r.Report.Identity.ProjectID, WorkerID: r.Report.WorkerSessionID,
					Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "fixture-revision", EnqueuedAt: now, NextAttemptAt: now}
				if _, err := s.EnqueueOrchestrationEvent(t.Context(), legacy); err != nil {
					t.Fatal(err)
				}
				if _, err := reportDB(t, dir).Exec(`UPDATE orchestration_events SET lease_token='mixed-fixture-token',destination_session_id=?`, chat.parent.SessionID); err != nil {
					t.Fatal(err)
				}
			case "retarget_retry":
				chat.parent.ControllerGeneration, chat.observed.ControllerGeneration = "replacement", "replacement"
			case "changed_payload":
				m, err := ports.ParseExactReportMessage(original.LeaseToken)
				if err != nil {
					t.Fatal(err)
				}
				m.Payload += "changed"
				if err := s.WithExactParentReport(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, func(w ports.ExactReportWriter) error {
					return w.Prepare(t.Context(), m, original, func() time.Time { return now })
				}); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatalf("changed payload CAS: %v", err)
				}
			case "lost_admission_receipt":
				m, err := ports.ParseExactReportMessage(original.LeaseToken)
				if err != nil {
					t.Fatal(err)
				}
				chat.authenticated = true
				chat.semantic = ports.ExactReportReconciliation{NativeEventID: "fixture-semantic-after-lost-result", ClientMessageID: m.ClientMessageID(), Message: m}
			}
			before := exactReportEvent(t, s, r)
			beforeSends, beforeWrites, beforePrepares := chat.sendCalls, chat.providerWrites, counts.prepares
			if name == "changed_payload" {
				if before.LeaseToken != original.LeaseToken || before.AttemptCount != original.AttemptCount {
					t.Fatal("failed CAS changed durable binding")
				}
				return
			}
			if name == "lost_admission_receipt" {
				if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); err != nil {
					t.Fatal(err)
				}
				after := exactReportEvent(t, s, r)
				if after.State != domain.OrchestrationAcknowledged || chat.sendCalls != beforeSends || chat.providerWrites != beforeWrites || counts.resolves != 1 {
					t.Fatalf("lost receipt recovery: %+v", after)
				}
				return
			}
			err = d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID)
			if name == "project_hold" {
				if !errors.Is(err, orchestrationevents.ErrDispatchHeld) || counts.holds != 0 || chat.parentReads != 0 {
					t.Fatalf("project hold: %v counts=%+v", err, counts)
				}
			} else if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatalf("expected HOLD: %v", err)
			}
			after := exactReportEvent(t, s, r)
			if chat.sendCalls != beforeSends || chat.providerWrites != beforeWrites || counts.resolves != 0 ||
				after.State != before.State || after.LeaseToken != before.LeaseToken || after.DestinationSessionID != before.DestinationSessionID ||
				after.AttemptCount != before.AttemptCount || !after.AcknowledgedAt.IsZero() {
				t.Fatalf("negative advanced event: before=%+v after=%+v counts=%+v chat=%+v", before, after, counts, chat)
			}
			if name != "project_hold" && after.AttentionRequiredAt.IsZero() {
				t.Fatal("human attention missing")
			}
			wantPrepares := beforePrepares
			if name == "terminated" {
				wantPrepares++
			} // real Store rejects even a dishonest live fixture
			if counts.prepares != wantPrepares {
				t.Fatalf("unexpected writer: %+v", counts)
			}
			got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID)
			if err != nil || !found || got != r {
				t.Fatalf("receipt mutated: %+v %v", got, err)
			}
			if strings.Contains(after.LastError, "fixture") {
				t.Fatal("untrusted error persisted")
			}
		})
	}
}

// Notifications here are disposable observations, not native semantic proof.
type reportAttentionFixture struct {
	notified, resolved, resolveCalls int
	store                            *sqlite.Store
	resolveErr                       error
}

func (f *reportAttentionFixture) Notify(context.Context, ports.NotificationIntent) error {
	f.notified++
	return nil
}
func (f *reportAttentionFixture) Resolve(ctx context.Context, resolution ports.NotificationResolution) error {
	f.resolveCalls++
	if f.resolveErr != nil {
		return f.resolveErr
	}
	if f.store != nil {
		if _, err := f.store.ResolveSessionNotifications(ctx, resolution.SessionID, resolution.Type, resolution.ResolvedAt); err != nil {
			return err
		}
	}
	f.resolved++
	return nil
}

func TestExactParentAttentionDispatchFirstRetryAndReopen(t *testing.T) {
	for _, name := range []string{"success", "resolve_failure", "no_notifier", "unresolved_peer"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			r, chat := seedExactParentDelivery(t, s)
			now := time.Now().UTC()
			exact := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat, Now: func() time.Time { return now }}
			if err := exact.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			prepared := exactReportEvent(t, s, r)
			m, err := ports.ParseExactReportMessage(prepared.LeaseToken)
			if err != nil {
				t.Fatal(err)
			}
			chat.authenticated = true
			chat.semantic = ports.ExactReportReconciliation{NativeEventID: "parent-semantic", ClientMessageID: m.ClientMessageID(), Message: m}
			if err := exact.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); err != nil {
				t.Fatal(err)
			}
			resolved := exactReportEvent(t, s, r)
			// The notifier owns clearing this marker, after durable resolution.
			resolved.AttentionRequiredAt, resolved.LastError = time.Time{}, ""
			// Seed only the documented stale legacy marker, never a synthetic ACK.
			db := reportDB(t, dir)
			if _, err := db.Exec(`UPDATE orchestration_events SET attention_required_at=?,last_error=? WHERE id=?`, now, ports.WorkerReportDeliveryHoldReason, r.EventID); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			marked := exactReportEvent(t, s, r)
			alert, created, err := s.CreateNotification(t.Context(), domain.NotificationRecord{
				ID: "existing-report-alert", SessionID: r.Report.WorkerSessionID, ProjectID: r.Report.Identity.ProjectID,
				Type: domain.NotificationOrchestrationAttention, Title: "Report needs attention", Status: domain.NotificationUnread, CreatedAt: now,
			})
			if err != nil || !created {
				t.Fatalf("seed notification: created=%v err=%v", created, err)
			}
			var peer domain.WorkerReportReceipt
			if name == "unresolved_peer" {
				report := r.Report
				report.ID, report.NativeTurnID = "peer-report", "peer-turn"
				peer, err = s.AcceptWorkerReport(t.Context(), report)
				if err != nil {
					t.Fatal(err)
				}
			}
			transport := &reportDeliveryTransport{}
			n := &reportAttentionFixture{}
			failure := errors.New("notification resolution unavailable")
			// Dispatch is the FIRST attention operation. Retry twice across cold
			// opens; the failure case must survive another failing wake after reopen.
			for phase := range 3 {
				n.store, n.resolveErr = s, nil
				failing := name == "resolve_failure" && phase < 2
				if failing {
					n.resolveErr = failure
				}
				legacy := &orchestrationevents.Dispatcher{Store: s, Transport: transport, Notifier: n, Now: func() time.Time { return now }}
				missingNotifier := name == "no_notifier" && phase == 0
				if missingNotifier {
					legacy.Notifier = nil
				}
				err := legacy.DispatchProject(t.Context(), r.Report.Identity.ProjectID)
				switch {
				case failing:
					if !errors.Is(err, failure) {
						t.Fatalf("failed resolution swallowed: %v", err)
					}
				case name == "unresolved_peer":
					if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
						t.Fatalf("peer not held: %v", err)
					}
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
				retained := failing || missingNotifier || name == "unresolved_peer"
				want := resolved
				if retained {
					want = marked
				}
				if got := exactReportEvent(t, s, r); !reflect.DeepEqual(got, want) {
					t.Fatalf("phase %d lost retry evidence or binding: want=%+v got=%+v", phase, want, got)
				}
				alerts, err := s.ListNotifications(t.Context(), domain.NotificationListAll, time.Time{}, "", 10)
				if err != nil || len(alerts) != 1 || alerts[0].ID != alert.ID || alerts[0].Resolved() == retained {
					t.Fatalf("phase %d existing alert: %+v err=%v retained=%v", phase, alerts, err, retained)
				}
				if name == "unresolved_peer" {
					got := exactReportEvent(t, s, peer)
					if got.State != domain.OrchestrationPending || got.AttentionRequiredAt.IsZero() || !got.AcknowledgedAt.IsZero() ||
						got.AttemptCount != 0 || got.LeaseToken != "" || n.resolveCalls != 0 {
						t.Fatalf("unresolved peer changed or notification resolved: %+v notifier=%+v", got, n)
					}
				}
				if got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID); err != nil || !found || got != r {
					t.Fatalf("immutable receipt changed: %+v %v", got, err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = sqlite.Open(dir)
				if err != nil {
					t.Fatal(err)
				}
			}
			wantResolves, wantCalls, wantNotifies := 1, 1, 0
			if name == "resolve_failure" {
				wantCalls = 3
			}
			if name == "unresolved_peer" {
				wantResolves, wantCalls, wantNotifies = 0, 0, 3
			}
			if n.resolved != wantResolves || n.resolveCalls != wantCalls || n.notified != wantNotifies ||
				chat.sendCalls != 1 || chat.providerWrites != 1 || chat.semanticReads != 1 || len(transport.batches) != 0 ||
				chat.acceptedID != m.ClientMessageID() || chat.acceptedPayload != m.Payload {
				t.Fatalf("unexpected notification retry or send: notifier=%+v chat=%+v batches=%v", n, chat, transport.batches)
			}
		})
	}
}

func TestExactParentReportReclaimDeadLetterSemanticOnly(t *testing.T) {
	for _, name := range []string{"submitted", "lost_receipt", "authentication", "foreign_generation", "foreign_conversation", "foreign_parent", "client_id", "event", "payload", "lease_revision", "work_attempt", "mixed", "project_hold"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			r, chat := seedExactParentDelivery(t, s)
			now := time.Now().UTC()
			counts := &exactReportWriteCounts{}
			d := &orchestrationevents.ExactParentReports{Store: countedExactReportStore{s, counts}, Parents: chat, Native: chat, Now: func() time.Time { return now }}
			if name == "lost_receipt" {
				chat.firstSendErr = errors.New("fixture lost admission result")
			}
			if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			prepared := exactReportEvent(t, s, r)
			m, err := ports.ParseExactReportMessage(prepared.LeaseToken)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			now = prepared.EnqueuedAt.Add(16 * time.Minute)
			s, err = sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			d.Store = countedExactReportStore{s, counts}
			if n, err := s.ReclaimOrchestrationEventLeases(t.Context(), now); err != nil || n != 1 {
				t.Fatalf("real startup reclaim: n=%d err=%v", n, err)
			}
			reclaimed := exactReportEvent(t, s, r)
			if reclaimed.State != domain.OrchestrationDeadLetter || reclaimed.LeaseToken != prepared.LeaseToken ||
				reclaimed.DestinationSessionID != prepared.DestinationSessionID || reclaimed.SourceRevision != prepared.SourceRevision ||
				reclaimed.AttemptCount != prepared.AttemptCount+1 || !reclaimed.SubmittedAt.Equal(prepared.SubmittedAt) || reclaimed.AttentionRequiredAt.IsZero() {
				t.Fatalf("reclaim lost durable binding: %+v", reclaimed)
			}
			if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatalf("dead letter delivery: %v", err)
			}
			if err := s.WithExactParentReport(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, func(w ports.ExactReportWriter) error {
				if err := w.Prepare(t.Context(), m, reclaimed, func() time.Time { return now }); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatalf("dead-letter rearm: %v", err)
				}
				return w.Admit(t.Context(), m, func() time.Time { return now })
			}); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatalf("dead-letter transport admission: %v", err)
			}
			if n, err := s.AcknowledgeOrchestrationBatchAccepted(t.Context(), m.Parent.SessionID, m.Token(), now); err != nil || n != 0 {
				t.Fatalf("generic ACK: %d %v", n, err)
			}
			if retried, err := s.RetryDeadLetterOrchestrationEvent(t.Context(), r.Report.Identity.ProjectID, r.EventID, now); err != nil || retried {
				t.Fatalf("generic dead-letter retry admitted report: %v %v", retried, err)
			}
			chat.authenticated = name != "authentication"
			chat.semantic = ports.ExactReportReconciliation{NativeEventID: "parent-semantic", ClientMessageID: m.ClientMessageID(), Message: m}
			switch name {
			case "foreign_generation":
				chat.observed.ControllerGeneration = "replacement"
			case "foreign_conversation":
				chat.observed.ProviderConversationID = "foreign"
			case "foreign_parent":
				chat.semantic.Message.Parent.SessionID = "foreign"
			case "client_id":
				chat.semantic.ClientMessageID = "generic-transport-ack"
			case "event":
				chat.semantic.Message.EventID = "foreign"
			case "payload":
				chat.semantic.Message.Payload += "changed"
			case "lease_revision":
				chat.semantic.Message.LeaseRevision++
			case "work_attempt":
				chat.semantic.Message.Parent.AttemptID = "foreign"
			case "mixed":
				peer := domain.OrchestrationEvent{ID: "mixed-peer", ProjectID: r.Report.Identity.ProjectID, WorkerID: r.Report.WorkerSessionID,
					Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "peer", EnqueuedAt: now, NextAttemptAt: now}
				if _, err := s.EnqueueOrchestrationEvent(t.Context(), peer); err != nil {
					t.Fatal(err)
				}
				if _, err := reportDB(t, dir).Exec(`UPDATE orchestration_events SET lease_token=? WHERE id=?`, m.Token(), peer.ID); err != nil {
					t.Fatal(err)
				}
			case "project_hold":
				if err := s.SetOrchestrationDispatchHold(t.Context(), r.Report.Identity.ProjectID, true); err != nil {
					t.Fatal(err)
				}
			}
			err = d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID)
			after := exactReportEvent(t, s, r)
			if name == "submitted" || name == "lost_receipt" {
				if err != nil || counts.resolves != 1 || after.State != domain.OrchestrationAcknowledged || after.AcknowledgedAt.IsZero() ||
					after.AttentionRequiredAt.IsZero() || after.LastError != ports.WorkerReportDeliveryHoldReason {
					t.Fatalf("semantic recovery: err=%v event=%+v counts=%+v", err, after, counts)
				}
			} else {
				wantErr := ports.ErrWorkerReportDeliveryHeld
				if name == "project_hold" {
					wantErr = orchestrationevents.ErrDispatchHeld
				}
				if !errors.Is(err, wantErr) || counts.resolves != 0 || after.State != domain.OrchestrationDeadLetter ||
					!after.AcknowledgedAt.IsZero() || after.AttentionRequiredAt.IsZero() {
					t.Fatalf("negative semantic recovery: err=%v event=%+v counts=%+v", err, after, counts)
				}
			}
			if chat.sendCalls != 1 || chat.providerWrites != 1 || counts.prepares != 1 ||
				after.LeaseToken != prepared.LeaseToken || after.DestinationSessionID != prepared.DestinationSessionID ||
				after.SourceRevision != prepared.SourceRevision || after.AttemptCount != reclaimed.AttemptCount ||
				!after.SubmittedAt.Equal(prepared.SubmittedAt) || chat.acceptedID != m.ClientMessageID() || chat.acceptedPayload != m.Payload {
				t.Fatalf("recovery changed transport or binding: %+v counts=%+v chat=%+v", after, counts, chat)
			}
			got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID)
			if err != nil || !found || got != r {
				t.Fatalf("immutable receipt changed: %+v %v", got, err)
			}
		})
	}
}

func TestExactParentSemanticResolutionSurvivesLegacyHoldAttentionAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, chat := seedExactParentDelivery(t, s)
	now := time.Now().UTC()
	d := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat, Now: func() time.Time { return now }}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	prepared := exactReportEvent(t, s, r)
	m, err := ports.ParseExactReportMessage(prepared.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	chat.authenticated = true
	chat.semantic = ports.ExactReportReconciliation{NativeEventID: "parent-semantic", ClientMessageID: m.ClientMessageID(), Message: m}
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, chat.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	resolved := exactReportEvent(t, s, r)
	resolved.AttentionRequiredAt, resolved.LastError = time.Time{}, ""
	transport := &reportDeliveryTransport{}
	for range 2 {
		// A stale attention marker from the prior legacy pass must also be
		// classified as resolved, without accepting an ordinary transport ACK.
		if _, err := reportDB(t, dir).Exec(`UPDATE orchestration_events SET attention_required_at=?,last_error=? WHERE id=?`, now, ports.WorkerReportDeliveryHoldReason, r.EventID); err != nil {
			t.Fatal(err)
		}
		notifier := &reportAttentionFixture{}
		legacy := &orchestrationevents.Dispatcher{Store: s, Transport: transport, Notifier: notifier, Now: func() time.Time { return now }}
		if err := legacy.ReconcileAttention(t.Context(), r.Report.Identity.ProjectID, now); err != nil || notifier.notified != 0 || notifier.resolved != 1 {
			t.Fatalf("semantic attention re-held: %v notifier=%+v", err, notifier)
		}
		if held, err := s.HoldWorkerReportDeliveries(t.Context(), r.Report.Identity.ProjectID, now); err != nil || held {
			t.Fatalf("semantic report re-held: %v %v", held, err)
		}
		if err := legacy.DispatchProject(t.Context(), r.Report.Identity.ProjectID); err != nil {
			t.Fatalf("resolved dispatch: %v", err)
		}
		if got := exactReportEvent(t, s, r); !reflect.DeepEqual(got, resolved) {
			t.Fatalf("legacy pass changed resolution: before=%+v after=%+v", resolved, got)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		d.Store = s
		if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); err != nil {
			t.Fatal(err)
		}
	}
	if chat.sendCalls != 1 || chat.providerWrites != 1 || chat.semanticReads != 1 || len(transport.batches) != 0 {
		t.Fatalf("resolved report replayed: chat=%+v batches=%v", chat, transport.batches)
	}
	got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID)
	if err != nil || !found || got != r {
		t.Fatalf("immutable receipt changed: %+v %v", got, err)
	}
}

func TestReportLegacyAckDoesNotAcquireSemanticResolution(t *testing.T) {
	for _, name := range []string{"generic_ack", "transport_only", "malformed", "foreign_source", "foreign_destination", "mixed", "no_attempt", "no_ack_time"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			r, chat := seedExactParentDelivery(t, s)
			now := time.Now().UTC()
			d := &orchestrationevents.ExactParentReports{Store: s, Parents: chat, Native: chat, Now: func() time.Time { return now }}
			if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			e := exactReportEvent(t, s, r)
			db := reportDB(t, dir)
			// Direct SQL is only a historical/corrupt-state fixture, never an ACK API.
			if name != "transport_only" {
				if _, err := db.Exec(`UPDATE orchestration_events SET state='acknowledged',acknowledged_at=? WHERE id=?`, now, e.ID); err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "generic_ack":
				_, err = db.Exec(`UPDATE orchestration_events SET lease_token='legacy-transport-batch' WHERE id=?`, e.ID)
			case "malformed":
				_, err = db.Exec(`UPDATE orchestration_events SET lease_token='ao-exact-report-v1:{}' WHERE id=?`, e.ID)
			case "foreign_source":
				// SQLite owns immutable event sources. Prove the trigger rejects
				// substitution, then corrupt only the mutable delivery binding.
				sourceBefore := exactReportEvent(t, s, r)
				if _, err := db.Exec(`UPDATE orchestration_events SET source_revision='foreign' WHERE id=?`, e.ID); err == nil || !strings.Contains(err.Error(), "immutable worker report event source") {
					t.Fatalf("immutable source substitution: %v", err)
				}
				if got := exactReportEvent(t, s, r); !reflect.DeepEqual(got, sourceBefore) {
					t.Fatalf("rejected source update changed durable event: %+v", got)
				}
				binding, parseErr := ports.ParseExactReportMessage(e.LeaseToken)
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				binding.SourceRevision = "foreign"
				_, err = db.Exec(`UPDATE orchestration_events SET lease_token=? WHERE id=?`, binding.Token(), e.ID)
			case "foreign_destination":
				_, err = db.Exec(`UPDATE orchestration_events SET destination_session_id=? WHERE id=?`, r.Report.WorkerSessionID, e.ID)
			case "mixed":
				peer := domain.OrchestrationEvent{ID: "peer", ProjectID: e.ProjectID, WorkerID: e.WorkerID,
					Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "peer", EnqueuedAt: now, NextAttemptAt: now}
				if _, err := s.EnqueueOrchestrationEvent(t.Context(), peer); err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec(`UPDATE orchestration_events SET lease_token=? WHERE id=?`, e.LeaseToken, peer.ID)
			case "no_attempt":
				_, err = db.Exec(`UPDATE orchestration_events SET attempt_count=0 WHERE id=?`, e.ID)
			case "no_ack_time":
				_, err = db.Exec(`UPDATE orchestration_events SET acknowledged_at=NULL WHERE id=?`, e.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := exactReportEvent(t, s, r)
			for range 2 {
				if resolved, err := s.WorkerReportSemanticallyResolved(t.Context(), before); err != nil || resolved {
					t.Fatalf("generic/corrupt ACK resolved: %v %v", resolved, err)
				}
				n := &reportAttentionFixture{}
				transport := &reportDeliveryTransport{}
				legacy := &orchestrationevents.Dispatcher{Store: s, Transport: transport, Notifier: n, Now: func() time.Time { return now }}
				if err := legacy.DispatchProject(t.Context(), e.ProjectID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || n.notified != 1 || n.resolved != 0 {
					t.Fatalf("legacy ACK attention: err=%v notifier=%+v", err, n)
				}
				after := exactReportEvent(t, s, r)
				if after.AttentionRequiredAt.IsZero() || after.LastError != ports.WorkerReportDeliveryHoldReason ||
					after.State != before.State || after.LeaseToken != before.LeaseToken || after.AttemptCount != before.AttemptCount ||
					!after.AcknowledgedAt.Equal(before.AcknowledgedAt) || after.DestinationSessionID != before.DestinationSessionID {
					t.Fatalf("held binding changed: %+v", after)
				}
				if err := s.ClearResolvedWorkerReportAttention(t.Context(), []domain.OrchestrationEvent{after}); err != nil {
					t.Fatal(err)
				}
				if got := exactReportEvent(t, s, r); !reflect.DeepEqual(got, after) || len(transport.batches) != 0 {
					t.Fatalf("generic/corrupt ACK cleared attention or sent: event=%+v batches=%v", got, transport.batches)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = sqlite.Open(dir)
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
