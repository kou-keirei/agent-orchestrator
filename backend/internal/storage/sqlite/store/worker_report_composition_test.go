package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type runtimeReportVerifier func(context.Context, domain.WorkerReport) error

func (v runtimeReportVerifier) VerifyWorkerReportActor(ctx context.Context, r domain.WorkerReport) error {
	return v(ctx, r)
}

func TestReportRuntimeIngressRestartAndUntrustedRoutes(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, native := seedComposedParentReport(t, s, dir, func(report domain.WorkerReport) (domain.WorkerReportReceipt, error) {
		a, err := reportactor.NewAcceptance(runtimeReportVerifier(func(_ context.Context, got domain.WorkerReport) error {
			if got != report {
				return domain.ErrWorkerReportActorMismatch
			}
			return nil
		}), s)
		if err != nil {
			return domain.WorkerReportReceipt{}, err
		}
		body, err := json.Marshal(report)
		if err != nil {
			return domain.WorkerReportReceipt{}, err
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/worker/reports", bytes.NewReader(body))
		req.RemoteAddr, req.Host = "127.0.0.1:1234", "localhost"
		w := httptest.NewRecorder()
		httpd.NewWorkerReportHandler(a).ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("ingress acceptance: %d %s", w.Code, w.Body.String())
		}
		var receipt domain.WorkerReportReceipt
		err = json.Unmarshal(w.Body.Bytes(), &receipt)
		return receipt, err
	})
	verified := 0
	// A fixture represents possession of an already-issued native caller handle.
	// Production HTTP has no such handle; request fields never mint one.
	a, err := reportactor.NewAcceptance(runtimeReportVerifier(func(_ context.Context, got domain.WorkerReport) error {
		verified++
		if got != r.Report {
			return domain.ErrWorkerReportActorMismatch
		}
		return nil
	}), s)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, peer, host, origin string
		status                   int
	}{
		{"remote", "192.0.2.2:1234", "localhost", "", http.StatusForbidden},
		{"foreign-host", "127.0.0.1:1234", "attacker.example", "", http.StatusForbidden},
		{"browser", "127.0.0.1:1234", "localhost", "https://attacker.example", http.StatusForbidden},
		{"trusted-fixture", "127.0.0.1:1234", "localhost", "", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(r.Report)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/worker/reports", bytes.NewReader(body))
			req.RemoteAddr, req.Host = tc.peer, tc.host
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			httpd.NewWorkerReportHandler(a).ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if verified != 1 || native.sendCalls != 0 {
		t.Fatal("ingress bypassed caller fence or sent directly")
	}
	tr := &capturedOrchestrationTransport{}
	n := &reportAttentionFixture{}
	d := &orchestrationevents.Dispatcher{Store: s, Transport: tr, Notifier: n,
		Reports: &orchestrationevents.ExactParentReports{Store: s, Source: s, Parents: s, Native: native}}
	if err := orchestrationevents.Recover(t.Context(), s, d); err != nil {
		t.Fatal(err)
	}
	e := exactReportEvent(t, s, r)
	m, err := ports.ParseExactReportMessage(e.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != domain.OrchestrationSubmitted || !e.AcknowledgedAt.IsZero() || native.sendCalls != 1 ||
		len(tr.batches) != 0 || m.Parent != native.parent || native.acceptedID != m.ClientMessageID() {
		t.Fatalf("runtime bypass or false ACK: %+v", e)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.Store = s
	d.Reports.Store, d.Reports.Source, d.Reports.Parents = s, s, s
	for range 2 {
		if err := orchestrationevents.Recover(t.Context(), s, d); err != nil {
			t.Fatal(err)
		}
	}
	if got := exactReportEvent(t, s, r); got.LeaseToken != e.LeaseToken || !got.AcknowledgedAt.IsZero() ||
		got.AttentionRequiredAt.IsZero() || native.sendCalls != 1 || native.providerWrites != 1 || len(tr.batches) != 0 {
		t.Fatalf("restart resent/ACKed report: %+v", got)
	}
}

type runtimeParentReader struct {
	ports.ExactReportParentReader
	afterRead func()
}

func TestReportRuntimeLostTransportRestartStableBinding(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, native := seedComposedParentReport(t, s, dir)
	native.firstSendErr = errors.New("fixture lost transport result")
	now := time.Now().UTC()
	exact := &orchestrationevents.ExactParentReports{Store: s, Source: s, Parents: s, Native: native, Now: func() time.Time { return now }}
	d := &orchestrationevents.Dispatcher{Store: s, Reports: exact}
	if err := d.DispatchProject(t.Context(), r.Report.Identity.ProjectID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	prepared := exactReportEvent(t, s, r)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.Store = s
	exact.Store, exact.Source, exact.Parents = s, s, s
	now = now.Add(orchestrationevents.LeaseDuration + time.Second)
	for range 2 {
		if err := d.DispatchProject(t.Context(), r.Report.Identity.ProjectID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
			t.Fatal(err)
		}
	}
	e := exactReportEvent(t, s, r)
	if e.LeaseToken != prepared.LeaseToken || e.DestinationSessionID != prepared.DestinationSessionID ||
		e.State != domain.OrchestrationSubmitted || !e.AcknowledgedAt.IsZero() || native.sendCalls != 2 || native.providerWrites != 1 {
		t.Fatalf("lost result changed binding or duplicated native delivery: %+v", e)
	}
}

func (r runtimeParentReader) ReadReportOriginalParent(ctx context.Context, work domain.WorkItemID, attempt domain.WorkAttemptID) (ports.ExactReportParent, error) {
	p, err := r.ExactReportParentReader.ReadReportOriginalParent(ctx, work, attempt)
	r.afterRead()
	return p, err
}

func TestReportRuntimeReaderLeaseRaceAndUntrustedParent(t *testing.T) {
	for _, change := range []string{"reader-lease-race", "wrong-live-parent", "legacy-parent", "project-hold"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, native := seedComposedParentReport(t, s, dir)
			exact := &orchestrationevents.ExactParentReports{Store: s, Source: s, Parents: s, Native: native}
			switch change {
			case "reader-lease-race":
				exact.Parents = runtimeParentReader{s, func() {
					lease, _, err := s.GetWorkItemLease(t.Context(), r.Report.Identity.ID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := s.RenewWorkItemLease(t.Context(), r.Report.Identity, lease, lease.ExpiresAt.Add(time.Hour)); err != nil {
						t.Fatal(err)
					}
				}}
			case "wrong-live-parent":
				native.observed.SessionID = "untrusted-replacement"
			case "legacy-parent":
				exact.Parents = native
				native.parentErr = domain.ErrWorkItemParentHeld
			case "project-hold":
				if err := s.SetOrchestrationDispatchHold(t.Context(), r.Report.Identity.ProjectID, true); err != nil {
					t.Fatal(err)
				}
			}
			d := &orchestrationevents.Dispatcher{Store: s, Reports: exact}
			if err := d.DispatchProject(t.Context(), r.Report.Identity.ProjectID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) && !errors.Is(err, orchestrationevents.ErrDispatchHeld) {
				t.Fatal(err)
			}
			if e := exactReportEvent(t, s, r); native.sendCalls != 0 || e.AttemptCount != 0 || e.LeaseToken != "" || !e.AcknowledgedAt.IsZero() {
				t.Fatalf("stale/untrusted runtime delivery: %+v", e)
			}
		})
	}
}

// Compose real immutable-parent persistence/readback, admission and the report
// outbox. Only native ownership/delivery/semantics are synthetic. This fixture
// does not establish live issuer acquisition or authenticated provider behavior.
func seedComposedParentReport(t *testing.T, s *sqlite.Store, dir string, ingress ...func(domain.WorkerReport) (domain.WorkerReportReceipt, error)) (domain.WorkerReportReceipt, *exactParentChatFixture) {
	t.Helper()
	work, target := seedParentPersistence(t, s)
	// The shared persistence fixture uses a worker-shaped parent. Delivery needs
	// the original orchestrator role; change only this disposable DB fixture.
	if _, err := reportDB(t, dir).Exec(`UPDATE sessions SET kind='orchestrator' WHERE id=?`, target.OriginalParentSessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PersistWorkItemOriginalParent(t.Context(), work.Identity, work.OriginalNativeSessionID, target); err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireWorkItemLease(t.Context(), work.Identity, domain.WorkLeaseGrant{ID: "composed-lease", Duration: time.Hour, AllowedEffects: []string{"report"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(t.Context(), work.Identity, lease); err != nil {
		t.Fatal(err)
	}
	report := domain.WorkerReport{ID: "composed-report", Type: "checkpoint", Version: 1, Identity: work.Identity,
		LeaseID: lease.Grant.ID, LeaseRevision: lease.Revision, WorkerSessionID: work.Identity.OriginalWorkerSessionID,
		NativeSessionID: work.OriginalNativeSessionID, OriginalNativeSessionID: work.OriginalNativeSessionID,
		NativeTurnID: "composed-worker-turn", Content: "composed checkpoint", Digest: reportDigest("composed checkpoint")}
	accept := func(report domain.WorkerReport) (domain.WorkerReportReceipt, error) {
		return s.AcceptWorkerReport(t.Context(), report)
	}
	if len(ingress) > 0 {
		accept = ingress[0]
	}
	receipt, err := accept(report)
	if err != nil {
		t.Fatal(err)
	}
	native := &exactParentChatFixture{parent: ports.ExactReportParent{
		WorkItemID: target.WorkItemID, AttemptID: target.AttemptID, SessionID: target.OriginalParentSessionID,
		NativeSessionID: target.NativeSessionID, ControllerGeneration: target.ControllerGeneration,
		ProviderConversationID: target.ProviderConversationID, ProviderScopeID: target.ProviderScopeID}}
	native.observed = native.parent
	if native.parent.NativeSessionID == report.OriginalNativeSessionID {
		t.Fatal("fixture must distinguish parent and worker native identities")
	}
	return receipt, native
}

func TestComposedParentReportReceiptLostACKAndRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, native := seedComposedParentReport(t, s, dir)
	before := exactReportEvent(t, s, r)
	if before.State != domain.OrchestrationPending || !before.AcknowledgedAt.IsZero() {
		t.Fatalf("accepted receipt became semantic ACK: %+v", before)
	}
	d := &orchestrationevents.ExactParentReports{Store: s, Parents: s, Native: native}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	submitted := exactReportEvent(t, s, r)
	m, err := ports.ParseExactReportMessage(submitted.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if m.Parent != native.parent || submitted.DestinationSessionID != r.Report.Identity.OriginalParentSessionID ||
		submitted.State != domain.OrchestrationSubmitted || !submitted.AcknowledgedAt.IsZero() || native.sendCalls != 1 {
		t.Fatalf("wrong parent or admission-as-ACK: %+v", submitted)
	}
	if n, err := s.AcknowledgeOrchestrationBatchAccepted(t.Context(), m.Parent.SessionID, m.Token(), time.Now()); err != nil || n != 0 {
		t.Fatalf("generic ACK advanced report: %d %v", n, err)
	}
	native.authenticated = true
	native.semantic = ports.ExactReportReconciliation{NativeEventID: "composed-semantic-event", ClientMessageID: m.ClientMessageID(), Message: m}
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, native.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	resolved := exactReportEvent(t, s, r)
	if resolved.State != domain.OrchestrationAcknowledged || resolved.AttentionRequiredAt.IsZero() {
		t.Fatalf("semantic ACK lost notification retry: %+v", resolved)
	}
	// Discard the response, close and reopen the real store, then replay ACK.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d = &orchestrationevents.ExactParentReports{Store: s, Parents: s, Native: native}
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, native.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); err != nil {
		t.Fatal(err)
	}
	if got := exactReportEvent(t, s, r); !reflect.DeepEqual(got, resolved) || native.sendCalls != 1 || native.providerWrites != 1 {
		t.Fatalf("lost ACK replay changed state or sent again: %+v", got)
	}
	if got, found, err := s.GetWorkerReport(t.Context(), r.Report.ID); err != nil || !found || got != r {
		t.Fatalf("immutable receipt changed: %+v %v", got, err)
	}
}

func TestComposedParentReportStaleLeaseGenerationAndWrongParent(t *testing.T) {
	for _, change := range []string{"lease_revision", "owner_generation", "parent_generation", "wrong_parent", "worker_native"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, native := seedComposedParentReport(t, s, dir)
			switch change {
			case "lease_revision":
				lease, _, err := s.GetWorkItemLease(t.Context(), r.Report.Identity.ID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.RenewWorkItemLease(t.Context(), r.Report.Identity, lease, lease.ExpiresAt.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			case "owner_generation":
				work, _, err := s.GetWorkItem(t.Context(), r.Report.Identity.ID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.ClaimWorkItemOwner(t.Context(), work, "replacement-owner"); err != nil {
					t.Fatal(err)
				}
			case "parent_generation":
				native.observed.ControllerGeneration = "replacement-generation"
			case "wrong_parent":
				native.observed.SessionID = "replacement-orchestrator"
			case "worker_native":
				native.observed.NativeSessionID = r.Report.OriginalNativeSessionID
			}
			parent, err := s.ReadReportOriginalParent(t.Context(), r.Report.Identity.ID, r.Report.Identity.AttemptID)
			if err != nil || parent != native.parent {
				t.Fatalf("ownership change retargeted immutable parent: %+v %v", parent, err)
			}
			d := &orchestrationevents.ExactParentReports{Store: s, Parents: s, Native: native}
			if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			e := exactReportEvent(t, s, r)
			if native.sendCalls != 0 || e.AttemptCount != 0 || e.LeaseToken != "" || e.DestinationSessionID != "" ||
				e.State != domain.OrchestrationPending || !e.AcknowledgedAt.IsZero() || e.AttentionRequiredAt.IsZero() {
				t.Fatalf("stale source or wrong parent sent: %+v", e)
			}
		})
	}
}

func TestComposedParentReportDeadLetterRestartNoResend(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, native := seedComposedParentReport(t, s, dir)
	native.firstSendErr = errors.New("disposable lost transport result")
	native.repeatSendErr = native.firstSendErr
	now := time.Now().UTC()
	d := &orchestrationevents.ExactParentReports{Store: s, Parents: s, Native: native, Now: func() time.Time { return now }}
	for range 8 {
		if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
			t.Fatal(err)
		}
		now = now.Add(orchestrationevents.LeaseDuration + time.Second)
	}
	prepared := exactReportEvent(t, s, r)
	if prepared.AttemptCount != 8 || native.sendCalls != 8 || native.providerWrites != 1 {
		t.Fatalf("retry exceeded stable delivery budget: %+v", prepared)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ReclaimOrchestrationEventLeases(t.Context(), now); err != nil || n != 1 {
		t.Fatalf("restart reclaim: %d %v", n, err)
	}
	d.Store, d.Parents = s, s
	if err := d.Deliver(t.Context(), r.Report.Identity.ProjectID, r.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || native.sendCalls != 8 {
		t.Fatalf("dead letter resent: %v", err)
	}
	dead := exactReportEvent(t, s, r)
	if dead.State != domain.OrchestrationDeadLetter || dead.LeaseToken != prepared.LeaseToken || dead.DestinationSessionID != prepared.DestinationSessionID {
		t.Fatalf("restart lost original binding: %+v", dead)
	}
	m, err := ports.ParseExactReportMessage(dead.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	native.authenticated = true
	native.semantic = ports.ExactReportReconciliation{NativeEventID: "composed-late-semantic", ClientMessageID: m.ClientMessageID(), Message: m}
	if err := d.Reconcile(t.Context(), r.Report.Identity.ProjectID, r.Report.ID, native.semantic.NativeEventID); err != nil {
		t.Fatal(err)
	}
	if e := exactReportEvent(t, s, r); e.State != domain.OrchestrationAcknowledged || e.AttentionRequiredAt.IsZero() || native.sendCalls != 8 {
		t.Fatalf("historical semantic reconciliation resent or lost attention: %+v", e)
	}
}

func TestComposedParentReaderRefusesLegacyAndForeignAttempt(t *testing.T) {
	s := newTestStore(t)
	r, _ := seedExactParentDelivery(t, s) // legacy work has no issued target
	if target, err := s.ReadReportOriginalParent(t.Context(), r.Report.Identity.ID, r.Report.Identity.AttemptID); target != (ports.ExactReportParent{}) || !errors.Is(err, domain.ErrWorkItemParentHeld) {
		t.Fatalf("legacy origin backfilled: %+v %v", target, err)
	}
	if target, err := s.ReadReportOriginalParent(t.Context(), r.Report.Identity.ID, "foreign-attempt"); target != (ports.ExactReportParent{}) || !errors.Is(err, domain.ErrWorkIdentityMismatch) {
		t.Fatalf("foreign attempt projected: %+v %v", target, err)
	}
}
