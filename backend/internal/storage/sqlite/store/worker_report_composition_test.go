package store_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// Compose real immutable-parent persistence/readback, admission and the report
// outbox. Only native ownership/delivery/semantics are synthetic. This fixture
// does not establish live issuer acquisition or authenticated provider behavior.
func seedComposedParentReport(t *testing.T, s *sqlite.Store, dir string) (domain.WorkerReportReceipt, *exactParentChatFixture) {
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
	receipt, err := s.AcceptWorkerReport(t.Context(), report)
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
