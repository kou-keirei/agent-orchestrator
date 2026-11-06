package reportactor_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func acceptanceFixture(t *testing.T) (*sqlite.Store, string, domain.WorkerReport) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := reportClaim()
	now := time.Now().UTC()
	if err := s.UpsertProject(ctx, domain.ProjectRecord{ID: string(r.Identity.ProjectID), Path: dir, RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	worker, err := s.CreateSession(ctx, domain.SessionRecord{
		ProjectID: r.Identity.ProjectID, Kind: domain.KindWorker, Harness: domain.HarnessFake,
		Activity:  domain.Activity{State: domain.ActivityActive, LastActivityAt: now},
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.WorkerSessionID, r.Identity.OwnerSessionID, r.Identity.OriginalWorkerSessionID = worker.ID, worker.ID, worker.ID
	if _, err := s.CreateWorkItem(ctx, store.WorkItemRecord{Identity: r.Identity, OriginalNativeSessionID: r.OriginalNativeSessionID}); err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireWorkItemLease(ctx, r.Identity, domain.WorkLeaseGrant{ID: r.LeaseID, Duration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	r.LeaseRevision = lease.Revision
	return s, dir, r
}

func assertAcceptanceState(t *testing.T, s *sqlite.Store, r domain.WorkerReport, wantEvents int) {
	t.Helper()
	_, found, err := s.GetWorkerReport(context.Background(), r.ID)
	if err != nil || found != (wantEvents == 1) {
		t.Fatalf("receipt found=%v err=%v, want events=%d", found, err, wantEvents)
	}
	events, err := s.ListOrchestrationEvents(context.Background(), r.Identity.ProjectID, 10)
	if err != nil || len(events) != wantEvents {
		t.Fatalf("outbox=%+v err=%v", events, err)
	}
	for _, event := range events {
		if event.State != domain.OrchestrationPending || !event.AcknowledgedAt.IsZero() || !event.SubmittedAt.IsZero() {
			t.Fatalf("acceptance advanced delivery: %+v", event)
		}
	}
}

func TestAcceptanceSQLiteUnavailableCannotReadAcceptedRetry(t *testing.T) {
	s, _, r := acceptanceFixture(t)
	a, err := reportactor.NewAcceptance(reportactor.Unavailable{}, s)
	if err != nil {
		t.Fatal(err)
	}
	for _, alreadyAccepted := range []bool{false, true} {
		if alreadyAccepted {
			// Persistence fixture only. This does not qualify a caller issuer.
			if _, err := s.AcceptWorkerReport(context.Background(), r); err != nil {
				t.Fatal(err)
			}
		}
		receipt, err := a.SubmitWorkerReport(context.Background(), r)
		requireHold(t, err)
		if receipt != (domain.WorkerReportReceipt{}) {
			t.Fatalf("unverified retry disclosed receipt: %+v", receipt)
		}
		want := 0
		if alreadyAccepted {
			want = 1
		}
		assertAcceptanceState(t, s, r, want)
	}
}

// A successful injected verifier here tests composition and the real writer's
// independent transaction checks only. It is not an authenticated native actor,
// provider event, qualified positive issuer, or production acceptance evidence.
func TestAcceptanceSQLiteWriterStillRejectsInvalidOwnership(t *testing.T) {
	for name, mutate := range map[string]func(*domain.WorkerReport){
		"spoofed-worker":               func(r *domain.WorkerReport) { r.WorkerSessionID, r.Identity.OwnerSessionID = "other", "other" },
		"wrong-work-item":              func(r *domain.WorkerReport) { r.Identity.ID = "other" },
		"wrong-attempt":                func(r *domain.WorkerReport) { r.Identity.AttemptID = "other" },
		"wrong-generation":             func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ },
		"wrong-native-session":         func(r *domain.WorkerReport) { r.NativeSessionID = "other" },
		"original-parent-substitution": func(r *domain.WorkerReport) { r.Identity.OriginalParentSessionID = "other" },
		"original-native-substitution": func(r *domain.WorkerReport) { r.OriginalNativeSessionID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			s, _, original := acceptanceFixture(t)
			a, err := reportactor.NewAcceptance(verifierFunc(func(context.Context, domain.WorkerReport) error { return nil }), s)
			if err != nil {
				t.Fatal(err)
			}
			r := original
			mutate(&r)
			if receipt, err := a.SubmitWorkerReport(context.Background(), r); err == nil || receipt != (domain.WorkerReportReceipt{}) {
				t.Fatalf("invalid ownership accepted: %+v %v", receipt, err)
			}
			assertAcceptanceState(t, s, original, 0)
		})
	}
}

func TestAcceptanceSQLiteLeaseChangesDuringVerification(t *testing.T) {
	for _, state := range []string{"expired", "released", "renewed", "owner-changed"} {
		t.Run(state, func(t *testing.T) {
			s, dir, r := acceptanceFixture(t)
			a, err := reportactor.NewAcceptance(verifierFunc(func(ctx context.Context, _ domain.WorkerReport) error {
				lease, _, err := s.GetWorkItemLease(ctx, r.Identity.ID)
				if err != nil {
					return err
				}
				switch state {
				case "expired":
					// Only this disposable fixture DB is opened. No primary AO data.
					db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "ao.db")))
					if err != nil {
						return err
					}
					defer db.Close()
					_, err = db.ExecContext(ctx, `UPDATE work_item_leases SET expires_at_ms=?`, time.Now().Add(-time.Second).UnixMilli())
					return err
				case "released":
					_, err = s.ReleaseWorkItemLease(ctx, r.Identity, lease)
				case "renewed":
					_, err = s.RenewWorkItemLease(ctx, r.Identity, lease, lease.ExpiresAt.Add(time.Hour))
				case "owner-changed":
					record, _, readErr := s.GetWorkItem(ctx, r.Identity.ID)
					if readErr != nil {
						return readErr
					}
					_, err = s.ClaimWorkItemOwner(ctx, record, "new-owner")
				}
				return err
			}), s)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := a.SubmitWorkerReport(context.Background(), r)
			if err == nil || receipt != (domain.WorkerReportReceipt{}) {
				t.Fatalf("lease changed before writer but accepted: %+v %v", receipt, err)
			}
			if state == "expired" && !errors.Is(err, domain.ErrWorkLeaseExpired) {
				t.Fatalf("expiry error: %v", err)
			}
			assertAcceptanceState(t, s, r, 0)
		})
	}
}

func TestAcceptanceSQLiteRetryNeverAcknowledgesParent(t *testing.T) {
	s, _, r := acceptanceFixture(t)
	verified := 0
	a, err := reportactor.NewAcceptance(verifierFunc(func(context.Context, domain.WorkerReport) error {
		verified++
		return nil
	}), s)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	want, err := a.SubmitWorkerReport(ctx, r)
	if err != nil || want.EventID == "" {
		t.Fatalf("fixture-verified acceptance: %+v %v", want, err)
	}
	for range 2 {
		if got, err := a.SubmitWorkerReport(ctx, r); err != nil || got != want {
			t.Fatalf("retry: %+v %v", got, err)
		}
		// A duplicate generic ACK cannot turn acceptance into parent semantics.
		if err := s.AcknowledgeOrchestrationEvents(ctx, []string{want.EventID, want.EventID}, "invented-ack", time.Now()); err == nil {
			t.Fatalf("duplicate generic ACK admitted: %v", err)
		}
	}
	if verified != 3 {
		t.Fatalf("retry bypassed verifier: %d", verified)
	}
	assertAcceptanceState(t, s, r, 1)
}
