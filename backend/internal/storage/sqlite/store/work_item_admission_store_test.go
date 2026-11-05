package store_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func admissionFixture(t *testing.T, ctx context.Context, s *sqlite.Store, suffix string) (store.WorkItemRecord, domain.WorkItemLease) {
	t.Helper()
	r := workLeaseRecord()
	r.Identity.ID += domain.WorkItemID(suffix)
	r.Identity.AttemptID += domain.WorkAttemptID(suffix)
	r.Identity.OwnerSessionID += domain.SessionID(suffix)
	r.Identity.ProjectID += domain.ProjectID(suffix) // capacity also spans projects
	r.Identity.WorktreePath += suffix
	r.Identity.Branch += suffix
	if _, err := s.CreateWorkItem(ctx, r); err != nil {
		t.Fatal(err)
	}
	l, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease"+suffix))
	if err != nil {
		t.Fatal(err)
	}
	return r, l
}

func admissionDB(t *testing.T, dir string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWorkAdmissionRestartRetryAndRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	a, lease := admissionFixture(t, ctx, s, "-a")
	b, otherLease := admissionFixture(t, ctx, s, "-b")
	receipt, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease)
	if err != nil || receipt.State != domain.WorkAdmissionReserved || receipt.AttemptID != a.Identity.AttemptID {
		t.Fatalf("reserve: %+v %v", receipt, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Bounded retries after lost response/reopen must not renew or create rows.
	for range 4 {
		if got, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease); err != nil || got != receipt {
			t.Fatalf("retry: %+v %v", got, err)
		}
		if _, err := s.ReserveWorkItemAdmission(ctx, b.Identity, otherLease); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
			t.Fatalf("other WorkItem admitted: %v", err)
		}
	}
	if got, _, err := s.GetWorkItemLease(ctx, a.Identity.ID); err != nil || got.Revision != lease.Revision || !got.ExpiresAt.Equal(lease.ExpiresAt) {
		t.Fatalf("retry changed lease: %+v %v", got, err)
	}
	for range 2 {
		if got, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); err != nil || got.State != domain.WorkAdmissionReleased {
			t.Fatalf("release: %+v %v", got, err)
		}
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("released attempt readmitted: %v", err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, b.Identity, otherLease); err != nil {
		t.Fatalf("next attempt: %v", err)
	}
	// A delayed release retry cannot free the next reservation.
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); err != nil {
		t.Fatal(err)
	}
	db := admissionDB(t, dir)
	var reserved, retained int
	if err := db.QueryRowContext(ctx, `SELECT count(*), sum(state='reserved') FROM work_item_admissions`).Scan(&retained, &reserved); err != nil || retained != 2 || reserved != 1 {
		t.Fatalf("occupancy: retained=%d reserved=%d %v", retained, reserved, err)
	}
	requireWorkItem(t, s, a) // candidate evolution never rewrites execution base
}

func TestWorkAdmissionFencesAndTimeoutHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, lease := admissionFixture(t, ctx, s, "-a")
	b, otherLease := admissionFixture(t, ctx, s, "-b")
	for _, mutate := range []func(*domain.WorkItemIdentity){
		func(i *domain.WorkItemIdentity) { i.OwnerGeneration++ },
		func(i *domain.WorkItemIdentity) { i.OwnerSessionID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.AttemptID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.ExecutionBaseSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
	} {
		wrong := a.Identity
		mutate(&wrong)
		if _, err := s.ReserveWorkItemAdmission(ctx, wrong, lease); err == nil {
			t.Fatalf("foreign identity accepted: %+v", wrong)
		}
	}
	for _, mutate := range []func(*domain.WorkItemLease){
		func(l *domain.WorkItemLease) { l.Revision++ },
		func(l *domain.WorkItemLease) { l.Grant.ID = "foreign" },
		func(l *domain.WorkItemLease) { l.WorkItemID = b.Identity.ID },
		func(l *domain.WorkItemLease) { l.ExpiresAt = l.ExpiresAt.Add(time.Hour) },
	} {
		wrong := lease
		mutate(&wrong)
		if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, wrong); !errors.Is(err, domain.ErrWorkLeaseConflict) {
			t.Fatalf("foreign lease accepted: %v", err)
		}
	}
	receipt, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease)
	if err != nil {
		t.Fatal(err)
	}
	wrong := receipt
	wrong.LeaseRevision++
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, wrong); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("foreign receipt released: %v", err)
	}
	renewed, err := s.RenewWorkItemLease(ctx, a.Identity, lease, lease.ExpiresAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, renewed); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("new lease revision readmitted attempt: %v", err)
	}
	db := admissionDB(t, dir)
	if _, err := db.ExecContext(ctx, `UPDATE work_item_leases SET expires_at_ms=1 WHERE work_item_id=?`, a.Identity.ID); err != nil {
		t.Fatal(err)
	}
	expired, _, err := s.GetWorkItemLease(ctx, a.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, expired); !errors.Is(err, domain.ErrWorkLeaseExpired) {
		t.Fatalf("expired retry accepted: %v", err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, b.Identity, otherLease); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("timeout reclaimed occupancy: %v", err)
	}
	if _, err := s.ReleaseWorkItemLease(ctx, a.Identity, expired); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, b.Identity, otherLease); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("lease release reclaimed occupancy: %v", err)
	}
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); err != nil {
		t.Fatalf("explicit reconciled release after timeout: %v", err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, b.Identity, otherLease); err != nil {
		t.Fatal(err)
	}
}

func TestWorkAdmissionOwnerChangeRetainsAmbiguousSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, lease := admissionFixture(t, ctx, s, "-a")
	b, otherLease := admissionFixture(t, ctx, s, "-b")
	receipt, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease)
	if err != nil {
		t.Fatal(err)
	}
	nextLease, err := s.SupersedeWorkItemLease(ctx, a.Identity, lease, "next-owner", leaseGrant("next-lease"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease); !errors.Is(err, domain.ErrStaleWorkOwner) {
		t.Fatalf("stale reserve: %v", err)
	}
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); !errors.Is(err, domain.ErrStaleWorkOwner) {
		t.Fatalf("stale release: %v", err)
	}
	a.Identity.OwnerGeneration++
	a.Identity.OwnerSessionID = "next-owner"
	if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, nextLease); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("new owner reclaimed old reservation: %v", err)
	}
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("new owner released old reservation: %v", err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, b.Identity, otherLease); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("ambiguous reservation freed: %v", err)
	}
}

func TestWorkAdmissionConcurrentIndependentStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	var stores [2]*sqlite.Store
	var records [2]store.WorkItemRecord
	var leases [2]domain.WorkItemLease
	for i, suffix := range []string{"-a", "-b"} {
		s, err := sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		stores[i] = s
		records[i], leases[i] = admissionFixture(t, ctx, s, suffix)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var errs [2]error
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = stores[i].ReserveWorkItemAdmission(ctx, records[i].Identity, leases[i])
		}()
	}
	close(start)
	wg.Wait()
	winners := 0
	winner := 0
	for i, err := range errs {
		if err == nil {
			winners++
			winner = i
		} else if !errors.Is(err, domain.ErrWorkAdmissionHeld) {
			t.Fatalf("unexpected contender result: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d, want 1", winners)
	}
	// Concurrent retries of the winning attempt get one retained receipt,
	// rather than a new slot or an extended lease for each caller.
	var receipts [2]domain.WorkItemAdmission
	start = make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			receipts[i], errs[i] = stores[i].ReserveWorkItemAdmission(ctx, records[winner].Identity, leases[winner])
		}()
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || receipts[0] != receipts[1] {
		t.Fatalf("duplicate attempt retries: %+v %v", receipts, errs)
	}
	db := admissionDB(t, dir)
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM work_item_admissions`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("duplicate attempt reservations: %d %v", n, err)
	}
}

func TestWorkAdmissionRollbackAndCanceledRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, lease := admissionFixture(t, ctx, s, "-a")
	db := admissionDB(t, dir)
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_admission AFTER INSERT ON work_item_admissions BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease); err == nil {
		t.Fatal("injected failure accepted")
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM work_item_admissions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rollback: %d %v", n, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_admission`); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := s.ReserveWorkItemAdmission(canceled, a.Identity, lease); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reserve: %v", err)
	}
	receipt, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_release AFTER UPDATE ON work_item_admissions BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); err == nil {
		t.Fatal("failed release accepted")
	}
	if got, err := s.ReserveWorkItemAdmission(ctx, a.Identity, lease); err != nil || got != receipt {
		t.Fatalf("failed release lost occupancy: %+v %v", got, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_release`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReleaseWorkItemAdmission(canceled, a.Identity, receipt); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled release: %v", err)
	}
	if _, err := s.ReleaseWorkItemAdmission(ctx, a.Identity, receipt); err != nil {
		t.Fatalf("retry after failed/canceled release: %v", err)
	}
	requireWorkItem(t, s, a)
}
