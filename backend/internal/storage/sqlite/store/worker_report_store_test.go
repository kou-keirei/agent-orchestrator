package store_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func reportDigest(content string) string {
	digest := sha256.Sum256([]byte(content))
	return hex.EncodeToString(digest[:])
}

func seedWorkerReport(t *testing.T, s *sqlite.Store) domain.WorkerReport {
	t.Helper()
	ctx := context.Background()
	seedProject(t, s, "project-1")
	worker, err := s.CreateSession(ctx, sampleRecord("project-1"))
	if err != nil {
		t.Fatal(err)
	}
	r := workLeaseRecord()
	r.Identity.OriginalWorkerSessionID, r.Identity.OwnerSessionID = worker.ID, worker.ID
	if _, err := s.CreateWorkItem(ctx, r); err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-1"))
	if err != nil {
		t.Fatal(err)
	}
	return domain.WorkerReport{ID: "report-1", Type: "checkpoint", Version: 1, Identity: r.Identity,
		LeaseID: lease.Grant.ID, LeaseRevision: lease.Revision, WorkerSessionID: worker.ID,
		NativeSessionID: r.OriginalNativeSessionID, NativeTurnID: "turn-1", OriginalNativeSessionID: r.OriginalNativeSessionID,
		Content: "explicit worker checkpoint", Digest: reportDigest("explicit worker checkpoint")}
}

func reportDB(t *testing.T, dir string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "ao.db"))+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWorkerReportRestartAndRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := seedWorkerReport(t, s)
	now := time.Now().UTC()
	old := domain.OrchestrationEvent{ID: "old-event", ProjectID: r.Identity.ProjectID, WorkerID: r.WorkerSessionID, Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "old-revision", EnqueuedAt: now, NextAttemptAt: now}
	if _, err := s.EnqueueOrchestrationEvent(ctx, old); err != nil {
		t.Fatal(err)
	}
	accepted, err := s.AcceptWorkerReport(ctx, r)
	if err != nil || accepted.Report != r || accepted.EventID == "" || accepted.AcceptedAtMS <= 0 {
		t.Fatalf("accept: %+v %v", accepted, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetWorkerReport(ctx, r.ID)
	if err != nil || !found || got != accepted {
		t.Fatalf("restart: %+v %v %v", got, found, err)
	}
	lease, _, err := s.GetWorkItemLease(ctx, r.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReleaseWorkItemLease(ctx, r.Identity, lease); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err = s.AcceptWorkerReport(ctx, r)
		if err != nil || got != accepted {
			t.Fatalf("retry: %+v %v", got, err)
		}
	}
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events: %+v %v", events, err)
	}
	for _, e := range events {
		if e.ID == old.ID {
			if e.Kind != old.Kind || e.SourceRevision != old.SourceRevision {
				t.Fatal("old event changed")
			}
			continue
		}
		bytes, _ := json.Marshal(r)
		sum := sha256.Sum256(bytes)
		if e.ID != accepted.EventID || e.Kind != domain.OrchestrationWorkerReport || e.SourceRevision != "worker-report:v1:"+hex.EncodeToString(sum[:]) || e.State != domain.OrchestrationPending || !e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() {
			t.Fatalf("event: %+v", e)
		}
	}
}

func TestWorkerReportRejectsChangedEnvelope(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	r := seedWorkerReport(t, s)
	if _, err := s.AcceptWorkerReport(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*domain.WorkerReport){
		func(r *domain.WorkerReport) { r.Content = "changed"; r.Digest = reportDigest(r.Content) },
		func(r *domain.WorkerReport) { r.Type = "done" },
		func(r *domain.WorkerReport) { r.NativeTurnID = "another-turn" },
		func(r *domain.WorkerReport) { r.NativeSessionID = "another-session" },
		func(r *domain.WorkerReport) { r.LeaseRevision++ },
		func(r *domain.WorkerReport) { r.Identity.OriginalParentSessionID = "foreign" },
	} {
		wrong := r
		mutate(&wrong)
		if _, err := s.AcceptWorkerReport(ctx, wrong); !errors.Is(err, domain.ErrWorkerReportConflict) {
			t.Fatalf("changed payload: %v", err)
		}
	}
	wrong := r
	wrong.Digest = strings.Repeat("0", 64)
	if _, err := s.AcceptWorkerReport(ctx, wrong); !errors.Is(err, domain.ErrInvalidWorkerReport) {
		t.Fatalf("digest: %v", err)
	}
}

func TestWorkerReportFreshIdentityAndLease(t *testing.T) {
	for name, mutate := range map[string]func(*domain.WorkerReport){
		"owner":         func(r *domain.WorkerReport) { r.Identity.OwnerSessionID = "foreign"; r.WorkerSessionID = "foreign" },
		"generation":    func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ },
		"lease":         func(r *domain.WorkerReport) { r.LeaseID = "foreign" },
		"revision":      func(r *domain.WorkerReport) { r.LeaseRevision++ },
		"attempt":       func(r *domain.WorkerReport) { r.Identity.AttemptID = "foreign" },
		"repo":          func(r *domain.WorkerReport) { r.Identity.Repository = "foreign" },
		"project":       func(r *domain.WorkerReport) { r.Identity.ProjectID = "foreign" },
		"parent":        func(r *domain.WorkerReport) { r.Identity.OriginalParentSessionID = "foreign" },
		"native-source": func(r *domain.WorkerReport) { r.OriginalNativeSessionID = "foreign" },
		"native-worker": func(r *domain.WorkerReport) { r.NativeSessionID = "foreign" },
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			r := seedWorkerReport(t, s)
			mutate(&r)
			if _, err := s.AcceptWorkerReport(context.Background(), r); err == nil {
				t.Fatal("foreign report accepted")
			}
			if _, found, err := s.GetWorkerReport(context.Background(), r.ID); err != nil || found {
				t.Fatalf("rejected report persisted: %v %v", found, err)
			}
		})
	}
	for _, state := range []string{"expired", "released", "completed", "renewed", "superseded", "identity-only-claim"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			s, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r := seedWorkerReport(t, s)
			ctx := context.Background()
			lease, _, err := s.GetWorkItemLease(ctx, r.Identity.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "expired":
				_, err = reportDB(t, dir).Exec(`UPDATE work_item_leases SET expires_at_ms=?`, time.Now().Add(-time.Second).UnixMilli())
			case "released":
				_, err = s.ReleaseWorkItemLease(ctx, r.Identity, lease)
			case "completed":
				_, err = s.CompleteWorkItemLease(ctx, r.Identity, lease)
			case "renewed":
				_, err = s.RenewWorkItemLease(ctx, r.Identity, lease, lease.ExpiresAt.Add(time.Hour))
			case "superseded":
				_, err = s.SupersedeWorkItemLease(ctx, r.Identity, lease, "new-owner", leaseGrant("lease-2"))
			case "identity-only-claim":
				record, _, _ := s.GetWorkItem(ctx, r.Identity.ID)
				_, err = s.ClaimWorkItemOwner(ctx, record, "new-owner")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.AcceptWorkerReport(ctx, r); err == nil {
				t.Fatal("stale/terminal lease accepted")
			}
			if state == "expired" && !errors.Is(err, domain.ErrWorkLeaseExpired) {
				t.Fatalf("expiry: %v", err)
			}
		})
	}
}

func TestWorkerReportAtomicRollbackAndBinding(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r := seedWorkerReport(t, s)
	db := reportDB(t, dir)
	if _, err = db.Exec(`CREATE TRIGGER reject_report_enqueue BEFORE INSERT ON orchestration_events WHEN NEW.kind='worker_report' BEGIN SELECT RAISE(ABORT,'injected enqueue failure'); END`); err != nil {
		t.Fatal(err)
	}
	if receipt, err := s.AcceptWorkerReport(ctx, r); err == nil || receipt.EventID != "" {
		t.Fatalf("rollback receipt: %+v %v", receipt, err)
	}
	if _, found, err := s.GetWorkerReport(ctx, r.ID); err != nil || found {
		t.Fatalf("partial report: %v %v", found, err)
	}
	if _, err = db.Exec(`DROP TRIGGER reject_report_enqueue`); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.AcceptWorkerReport(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE worker_reports SET envelope='changed'`,
		`DELETE FROM worker_reports`,
		`UPDATE orchestration_events SET source_revision='changed' WHERE kind='worker_report'`,
		`UPDATE orchestration_events SET worker_report_id=NULL WHERE kind='worker_report'`,
		`INSERT INTO orchestration_events(id,project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at) SELECT 'fake',project_id,worker_id,'worker_report','fake',enqueued_at,next_attempt_at FROM orchestration_events LIMIT 1`,
	} {
		if _, err = db.Exec(statement); err == nil {
			t.Fatalf("immutable/binding violation accepted: %s", statement)
		}
	}
	if _, err = db.Exec(`UPDATE orchestration_events SET state='submitted' WHERE id=?`, receipt.EventID); err != nil {
		t.Fatal(err)
	}
	if got, err := s.AcceptWorkerReport(ctx, r); err != nil || got != receipt {
		t.Fatalf("submitted retry: %+v %v", got, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 5)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationSubmitted {
		t.Fatalf("retry reset delivery: %+v %v", events, err)
	}
}

func TestWorkerReportConcurrentIndependentOpens(t *testing.T) {
	for _, differentIDs := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-id", true: "duplicate-done"}[differentIDs], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			first, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			r := seedWorkerReport(t, first)
			r.Type = "done"
			second, err := sqlite.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			start := make(chan struct{})
			var wg sync.WaitGroup
			results := make(chan error, 2)
			for idx, s := range []*sqlite.Store{first, second} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					report := r
					if differentIDs && idx == 1 {
						report.ID = "other-done"
					}
					_, err := s.AcceptWorkerReport(ctx, report)
					results <- err
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			conflicts := 0
			for err := range results {
				if errors.Is(err, domain.ErrWorkerReportConflict) {
					conflicts++
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if differentIDs && conflicts != 1 || !differentIDs && conflicts != 0 {
				t.Fatalf("conflicts=%d", conflicts)
			}
			events, err := first.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
			if err != nil || len(events) != 1 {
				t.Fatalf("events: %+v %v", events, err)
			}
			got, found, err := first.GetWorkerReport(ctx, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !differentIDs && (!found || !reflect.DeepEqual(got.Report, r)) {
				t.Fatalf("report: %+v %v", got, found)
			}
		})
	}
}

func TestWorkerReportDoneSurvivesOwnerChangeAndCleanup(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r := seedWorkerReport(t, s)
	r.Type = "done"
	receipt, err := s.AcceptWorkerReport(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := s.CreateSession(ctx, sampleRecord("project-1"))
	if err != nil {
		t.Fatal(err)
	}
	lease, _, err := s.GetWorkItemLease(ctx, r.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.SupersedeWorkItemLease(ctx, r.Identity, lease, owner.ID, leaseGrant("lease-2"))
	if err != nil {
		t.Fatal(err)
	}
	newReport := r
	newReport.ID = "second-done"
	newReport.Identity.OwnerGeneration = next.OwnerGeneration
	newReport.Identity.OwnerSessionID = owner.ID
	newReport.WorkerSessionID = owner.ID
	newReport.LeaseID = next.Grant.ID
	newReport.LeaseRevision = next.Revision
	newReport.NativeSessionID = "new-owner-native"
	if _, err := s.AcceptWorkerReport(ctx, newReport); !errors.Is(err, domain.ErrWorkerReportConflict) {
		t.Fatalf("second-generation done: %v", err)
	}
	if got, err := s.AcceptWorkerReport(ctx, r); err != nil || got != receipt {
		t.Fatalf("historical retry: %+v %v", got, err)
	}
	db := reportDB(t, dir)
	if _, err := db.Exec(`DELETE FROM change_log WHERE session_id=?`, r.WorkerSessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM sessions WHERE id=?`, r.WorkerSessionID); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetWorkerReport(ctx, r.ID)
	if err != nil || !found || got != receipt {
		t.Fatalf("cleanup lost report: %+v %v %v", got, found, err)
	}
	if got, err := s.AcceptWorkerReport(ctx, r); err != nil || got != receipt {
		t.Fatalf("cleanup retry: %+v %v", got, err)
	}
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("cleanup retry re-enqueued: %+v %v", events, err)
	}
}

func TestWorkerReportAllTypesAndLostResponseRetry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	r := seedWorkerReport(t, s)
	for _, kind := range []string{"checkpoint", "needs_input", "stuck", "failure", "artifact", "done"} {
		report := r
		report.ID = kind
		report.Type = kind
		if kind == "artifact" {
			report.Content = ""
			report.ArtifactReference = "artifact://local-result"
		}
		receipt, err := s.AcceptWorkerReport(ctx, report)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if got, err := s.AcceptWorkerReport(ctx, report); err != nil || got != receipt {
			t.Fatalf("lost response %s: %+v %v", kind, got, err)
		}
	}
	events, err := s.ListOrchestrationEvents(ctx, r.Identity.ProjectID, 20)
	if err != nil || len(events) != 6 {
		t.Fatalf("events: %+v %v", events, err)
	}
}

func TestWorkerReportWaitsForDurableWriter(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := seedWorkerReport(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	other, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	db := reportDB(t, dir)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_leases SET revision=revision+1 WHERE work_item_id=?`, r.Identity.ID); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() { close(started); _, err := other.AcceptWorkerReport(ctx, r); result <- err }()
	<-started
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("stale writer accepted: %v", err)
	}
	if _, found, err := s.GetWorkerReport(ctx, r.ID); err != nil || found {
		t.Fatalf("stale report persisted: %v %v", found, err)
	}
}

func TestWorkerReportExpiredRetryAndCurrentOwnerWithOldLease(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r := seedWorkerReport(t, s)
	receipt, err := s.AcceptWorkerReport(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	db := reportDB(t, dir)
	if _, err := db.Exec(`UPDATE work_item_leases SET expires_at_ms=?`, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if got, err := s.AcceptWorkerReport(ctx, r); err != nil || got != receipt {
		t.Fatalf("expired retry: %+v %v", got, err)
	}
	newReport := r
	newReport.ID = "new-after-expiry"
	if _, err := s.AcceptWorkerReport(ctx, newReport); !errors.Is(err, domain.ErrWorkLeaseExpired) {
		t.Fatalf("new expired report: %v", err)
	}
	owner, err := s.CreateSession(ctx, sampleRecord("project-1"))
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := s.GetWorkItem(ctx, r.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimWorkItemOwner(ctx, record, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	newReport.Identity = next.Identity
	newReport.WorkerSessionID = owner.ID
	newReport.NativeSessionID = "new-native"
	if _, err := s.AcceptWorkerReport(ctx, newReport); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("current owner reused old-generation lease: %v", err)
	}
	newReport = r
	newReport.ID = "oversized"
	newReport.Content = strings.Repeat("x", 1<<20)
	newReport.Digest = reportDigest(newReport.Content)
	if _, err := s.AcceptWorkerReport(ctx, newReport); !errors.Is(err, domain.ErrInvalidWorkerReport) {
		t.Fatalf("oversized envelope: %v", err)
	}
}
