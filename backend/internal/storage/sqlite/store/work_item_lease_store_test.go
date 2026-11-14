package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// The accepted fixture freezes every identity field, including the original
// execution base; changing candidate HEAD never rewrites this record.
func workLeaseRecord() store.WorkItemRecord {
	r := workItemRecord()
	r.Identity.ExecutionBaseSHA = "b9bacf232607980432d4afd76dd29f39bc503940"
	return r
}

func leaseGrant(id string) domain.WorkLeaseGrant {
	return domain.WorkLeaseGrant{ID: id, Duration: time.Hour, AllowedEffects: []string{"source.edit", "local.commit"}}
}

func TestWorkLeaseLifecycleRestartAndEffects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := sqlitetest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r := workLeaseRecord()
	if _, err := s.CreateWorkItem(ctx, r); err != nil {
		t.Fatal(err)
	}
	l, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-1"))
	if err != nil || l.Revision != 1 {
		t.Fatalf("acquire: %+v %v", l, err)
	}
	if got, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-1")); err != nil || !reflect.DeepEqual(got, l) {
		t.Fatalf("acquire retry: %+v %v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, found, err := s.GetWorkItemLease(ctx, r.Identity.ID); err != nil || !found || !reflect.DeepEqual(got, l) {
		t.Fatalf("restart: %+v %v %v", got, found, err)
	}
	candidate := domain.WorkItemCandidate{Identity: r.Identity, HeadSHA: strings.Repeat("b", 40)}
	if err := s.CheckWorkItemEffect(ctx, candidate, l, "source.edit"); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckWorkItemEffect(ctx, candidate, l, "remote.push"); !errors.Is(err, domain.ErrWorkEffectDenied) {
		t.Fatalf("unlisted effect: %v", err)
	}
	for _, mutate := range []func(*domain.WorkItemIdentity){
		func(i *domain.WorkItemIdentity) { i.ID = "missing" },
		func(i *domain.WorkItemIdentity) { i.AttemptID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.ProjectID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.Repository = "foreign" },
		func(i *domain.WorkItemIdentity) { i.ExecutionBaseSHA = candidate.HeadSHA },
		func(i *domain.WorkItemIdentity) { i.OriginalParentSessionID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.OriginalWorkerSessionID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.OwnerSessionID = "foreign" },
		func(i *domain.WorkItemIdentity) { i.OwnerGeneration++ },
		func(i *domain.WorkItemIdentity) { i.Branch += "-foreign" },
		func(i *domain.WorkItemIdentity) { i.WorktreePath += "-foreign" },
	} {
		wrong := candidate
		mutate(&wrong.Identity)
		if err := s.CheckWorkItemEffect(ctx, wrong, l, "source.edit"); err == nil {
			t.Fatalf("foreign effect accepted: %+v", wrong)
		}
		if _, err := s.AcquireWorkItemLease(ctx, wrong.Identity, leaseGrant("foreign")); err == nil {
			t.Fatal("foreign acquire accepted")
		}
		if _, err := s.CompleteWorkItemLease(ctx, wrong.Identity, l); err == nil {
			t.Fatal("foreign completion accepted")
		}
	}
	forged := l
	forged.Grant = leaseGrant("lease-1")
	forged.Grant.AllowedEffects = append(forged.Grant.AllowedEffects, "remote.push")
	if err := s.CheckWorkItemEffect(ctx, candidate, forged, "remote.push"); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("forged allowlist: %v", err)
	}
	badHead := candidate
	badHead.HeadSHA = "unknown"
	if err := s.CheckWorkItemEffect(ctx, badHead, l, "source.edit"); err == nil {
		t.Fatal("malformed HEAD accepted")
	}
	renewed, err := s.RenewWorkItemLease(ctx, r.Identity, l, l.ExpiresAt.Add(time.Hour))
	if err != nil || renewed.Revision != 2 {
		t.Fatalf("renew: %+v %v", renewed, err)
	}
	if got, err := s.RenewWorkItemLease(ctx, r.Identity, l, renewed.ExpiresAt); err != nil || !reflect.DeepEqual(got, renewed) {
		t.Fatalf("renew retry: %+v %v", got, err)
	}
	if err := s.CheckWorkItemEffect(ctx, candidate, l, "source.edit"); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("old revision: %v", err)
	}
	released, err := s.ReleaseWorkItemLease(ctx, r.Identity, renewed)
	if err != nil || released.State != domain.WorkLeaseReleased {
		t.Fatalf("release: %+v %v", released, err)
	}
	if got, err := s.ReleaseWorkItemLease(ctx, r.Identity, renewed); err != nil || !reflect.DeepEqual(got, released) {
		t.Fatalf("release retry: %+v %v", got, err)
	}
	if _, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-2")); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("released reacquire: %v", err)
	}
	next, err := s.SupersedeWorkItemLease(ctx, r.Identity, released, "worker-2", leaseGrant("lease-2"))
	if err != nil || next.OwnerGeneration != 2 || next.Revision != 4 {
		t.Fatalf("supersede: %+v %v", next, err)
	}
	if err := s.CheckWorkItemEffect(ctx, candidate, released, "source.edit"); !errors.Is(err, domain.ErrStaleWorkOwner) {
		t.Fatalf("stale owner: %v", err)
	}
	r.Identity.OwnerSessionID, r.Identity.OwnerGeneration = "worker-2", 2
	requireWorkItem(t, s, r)
	candidate.Identity = r.Identity
	if err := s.CheckWorkItemEffect(ctx, candidate, next, "local.commit"); err != nil {
		t.Fatal(err)
	}
	completed, err := s.CompleteWorkItemLease(ctx, r.Identity, next)
	if err != nil || completed.State != domain.WorkLeaseCompleted {
		t.Fatalf("complete: %+v %v", completed, err)
	}
	if got, err := s.CompleteWorkItemLease(ctx, r.Identity, next); err != nil || !reflect.DeepEqual(got, completed) {
		t.Fatalf("completion retry: %+v %v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.CompleteWorkItemLease(ctx, r.Identity, next); err != nil || !reflect.DeepEqual(got, completed) {
		t.Fatalf("completion retry after restart: %+v %v", got, err)
	}
	if _, err := s.SupersedeWorkItemLease(ctx, r.Identity, completed, "worker-3", leaseGrant("lease-3")); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("completed supersede: %v", err)
	}
	if err := s.CheckWorkItemEffect(ctx, candidate, completed, "source.edit"); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("completed effect: %v", err)
	}
	requireWorkItem(t, s, r)
}

func TestWorkLeaseExpiryAndExternalOwnerFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := sqlitetest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := workLeaseRecord()
	if _, err := s.CreateWorkItem(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-1")); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Deterministic elapsed deadline, without sleeping or probing a real process.
	if _, err := db.ExecContext(ctx, `UPDATE work_item_leases SET expires_at_ms=1`); err != nil {
		t.Fatal(err)
	}
	expired, _, err := s.GetWorkItemLease(ctx, r.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	candidate := domain.WorkItemCandidate{Identity: r.Identity, HeadSHA: strings.Repeat("b", 40)}
	if err := s.CheckWorkItemEffect(ctx, candidate, expired, "source.edit"); !errors.Is(err, domain.ErrWorkLeaseExpired) {
		t.Fatalf("expired effect: %v", err)
	}
	if _, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-2")); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("expiry reclaimed: %v", err)
	}
	if _, err := s.RenewWorkItemLease(ctx, r.Identity, expired, time.UnixMilli(time.Now().Add(time.Hour).UnixMilli())); !errors.Is(err, domain.ErrWorkLeaseExpired) {
		t.Fatalf("expired renew: %v", err)
	}
	if _, err := s.CompleteWorkItemLease(ctx, r.Identity, expired); !errors.Is(err, domain.ErrWorkLeaseExpired) {
		t.Fatalf("expired completion: %v", err)
	}
	requireWorkItem(t, s, r) // no owner, native-history or worktree replacement
	next, err := s.SupersedeWorkItemLease(ctx, r.Identity, expired, "worker-2", leaseGrant("lease-2"))
	if err != nil {
		t.Fatal(err)
	}
	r.Identity.OwnerSessionID, r.Identity.OwnerGeneration = "worker-2", 2
	claimed, err := s.ClaimWorkItemOwner(ctx, r, "worker-3")
	if err != nil {
		t.Fatal(err)
	}
	candidate.Identity = claimed.Identity
	if err := s.CheckWorkItemEffect(ctx, candidate, next, "source.edit"); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("externally advanced owner reused lease: %v", err)
	}
	if _, err := s.CompleteWorkItemLease(ctx, claimed.Identity, next); !errors.Is(err, domain.ErrWorkLeaseConflict) {
		t.Fatalf("external owner completed old lease: %v", err)
	}
}

func TestWorkLeaseConcurrentIndependentOpens(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	var stores []*sqlite.Store
	for range 2 {
		s, err := sqlite.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		stores = append(stores, s)
	}
	r := workLeaseRecord()
	if _, err := stores[0].CreateWorkItem(ctx, r); err != nil {
		t.Fatal(err)
	}
	run := func(fn func(int, *sqlite.Store) error) {
		t.Helper()
		const n = 12
		start := make(chan struct{})
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; errs[i] = fn(i, stores[i%2]) }()
		}
		close(start)
		wg.Wait()
		wins := 0
		for _, err := range errs {
			if err == nil {
				wins++
			} else if !errors.Is(err, domain.ErrWorkLeaseConflict) && !errors.Is(err, domain.ErrStaleWorkOwner) {
				t.Fatalf("unexpected contender error: %v", err)
			}
		}
		if wins != 1 {
			t.Fatalf("%d winners, want 1", wins)
		}
	}
	run(func(i int, s *sqlite.Store) error {
		_, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant(fmt.Sprintf("lease-%d", i)))
		return err
	})
	l, _, err := stores[0].GetWorkItemLease(ctx, r.Identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	run(func(i int, s *sqlite.Store) error {
		_, err := s.RenewWorkItemLease(ctx, r.Identity, l, l.ExpiresAt.Add(time.Duration(i+1)*time.Minute))
		return err
	})
	l, _, err = stores[0].GetWorkItemLease(ctx, r.Identity.ID)
	if err != nil || l.Revision != 2 {
		t.Fatalf("renew readback: %+v %v", l, err)
	}
	run(func(i int, s *sqlite.Store) error {
		_, err := s.SupersedeWorkItemLease(ctx, r.Identity, l, domain.SessionID(fmt.Sprintf("owner-%d", i)), leaseGrant(fmt.Sprintf("next-%d", i)))
		return err
	})
	current, _, err := stores[0].GetWorkItem(ctx, r.Identity.ID)
	if err != nil || current.Identity.OwnerGeneration != 2 {
		t.Fatalf("owner readback: %+v %v", current, err)
	}
	l, _, err = stores[0].GetWorkItemLease(ctx, r.Identity.ID)
	if err != nil || l.Revision != 3 || l.OwnerGeneration != 2 {
		t.Fatalf("lease readback: %+v %v", l, err)
	}
	// Release and completion are incompatible terminal transitions. Exactly
	// one may win, even when separate Store opens submit the same revision.
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := stores[0].CompleteWorkItemLease(ctx, current.Identity, l)
		errs <- err
	}()
	go func() {
		<-start
		_, err := stores[1].ReleaseWorkItemLease(ctx, current.Identity, l)
		errs <- err
	}()
	close(start)
	winners := 0
	for range 2 {
		if err := <-errs; err == nil {
			winners++
		} else if !errors.Is(err, domain.ErrWorkLeaseConflict) {
			t.Fatalf("terminal contender: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("terminal winners: %d", winners)
	}
}

func TestWorkLeaseRollbackCancellationAndValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := sqlitetest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := workLeaseRecord()
	if _, err := s.CreateWorkItem(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, g := range []domain.WorkLeaseGrant{
		{}, {ID: "bad\n", Duration: time.Hour}, {ID: "a", Duration: -1}, {ID: "a", Duration: time.Nanosecond},
		{ID: "a", Duration: time.Hour, AllowedEffects: []string{""}}, {ID: "a", Duration: time.Hour, AllowedEffects: []string{"x", "x"}},
	} {
		if _, err := s.AcquireWorkItemLease(ctx, r.Identity, g); !errors.Is(err, domain.ErrInvalidWorkLease) {
			t.Fatalf("invalid grant: %v", err)
		}
	}
	l, err := s.AcquireWorkItemLease(ctx, r.Identity, leaseGrant("lease-1"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_lease AFTER UPDATE ON work_item_leases BEGIN SELECT RAISE(ABORT, 'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupersedeWorkItemLease(ctx, r.Identity, l, "worker-2", leaseGrant("lease-2")); err == nil {
		t.Fatal("injected failure accepted")
	}
	requireWorkItem(t, s, r) // owner update rolled back with the lease update
	if got, _, err := s.GetWorkItemLease(ctx, r.Identity.ID); err != nil || !reflect.DeepEqual(got, l) {
		t.Fatalf("rollback: %+v %v", got, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_lease`); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := s.CompleteWorkItemLease(canceled, r.Identity, l); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled completion: %v", err)
	}
	if _, err := s.SupersedeWorkItemLease(ctx, r.Identity, l, "worker-2", leaseGrant("lease-2")); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}
