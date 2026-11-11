package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// Fixture for possession of a pre-issued guard, not a native callback or live
// provider proof. Actual Chat controller/turn guards have their own tests.
type ingressActorFixture struct {
	claim   domain.WorkerReport
	reader  workerReportBindings
	retired bool
}

func (a *ingressActorFixture) VerifyWorkerReportActor(ctx context.Context, r domain.WorkerReport) error {
	release, err := a.AcquireWorkerReportActor(ctx, r)
	if err == nil {
		release()
	}
	return err
}

func (a *ingressActorFixture) AcquireWorkerReportActor(ctx context.Context, r domain.WorkerReport) (func(), error) {
	if a.retired || r.Identity != a.claim.Identity || r.WorkerSessionID != a.claim.WorkerSessionID ||
		r.NativeSessionID != a.claim.NativeSessionID || r.NativeTurnID != a.claim.NativeTurnID ||
		r.LeaseID != a.claim.LeaseID || r.LeaseRevision != a.claim.LeaseRevision {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	b, found, err := a.reader.ReadWorkerReportBinding(ctx, r.Identity.ID)
	if err != nil {
		return nil, err
	}
	if !found || b.Identity != r.Identity {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	if b.Lease.Revision != r.LeaseRevision || b.Lease.State != domain.WorkLeaseActive {
		return nil, domain.ErrWorkLeaseConflict
	}
	return func() {}, nil
}

func TestNativeWorkerReportPrivateBoundaryAndRestart(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	st, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	now := time.Now()
	if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: dir, RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	w, err := st.CreateSession(ctx, domain.SessionRecord{ProjectID: "project", Kind: domain.KindWorker, Harness: domain.HarnessFake, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	i := domain.WorkItemIdentity{ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo",
		ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent", OriginalWorkerSessionID: w.ID,
		OwnerSessionID: w.ID, OwnerGeneration: 1, WorktreePath: dir, Branch: "branch"}
	item, err := st.CreateWorkItem(ctx, store.WorkItemRecord{Identity: i, OriginalNativeSessionID: "native"})
	if err != nil {
		t.Fatal(err)
	}
	l, err := st.AcquireWorkItemLease(ctx, i, domain.WorkLeaseGrant{ID: "lease", Duration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("checkpoint"))
	r := domain.WorkerReport{ID: "report", Type: "checkpoint", Version: 1, Identity: i,
		LeaseID: l.Grant.ID, LeaseRevision: l.Revision, WorkerSessionID: w.ID,
		NativeSessionID: "native", NativeTurnID: "turn", OriginalNativeSessionID: "native",
		Content: "checkpoint", Digest: hex.EncodeToString(sum[:])}
	// A missing handle must return a typed hold even without a usable store.
	if got, err := submitNativeWorkerReport(ctx, nil, nil, r); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got != (domain.WorkerReportReceipt{}) {
		t.Fatalf("missing handle: %+v %v", got, err)
	}
	a := &ingressActorFixture{claim: r, reader: workerReportBindings{st}}
	for _, mutate := range []func(*domain.WorkerReport){
		func(r *domain.WorkerReport) { r.WorkerSessionID = "foreign" },
		func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ },
		func(r *domain.WorkerReport) { r.NativeTurnID = "foreign" },
	} {
		bad := r
		mutate(&bad)
		if got, err := submitNativeWorkerReport(ctx, a, st, bad); !errors.Is(err, domain.ErrWorkerReportActorMismatch) || got != (domain.WorkerReportReceipt{}) {
			t.Fatalf("foreign actor: %+v %v", got, err)
		}
	}
	if events, err := st.ListOrchestrationEvents(ctx, "project", 10); err != nil || len(events) != 0 {
		t.Fatalf("refusal effects: %+v %v", events, err)
	}
	receipt, err := submitNativeWorkerReport(ctx, a, st, r)
	if err != nil || receipt.Report != r || receipt.EventID == "" {
		t.Fatalf("acceptance: %+v %v", receipt, err)
	}
	// Discard the response; exact retry must return the original receipt.
	if got, err := submitNativeWorkerReport(ctx, a, st, r); err != nil || got != receipt {
		t.Fatalf("lost response: %+v %v", got, err)
	}
	conflict := r
	conflict.Type = "done"
	if _, err := submitNativeWorkerReport(ctx, a, st, conflict); !errors.Is(err, domain.ErrWorkerReportConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	a.reader = workerReportBindings{st}
	a.retired = true // A reboot does not restore possession of a native actor.
	if _, err := submitNativeWorkerReport(ctx, a, st, r); !errors.Is(err, domain.ErrWorkerReportActorMismatch) {
		t.Fatalf("retired retry: %v", err)
	}
	if got, found, err := st.GetWorkerReport(ctx, r.ID); err != nil || !found || got != receipt {
		t.Fatalf("reopened receipt: %+v %v %v", got, found, err)
	}
	if events, err := st.ListOrchestrationEvents(ctx, "project", 10); err != nil || len(events) != 1 || events[0].ID != receipt.EventID || events[0].State != domain.OrchestrationPending || !events[0].AcknowledgedAt.IsZero() {
		t.Fatalf("original outbox / semantic ACK hold: %+v %v", events, err)
	}
	a.retired = false
	if _, err := st.ClaimWorkItemOwner(ctx, item, "replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err := submitNativeWorkerReport(ctx, a, st, r); !errors.Is(err, domain.ErrWorkerReportActorMismatch) {
		t.Fatalf("stale owner receipt lookup: %v", err)
	}
}

func TestWorkerReportBindingsReadAuthoritativeOwnerAndLease(t *testing.T) {
	st, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	reader := workerReportBindings{store: st}
	ctx := t.Context()
	if _, found, err := reader.ReadWorkerReportBinding(ctx, "missing"); err != nil || found {
		t.Fatalf("missing: %v %v", found, err)
	}
	identity := domain.WorkItemIdentity{ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo",
		ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent", OriginalWorkerSessionID: "worker",
		OwnerSessionID: "worker", OwnerGeneration: 1, WorktreePath: "/disposable/work", Branch: "branch"}
	item, err := st.CreateWorkItem(ctx, store.WorkItemRecord{Identity: identity, OriginalNativeSessionID: "native"})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.ReadWorkerReportBinding(ctx, identity.ID); err != nil || found {
		t.Fatalf("missing lease: %v %v", found, err)
	}
	lease, err := st.AcquireWorkItemLease(ctx, identity, domain.WorkLeaseGrant{ID: "lease", Duration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	binding, found, err := reader.ReadWorkerReportBinding(ctx, identity.ID)
	if err != nil || !found || binding.Identity != identity || binding.OriginalNativeSessionID != "native" || binding.Lease.Revision != lease.Revision {
		t.Fatalf("binding: %+v %v %v", binding, found, err)
	}
	owner, err := st.ClaimWorkItemOwner(ctx, item, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	binding, found, err = reader.ReadWorkerReportBinding(ctx, identity.ID)
	if err != nil || !found || binding.Identity != owner.Identity || binding.Identity.OwnerGeneration == lease.OwnerGeneration {
		t.Fatalf("owner change hidden by retained lease: %+v %v %v", binding, found, err)
	}
}
