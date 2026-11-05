package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

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
