package daemon

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	sessionmanager "github.com/aoagents/agent-orchestrator/backend/internal/session_manager"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestNativeWorkItemCallbackUnavailableOwnerNeverLaunchesOrBinds(t *testing.T) {
	st, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := chatsvc.New(chatsvc.Options{Store: st, Sessions: st})
	manager := sessionmanager.New(sessionmanager.Deps{Store: st})
	spec := sessionmanager.NativeWorkItemSpec{Parent: "missing-parent"}
	for _, deps := range []struct {
		manager *sessionmanager.Manager
		chat    *chatsvc.Service
		store   *sqlite.Store
	}{
		{nil, svc, st}, {manager, nil, st}, {manager, svc, nil}, {manager, svc, st},
	} {
		if command, err := prepareNativeWorkItem(deps.manager, deps.chat, deps.store, spec); err == nil || command != nil {
			t.Fatalf("unbound producer: %+v %v", command, err)
		}
	}
	for _, command := range []*nativeWorkItemCommand{nil, {}} {
		if rec, err := command.Spawn(t.Context()); !errors.Is(err, domain.ErrWorkAdmissionHeld) || rec.ID != "" {
			t.Fatalf("zero birth: %+v %v", rec, err)
		}
		if err := command.ConnectReporter(t.Context()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
			t.Fatalf("zero reporter: %v", err)
		}
	}
	sessions, err := st.ListAllSessions(t.Context())
	if err != nil || len(sessions) != 0 {
		t.Fatalf("unbound owner allocated sessions: %+v %v", sessions, err)
	}
}

func TestPrepareWorkerReportToolRetainsExactAdmittedBinding(t *testing.T) {
	ctx, dir := t.Context(), t.TempDir()
	st, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "report-project", Path: dir, RegisteredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	create := func(kind domain.SessionKind) domain.SessionRecord {
		r, err := st.CreateSession(ctx, domain.SessionRecord{ProjectID: "report-project", Kind: kind, Mode: domain.SessionModeChat,
			Harness: domain.HarnessCodex, CreatedAt: time.Now(), UpdatedAt: time.Now(),
			Metadata: domain.SessionMetadata{ProviderConversationID: "parent-native", ControllerGeneration: "parent-generation"}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	parent, worker := create(domain.KindOrchestrator), create(domain.KindWorker)
	i := domain.WorkItemIdentity{ID: "report-work", AttemptID: "report-attempt", ProjectID: worker.ProjectID, Repository: "repo",
		ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: parent.ID, OriginalWorkerSessionID: worker.ID,
		OwnerSessionID: worker.ID, OwnerGeneration: 1, WorktreePath: filepath.Join(dir, "worker"), Branch: "report-branch"}
	for _, rec := range []domain.SessionRecord{parent, worker} {
		path := i.WorktreePath
		if rec.ID == parent.ID {
			path = filepath.Join(dir, "parent")
		}
		if err := st.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: rec.ID, RepoName: i.Repository,
			WorktreePath: path, Branch: i.Branch, BaseSHA: i.ExecutionBaseSHA}); err != nil {
			t.Fatal(err)
		}
	}
	conversation, err := st.CreateConversation(ctx, "parent-report-conversation", domain.ConversationScopeSession, parent.ProjectID, parent.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	branch, err := st.ConversationBranch(ctx, conversation.ID, conversation.ActiveBranchID)
	if err != nil {
		t.Fatal(err)
	}
	target := domain.WorkItemParentTarget{WorkItemID: i.ID, AttemptID: i.AttemptID, OriginalParentSessionID: parent.ID,
		NativeSessionID: "parent-native", ProviderConversationID: "parent-native", ControllerGeneration: "parent-generation",
		ProviderScopeID: branch.ProviderScopeID, IssuedBySessionID: parent.ID, IssuedByControllerGeneration: "parent-generation", Provenance: domain.WorkItemParentLiveProvenance}
	receipt, err := st.PersistWorkItemSpawn(ctx, i, "", target, domain.WorkLeaseGrant{ID: "report-lease", Duration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := prepareWorkerReportTool(ctx, st, i.ID, worker.ID)
	if err != nil || admission.Binding.Identity != i || admission.Binding.Lease.Grant.ID != receipt.Lease.Grant.ID ||
		admission.Binding.Lease.Revision != receipt.Lease.Revision || admission.Bindings == nil || admission.Acceptance == nil {
		t.Fatalf("admission: %+v %v", admission, err)
	}
	for _, claim := range []struct {
		work   domain.WorkItemID
		worker domain.SessionID
	}{
		{"", worker.ID}, {"missing", worker.ID}, {i.ID, parent.ID}, {i.ID, "foreign-worker"},
	} {
		if got, err := prepareWorkerReportTool(ctx, st, claim.work, claim.worker); err == nil || got.Acceptance != nil {
			t.Fatalf("foreign admission: %+v %v", got, err)
		}
	}
	renewed, err := st.RenewWorkItemLease(ctx, i, receipt.Lease, receipt.Lease.ExpiresAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := prepareWorkerReportTool(ctx, st, i.ID, worker.ID); !errors.Is(err, domain.ErrWorkAdmissionHeld) || got.Acceptance != nil {
		t.Fatalf("new lease inherited original launch admission: %+v %v", got, err)
	}
	if _, err := st.ReleaseWorkItemLease(ctx, i, renewed); err != nil {
		t.Fatal(err)
	}
	if got, err := prepareWorkerReportTool(ctx, st, i.ID, worker.ID); err == nil || got.Acceptance != nil {
		t.Fatalf("released lease admitted: %+v %v", got, err)
	}
	// Captured authority is never silently replaced by the readback of a later lease.
	if admission.Binding.Lease.State != domain.WorkLeaseActive || admission.Binding.Lease.Revision != receipt.Lease.Revision {
		t.Fatal("captured lease changed")
	}
	if events, err := st.ListOrchestrationEvents(ctx, worker.ProjectID, 10); err != nil || len(events) != 0 {
		t.Fatalf("admission submitted report: %+v %v", events, err)
	}
	if got, err := prepareWorkerReportTool(ctx, nil, i.ID, worker.ID); err == nil || got.Acceptance != nil {
		t.Fatalf("nil store admitted: %+v %v", got, err)
	}
}
