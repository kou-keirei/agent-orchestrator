package chat_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func parentControllerReady(st *sqlite.Store) func(chatsvc.StartResult) (chatsvc.ControllerCommit, error) {
	return func(result chatsvc.StartResult) (chatsvc.ControllerCommit, error) {
		metadata := domain.SessionMetadata{ProviderConversationID: result.ProviderConversationID, ControllerGeneration: result.ControllerGeneration}
		manager := lifecycle.New(st, nil)
		conversation := result.Conversation
		var err error
		if result.ProviderBoundary == nil {
			err = manager.MarkSpawned(context.Background(), testSession, metadata)
		} else {
			err = manager.MarkChatSpawned(context.Background(), testSession, metadata, *result.ProviderBoundary)
			conversation.ActiveBranchID = result.ProviderBoundary.ID
		}
		return chatsvc.ControllerCommit{Conversation: conversation}, err
	}
}

func parentIssuerFixture(t *testing.T) (*sqlite.Store, *chatsvc.Service, domain.WorkItemIdentity, ports.WorkItemParentIssuer) {
	t.Helper()
	st := openStore(t)
	worker, err := st.CreateSession(t.Context(), domain.SessionRecord{ProjectID: testProject, Kind: domain.KindWorker,
		Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	i := domain.WorkItemIdentity{ID: "issued-work", AttemptID: "issued-attempt", ProjectID: testProject, Repository: domain.RootWorkspaceRepoName,
		ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: testSession, OriginalWorkerSessionID: worker.ID,
		OwnerSessionID: worker.ID, OwnerGeneration: 1, WorktreePath: t.TempDir(), Branch: "ao/worker/root"}
	for _, id := range []domain.SessionID{testSession, worker.ID} {
		path := i.WorktreePath
		if id == testSession {
			path = t.TempDir()
		}
		if err := st.UpsertSessionWorktree(t.Context(), domain.SessionWorktreeRecord{SessionID: id, RepoName: i.Repository,
			WorktreePath: path, Branch: i.Branch, BaseSHA: i.ExecutionBaseSHA}); err != nil {
			t.Fatal(err)
		}
	}
	nextID := 0
	svc := chatsvc.New(chatsvc.Options{Store: st, Sessions: st, Reader: fullSnapshotReader(st),
		Drivers: fakeRegistry{driver: fakeDriver{start: func(ports.ChatStartConfig) (ports.ChatConversation, error) { return newFakeConversation(), nil }}}, NewID: func() string { nextID++; return fmt.Sprintf("parent-%d", nextID) }})
	t.Cleanup(func() { _ = svc.Stop(context.Background(), testSession) })
	if _, err := svc.Start(t.Context(), chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject,
		Kind: domain.KindOrchestrator, Harness: domain.HarnessCodex, WorkspacePath: t.TempDir(), ControllerReady: parentControllerReady(st)}); err != nil {
		t.Fatal(err)
	}
	issuer, err := svc.OriginalParentIssuer(testSession)
	if err != nil {
		t.Fatal(err)
	}
	return st, svc, i, issuer
}

func TestOriginalParentIssuerPositiveFromOwnedController(t *testing.T) {
	st, svc, i, issuer := parentIssuerFixture(t)
	c, err := svc.Controller(testSession)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		target, err := issuer.IssueWorkItemFromOriginalParent(t.Context(), i, "worker-native-distinct")
		if err != nil {
			rec, _, _ := st.GetSession(t.Context(), testSession)
			conv, _ := st.ConversationForSession(t.Context(), testSession)
			branch, _ := st.ConversationBranch(t.Context(), conv.ID, conv.ActiveBranchID)
			t.Fatalf("%v; durable provider=%q generation=%q branch=%+v live provider=%q generation=%q", err, rec.Metadata.ProviderConversationID, rec.Metadata.ControllerGeneration, branch, c.ProviderConversationID(), c.Generation())
		}
		if target.NativeSessionID != c.ProviderConversationID() || target.ControllerGeneration != c.Generation() ||
			target.IssuedBySessionID != testSession || target.Provenance != domain.WorkItemParentLiveProvenance {
			t.Fatalf("wrong controller observation: %+v", target)
		}
		got, err := st.GetWorkItemOriginalParent(t.Context(), i.ID, i.AttemptID)
		if err != nil || got != target {
			t.Fatalf("durable readback: %+v %v", got, err)
		}
	}
	if events, err := st.ListOrchestrationEvents(t.Context(), testProject, 10); err != nil || len(events) != 0 {
		t.Fatalf("issuance sent/woke/ACKed: %+v %v", events, err)
	}
}

func TestOriginalParentIssuerRefusesForeignStaleHandoffAndStopped(t *testing.T) {
	for _, kind := range []string{"foreign-parent", "stale-durable-generation", "stopped", "handoff", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			st, svc, i, issuer := parentIssuerFixture(t)
			ctx := t.Context()
			c, err := svc.Controller(testSession)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "foreign-parent":
				i.OriginalParentSessionID = i.OriginalWorkerSessionID
			case "stale-durable-generation":
				if err := st.ClaimChatControllerGeneration(ctx, testSession, "replacement-generation"); err != nil {
					t.Fatal(err)
				}
			case "stopped":
				if err := svc.Stop(ctx, testSession); err != nil {
					t.Fatal(err)
				}
			case "handoff":
				if err := c.ArmHandoff(ctx, domain.SessionInterfaceTransitionDrain); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			target, err := issuer.IssueWorkItemFromOriginalParent(ctx, i, "worker-native")
			if target != (domain.WorkItemParentTarget{}) || err == nil {
				t.Fatalf("accepted %s: %+v %v", kind, target, err)
			}
			if kind == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, domain.ErrWorkItemParentHeld) {
				t.Fatal(err)
			}
			if _, found, err := st.GetWorkItem(t.Context(), i.ID); err != nil || found {
				t.Fatalf("refusal created work: %v %v", found, err)
			}
		})
	}
}

func TestOriginalParentIssuerCapturedBeforeReplacementNeverFollowsIt(t *testing.T) {
	st, svc, i, issuer := parentIssuerFixture(t)
	if err := svc.Stop(t.Context(), testSession); err != nil {
		t.Fatal(err)
	}
	// A genuinely different controller instance replaces the stopped controller
	// in the same registry; the old capability must not follow the new pointer.
	if _, err := svc.Start(t.Context(), chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject,
		Kind: domain.KindOrchestrator, Harness: domain.HarnessCodex, WorkspacePath: t.TempDir(), ControllerReady: parentControllerReady(st)}); err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.IssueWorkItemFromOriginalParent(t.Context(), i, "worker-native"); !errors.Is(err, domain.ErrWorkItemParentHeld) {
		t.Fatalf("old capability followed replacement: %v", err)
	}
	if _, found, err := st.GetWorkItem(t.Context(), i.ID); err != nil || found {
		t.Fatalf("old issuer created work: %v %v", found, err)
	}
	fresh, err := svc.OriginalParentIssuer(testSession)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.IssueWorkItemFromOriginalParent(t.Context(), i, "worker-native"); err != nil {
		t.Fatalf("new authorization capability cannot issue: %v", err)
	}
}
