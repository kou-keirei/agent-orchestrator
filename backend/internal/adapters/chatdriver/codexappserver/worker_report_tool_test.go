package codexappserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type reportToolRegistry struct{ driver ports.ChatDriver }

func (r reportToolRegistry) Driver(domain.AgentHarness) (ports.ChatDriver, error) {
	return r.driver, nil
}
func (reportToolRegistry) SupportsChat(domain.AgentHarness) bool { return true }

type reportToolDriver struct {
	*Driver
	parent   *Driver
	parentID domain.SessionID
}

func (d reportToolDriver) Probe(context.Context) (ports.ChatCapabilities, error) {
	return capabilities(), nil
}
func (d reportToolDriver) ProbeForWorkspace(context.Context, string) (ports.ChatCapabilities, error) {
	return capabilities(), nil
}
func (d reportToolDriver) Start(ctx context.Context, cfg ports.ChatStartConfig) (ports.ChatConversation, error) {
	if cfg.SessionID == d.parentID {
		return d.parent.Start(ctx, cfg)
	}
	return d.Driver.Start(ctx, cfg)
}

type reportToolBindings struct{ store *sqlite.Store }

func (b reportToolBindings) ReadWorkerReportBinding(ctx context.Context, id domain.WorkItemID) (domain.WorkerReportActorBinding, bool, error) {
	i, found, err := b.store.GetWorkItem(ctx, id)
	if err != nil || !found {
		return domain.WorkerReportActorBinding{}, false, err
	}
	l, found, err := b.store.GetWorkItemLease(ctx, id)
	return domain.WorkerReportActorBinding{Identity: i.Identity, OriginalNativeSessionID: i.OriginalNativeSessionID, Lease: l}, found, err
}

// This drives the actual conn read loop, server-request handler, native turn
// projector, guarded bridge and SQLite acceptance/outbox. Only provider I/O and
// trusted launch admission are synthetic; no real provider or semantic ACK runs.
func TestWorkerReportToolNativeDispatchBridgeAndFences(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	st, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "tool-project", Path: dir, RegisteredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	create := func(kind domain.SessionKind) domain.SessionRecord {
		r, err := st.CreateSession(ctx, domain.SessionRecord{ProjectID: "tool-project", Kind: kind, Harness: domain.HarnessCodex,
			Mode: domain.SessionModeChat, CreatedAt: time.Now(), UpdatedAt: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	parent, worker := create(domain.KindOrchestrator), create(domain.KindWorker)
	d, server := newTestDriver(t)
	pd, parentServer := newTestDriver(t)
	parentServer.reply("thread/start", `{"thread":{"id":"original-parent-native"}}`)
	bindings := reportToolBindings{st}
	var seq atomic.Int64
	svc := chatsvc.New(chatsvc.Options{Store: st, Sessions: st, NewID: func() string { return fmt.Sprintf("tool-id-%d", seq.Add(1)) },
		Drivers: reportToolRegistry{reportToolDriver{Driver: d, parent: pd, parentID: parent.ID}},
		PrepareWorkerReportTool: func(ctx context.Context, id domain.WorkItemID, session domain.SessionID) (ports.WorkerReportToolAdmission, error) {
			b, found, err := bindings.ReadWorkerReportBinding(ctx, id)
			if err != nil {
				return ports.WorkerReportToolAdmission{}, err
			}
			if !found || b.Identity.OwnerSessionID != session {
				return ports.WorkerReportToolAdmission{}, domain.ErrWorkerReportActorMismatch
			}
			return ports.WorkerReportToolAdmission{Binding: b, Bindings: bindings, Acceptance: func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
				return reportactor.NewAcceptance(actor, st)
			}}, nil
		},
	})
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		svc.StopAll(stop)
	})
	base := strings.Repeat("a", 40)
	start := func(rec domain.SessionRecord, work domain.WorkItemID) {
		path := filepath.Join(dir, string(rec.ID))
		if err := st.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: rec.ID, RepoName: "repo", WorktreePath: path, Branch: "tool-branch", BaseSHA: base}); err != nil {
			t.Fatal(err)
		}
		_, err := svc.StartChat(ctx, chatsvc.StartConfig{SessionID: rec.ID, ProjectID: rec.ProjectID, Kind: rec.Kind, Harness: rec.Harness,
			WorkspacePath: dir, WorkerReportWorkItem: work, Permissions: ports.PermissionModeDefault,
			ControllerReady: func(start ports.ChatControllerStarted) (ports.ChatControllerCommit, error) {
				rec.Metadata.ProviderConversationID = start.ProviderConversationID
				rec.Metadata.ControllerGeneration = start.ControllerGeneration
				return ports.ChatControllerCommit{}, st.UpdateSession(ctx, rec)
			}})
		if err != nil {
			t.Fatal(err)
		}
	}
	start(parent, "")
	identity := domain.WorkItemIdentity{ID: "tool-work", AttemptID: "tool-attempt", ProjectID: worker.ProjectID, Repository: "repo", ExecutionBaseSHA: base,
		OriginalParentSessionID: parent.ID, OriginalWorkerSessionID: worker.ID, OwnerSessionID: worker.ID, OwnerGeneration: 1,
		WorktreePath: filepath.Join(dir, string(worker.ID)), Branch: "tool-branch"}
	if err := st.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: worker.ID, RepoName: "repo", WorktreePath: identity.WorktreePath, Branch: identity.Branch, BaseSHA: base}); err != nil {
		t.Fatal(err)
	}
	issuer, err := svc.OriginalParentIssuer(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.(ports.WorkItemSpawnIssuer).IssueWorkItemSpawn(ctx, identity, "", domain.WorkLeaseGrant{ID: "tool-lease", Duration: time.Minute}); err != nil {
		t.Fatal(err)
	}
	start(worker, identity.ID)
	opened := server.awaitFrame(func(f frame) bool { return f.Method == "thread/start" })
	var params struct {
		DynamicTools []struct {
			Name        string         `json:"name"`
			InputSchema map[string]any `json:"inputSchema"`
		} `json:"dynamicTools"`
	}
	if err := json.Unmarshal(opened.Params, &params); err != nil || len(params.DynamicTools) != 1 || params.DynamicTools[0].Name != workerReportToolName || params.DynamicTools[0].InputSchema["additionalProperties"] != false {
		t.Fatalf("tool registration: %s %v", opened.Params, err)
	}
	parentOpen := parentServer.awaitFrame(func(f frame) bool { return f.Method == "thread/start" })
	if strings.Contains(string(parentOpen.Params), "dynamicTools") {
		t.Fatal("parent received worker report tool")
	}
	content := "explicit native authored checkpoint"
	digest := sha256.Sum256([]byte(content))
	args := map[string]any{"id": "native-report", "type": "checkpoint", "version": 1, "content": content, "artifactReference": "", "digest": hex.EncodeToString(digest[:])}
	call := func(id int, thread, turn, tool string, arguments any) frame {
		payload, err := json.Marshal(map[string]any{"id": id, "method": "item/tool/call", "params": map[string]any{"threadId": thread, "turnId": turn, "callId": fmt.Sprintf("call-%d", id), "tool": tool, "arguments": arguments}})
		if err != nil {
			t.Fatal(err)
		}
		server.push(string(payload))
		return server.awaitFrame(func(f frame) bool { return f.Method == "" && f.ID != nil && string(*f.ID) == fmt.Sprint(id) })
	}
	server.push(`{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"inProgress","items":[]}}}`)
	first := call(700, "thread-1", "turn-1", workerReportToolName, args)
	if first.Error != nil {
		t.Fatalf("native tool rejected: %+v", first.Error)
	}
	for i := 701; i <= 702; i++ {
		retry := call(i, "thread-1", "turn-1", workerReportToolName, args)
		if retry.Error != nil || string(retry.Result) != string(first.Result) {
			t.Fatalf("unstable tool retry: %s %v", retry.Result, retry.Error)
		}
	}
	receipt, found, err := st.GetWorkerReport(ctx, "native-report")
	if err != nil || !found || receipt.Report.Identity != identity || receipt.Report.NativeSessionID != "thread-1" || receipt.Report.NativeTurnID != "turn-1" {
		t.Fatalf("report identity: %+v %v %v", receipt, found, err)
	}
	target, err := st.GetWorkItemOriginalParent(ctx, identity.ID, identity.AttemptID)
	if err != nil || target.OriginalParentSessionID != parent.ID || target.NativeSessionID != "original-parent-native" {
		t.Fatalf("original parent changed: %+v %v", target, err)
	}
	for _, tc := range []struct {
		name, thread, turn, tool string
		args                     any
	}{
		{"foreign-thread", "foreign", "turn-1", workerReportToolName, args},
		{"foreign-turn", "thread-1", "foreign", workerReportToolName, args},
		{"unknown-tool", "thread-1", "turn-1", "unknown", args},
		{"claimed-authority", "thread-1", "turn-1", workerReportToolName, map[string]any{"id": "spoof", "parentSessionId": parent.ID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if response := call(710+len(tc.name), tc.thread, tc.turn, tc.tool, tc.args); response.Error == nil {
				t.Fatalf("unsafe tool accepted: %s", response.Result)
			}
		})
	}
	// Provider prose and generic activity do not invoke the report capability.
	server.push(`{"method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"prose","delta":"ao_worker_report {parentSessionId: spoof}"}}`)
	if err := st.ClaimChatControllerGeneration(ctx, worker.ID, "replacement-generation"); err != nil {
		t.Fatal(err)
	}
	if response := call(750, "thread-1", "turn-1", workerReportToolName, args); response.Error == nil {
		t.Fatal("old controller returned receipt")
	}
	events, err := st.ListOrchestrationEvents(ctx, worker.ProjectID, 10)
	if err != nil || len(events) != 1 || events[0].State != domain.OrchestrationPending || !events[0].SubmittedAt.IsZero() || !events[0].AcknowledgedAt.IsZero() {
		t.Fatalf("outbox or semantic ACK changed: %+v %v", events, err)
	}
}

func TestWorkerReportToolUnofferedNativeDispatchRefused(t *testing.T) {
	d, server := newTestDriver(t)
	conv, err := d.Start(t.Context(), ports.ChatStartConfig{SessionID: "ordinary-worker", WorkspacePath: t.TempDir(), Permissions: ports.PermissionModeDefault})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conv.Close() }()
	server.push(`{"id":900,"method":"item/tool/call","params":{"threadId":"thread-1","turnId":"turn-1","callId":"foreign-worker","tool":"ao_worker_report","arguments":{}}}`)
	response := server.awaitFrame(func(f frame) bool { return f.ID != nil && string(*f.ID) == "900" })
	if response.Error == nil {
		t.Fatalf("unregistered worker accepted tool: %s", response.Result)
	}
}
