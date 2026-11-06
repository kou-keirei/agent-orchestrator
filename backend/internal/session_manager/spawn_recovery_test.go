package sessionmanager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestReconcileLive_MissingHandleRequiresExactStoppedHandoffOwner(t *testing.T) {
	for _, mutation := range []string{
		"missing handoff", "source stopping", "uncertain source stop", "unconfirmed restore",
		"foreign native", "foreign provider", "newer launch", "newer native launch", "newer Chat generation",
		"opposite handoff", "superseded handoff",
	} {
		t.Run(mutation, func(t *testing.T) {
			m, st, rt, _, _ := newTransitionManager(t, domain.SessionModeTUI)
			rec := st.sessions["session-1"]
			rec.Metadata.RuntimeHandleID = ""
			rec.Metadata.RuntimeLaunchID = ""
			rec.Metadata.AgentSessionIDLaunchID = ""
			rec.Metadata.ProviderConversationID = rec.Metadata.AgentSessionID
			now := time.Now()
			transition := domain.SessionInterfaceTransition{
				ID: "stopped-source", SessionID: rec.ID,
				SourceMode: domain.SessionModeTUI, TargetMode: domain.SessionModeChat,
				Phase: domain.SessionInterfaceTransitionRecovery, ErrorCode: "DAEMON_RESTARTED",
				NativeConversationID: rec.Metadata.AgentSessionID, CreatedAt: now, UpdatedAt: now,
			}
			switch mutation {
			case "source stopping":
				transition.Phase = domain.SessionInterfaceTransitionSourceStopping
			case "uncertain source stop":
				transition.ErrorCode = "SOURCE_STOP_UNCERTAIN"
			case "unconfirmed restore":
				transition.ErrorCode = "SOURCE_RESTORE_UNCONFIRMED"
			case "foreign native":
				rec.Metadata.AgentSessionID = "foreign-native"
				rec.Metadata.ProviderConversationID = "foreign-native"
			case "foreign provider":
				rec.Metadata.ProviderConversationID = "foreign-provider"
			case "newer launch":
				rec.Metadata.RuntimeLaunchID = "newer-launch"
			case "newer native launch":
				rec.Metadata.AgentSessionIDLaunchID = "newer-launch"
			case "newer Chat generation":
				rec.Metadata.ControllerGeneration = "newer-generation"
			case "opposite handoff":
				transition.SourceMode, transition.TargetMode = transition.TargetMode, transition.SourceMode
			case "superseded handoff":
				newer := transition
				newer.ID, newer.CreatedAt = "newer-handoff", now.Add(time.Second)
				newer.Phase = domain.SessionInterfaceTransitionCancelled
				st.transitions[newer.ID] = newer
			}
			if mutation != "missing handoff" {
				st.transitions[transition.ID] = transition
			}
			st.sessions[rec.ID] = rec
			beforeTransitions := make(map[string]domain.SessionInterfaceTransition)
			for id, transition := range st.transitions {
				beforeTransitions[id] = transition
			}
			for pass := 1; pass <= 2; pass++ {
				if err := m.reconcileLive(context.Background(), rec); !errors.Is(err, ErrSpawnRecoveryUncertain) {
					t.Fatalf("pass %d: outcome = %v", pass, err)
				}
				if !reflect.DeepEqual(st.sessions[rec.ID], rec) || !reflect.DeepEqual(st.transitions, beforeTransitions) {
					t.Fatalf("pass %d changed uncertain owner or handoff", pass)
				}
			}
			ws := m.workspace.(*fakeWorkspace)
			if rt.created != 0 || rt.destroyed != 0 || len(ws.restoreConfigs) != 0 || ws.destroyed != 0 {
				t.Fatal("uncertain owner touched runtime or workspace")
			}
		})
	}
}

func TestReconcileLive_PreservesSpawnWithoutRuntimeAcknowledgement(t *testing.T) {
	for _, kind := range []domain.SessionKind{domain.KindWorker, domain.KindOrchestrator} {
		for _, phase := range []string{"workspace", "launch", "provider birth"} {
			t.Run(string(kind)+"/"+phase, func(t *testing.T) {
				m, st, rt, ws := newManager()
				rec := domain.SessionRecord{
					ID: "mer-1", ProjectID: "mer", Kind: kind,
					Mode: domain.SessionModeTUI, Harness: domain.HarnessClaudeCode,
					Activity: domain.Activity{State: domain.ActivityIdle},
					Metadata: domain.SessionMetadata{
						Branch: "ao/mer-1/root", WorkspacePath: "/wt/mer-1", WorkspaceRepoPath: "/repo",
						Prompt: "unique unfinished work",
					},
				}
				if phase != "workspace" {
					rec.Metadata.RuntimeLaunchID = "unacknowledged-launch"
				}
				if phase == "provider birth" {
					rec.Metadata.AgentSessionID = "native-1"
					rec.Metadata.AgentSessionIDLaunchID = "unacknowledged-launch"
					rec.Metadata.ProviderConversationID = "thread-1"
					rec.Metadata.NativeTranscriptPath = "/transcripts/native-1"
				}
				st.sessions[rec.ID] = rec
				rows := []domain.SessionWorktreeRecord{{SessionID: rec.ID, RepoName: domain.RootWorkspaceRepoName, WorktreePath: rec.Metadata.WorkspacePath, State: "active"}}
				st.worktrees[rec.ID] = append([]domain.SessionWorktreeRecord(nil), rows...)
				// A lost Create acknowledgement supplies no runtime handle to probe.
				// Even the workspace adapter must remain untouched: restoring it may
				// discard unique work before provider liveness is known.
				for pass := 1; pass <= 2; pass++ {
					if err := m.reconcileLive(context.Background(), rec); !errors.Is(err, ErrSpawnRecoveryUncertain) {
						t.Fatalf("pass %d: missing runtime acknowledgement must refuse relaunch: %v", pass, err)
					}
					if err := m.Reconcile(context.Background()); err != nil {
						t.Fatalf("startup %d: %v", pass, err)
					}
					if got := st.sessions[rec.ID]; !reflect.DeepEqual(got, rec) {
						t.Fatalf("pass %d retargeted interrupted spawn: got %+v, want %+v", pass, got, rec)
					}
					if !reflect.DeepEqual(st.worktrees[rec.ID], rows) {
						t.Fatalf("pass %d changed worktree history: %+v", pass, st.worktrees[rec.ID])
					}
				}
				if rt.created != 0 || rt.destroyed != 0 || ws.destroyed != 0 || len(ws.restoreConfigs) != 0 || ws.stashCalls != 0 {
					t.Fatalf("ambiguous spawn touched resources: runtime=%+v workspace=%+v", rt, ws)
				}
				if lcm := m.lcm.(*fakeLCM); lcm.completed != 0 || lcm.terminated[rec.ID] != 0 {
					t.Fatalf("ambiguous spawn changed lifecycle: %+v", lcm)
				}
			})
		}
	}
}

type spawnRecoverySnapshotStore struct {
	Store
	snapshot domain.SessionRecord
}

func (s spawnRecoverySnapshotStore) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	return []domain.SessionRecord{s.snapshot}, nil
}

func TestReconcileStartup_InterruptedSpawnSnapshotCannotRetargetNewerOwner(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "newer generation", true: "foreign provider"}[foreign], func(t *testing.T) {
			m, st, rt, ws := newManager()
			before := domain.SessionRecord{
				ID: "mer-1", ProjectID: "mer", Kind: domain.KindWorker, Harness: domain.HarnessClaudeCode,
				Metadata: domain.SessionMetadata{
					Branch: "ao/mer-1/root", WorkspacePath: "/wt/mer-1",
					RuntimeLaunchID: "lost-ack", AgentSessionID: "native-1", AgentSessionIDLaunchID: "lost-ack",
				},
			}
			current := before
			current.Revision = before.Revision + 1
			current.Metadata.RuntimeLaunchID = "newer-launch"
			current.Metadata.RuntimeHandleID = "newer-runtime"
			current.Metadata.AgentSessionIDLaunchID = "newer-launch"
			if foreign {
				current.Harness = domain.HarnessCodex
				current.Metadata.AgentSessionID = "foreign-native"
				current.Metadata.ProviderConversationID = "foreign-thread"
				current.Metadata.NativeTranscriptPath = "/transcripts/foreign-native"
			}
			st.sessions[current.ID] = current
			// Startup enumeration may precede an ownership commit. The live pass
			// must reread that row before deciding whether to preserve or adopt.
			m.store = spawnRecoverySnapshotStore{Store: st, snapshot: before}
			rt.aliveByHandle = map[string]bool{"newer-runtime": true}
			for pass := 1; pass <= 2; pass++ {
				if err := m.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
				if got := st.sessions[current.ID]; !reflect.DeepEqual(got, current) {
					t.Fatalf("startup %d retargeted newer owner: %+v", pass, got)
				}
			}
			if rt.created != 0 || rt.destroyed != 0 || ws.destroyed != 0 || len(ws.restoreConfigs) != 0 {
				t.Fatal("stale interrupted-spawn snapshot changed resources")
			}
		})
	}
}

func TestReconcileLive_PreservesSpawnOnUnknownRuntimeProbe(t *testing.T) {
	m, st, rt, ws := newManager()
	rec := domain.SessionRecord{
		ID: "mer-1", ProjectID: "mer", Kind: domain.KindWorker, Harness: domain.HarnessClaudeCode,
		Metadata: domain.SessionMetadata{
			Branch: "ao/mer-1/root", WorkspacePath: "/wt/mer-1", RuntimeHandleID: "born-runtime",
			RuntimeLaunchID: "launch-1", AgentSessionID: "native-1", AgentSessionIDLaunchID: "launch-1",
		},
	}
	st.sessions[rec.ID] = rec
	rt.aliveErr = context.DeadlineExceeded
	for pass := 1; pass <= 2; pass++ {
		if err := m.reconcileLive(context.Background(), rec); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("pass %d: probe error = %v", pass, err)
		}
		if got := st.sessions[rec.ID]; !reflect.DeepEqual(got, rec) {
			t.Fatalf("pass %d changed unknown runtime owner: %+v", pass, got)
		}
	}
	if rt.created != 0 || rt.destroyed != 0 || ws.destroyed != 0 || len(ws.restoreConfigs) != 0 {
		t.Fatal("unknown runtime probe changed resources")
	}
}

func TestReconcileStartupSafety_PreservesInterruptedSpawnRecoveryAnchors(t *testing.T) {
	// Freeze the donor's 13 anchor cases at the startup boundary only. The
	// branch, repository, launch, native identity, provider, generation, and
	// transcript cases expose the eight missing guards on the exact base.
	for _, tc := range []struct {
		name     string
		metadata domain.SessionMetadata
		rows     []domain.SessionWorktreeRecord
	}{
		{name: "workspace before branch metadata", metadata: domain.SessionMetadata{WorkspacePath: "/wt/mer-orchestrator"}},
		{name: "branch before workspace metadata", metadata: domain.SessionMetadata{Branch: "ao/mer-orchestrator"}},
		{name: "repository metadata", metadata: domain.SessionMetadata{WorkspaceRepoPath: "/repo"}},
		{name: "runtime before MarkSpawned", metadata: domain.SessionMetadata{RuntimeHandleID: "live-runtime"}},
		{name: "runtime launch", metadata: domain.SessionMetadata{RuntimeLaunchID: "launch-1"}},
		{name: "native agent identity", metadata: domain.SessionMetadata{AgentSessionID: "native-1"}},
		{name: "native agent launch identity", metadata: domain.SessionMetadata{AgentSessionIDLaunchID: "launch-1"}},
		{name: "provider conversation", metadata: domain.SessionMetadata{ProviderConversationID: "thread-1"}},
		{name: "Chat controller generation", metadata: domain.SessionMetadata{ControllerGeneration: "generation-1"}},
		{name: "native transcript", metadata: domain.SessionMetadata{NativeTranscriptPath: "/transcripts/native-1"}},
		{name: "empty shutdown marker", rows: []domain.SessionWorktreeRecord{{RepoName: domain.RootWorkspaceRepoName, State: "removed"}}},
		{name: "child repository marker", rows: []domain.SessionWorktreeRecord{{RepoName: "api", State: "active"}}},
		{name: "multi repository markers", rows: []domain.SessionWorktreeRecord{
			{RepoName: domain.RootWorkspaceRepoName, WorktreePath: "/wt/root", State: "active"},
			{RepoName: "api", WorktreePath: "/wt/root/api", State: "removed"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, st, _, _ := newManager()
			lcm := m.lcm.(*fakeLCM)
			// Non-nil adapters with nil embedded interfaces panic on any method
			// call, including probes. Startup must use durable facts alone.
			m.runtime = struct{ runtimeController }{}
			m.workspace = struct{ ports.Workspace }{}
			mode := domain.SessionModeTUI
			if tc.metadata.ProviderConversationID != "" || tc.metadata.ControllerGeneration != "" {
				mode = domain.SessionModeChat
			}
			rec := domain.SessionRecord{
				ID: "mer-orchestrator", ProjectID: "mer", Kind: domain.KindOrchestrator,
				Mode: mode, Metadata: tc.metadata,
			}
			st.sessions[rec.ID] = rec
			// Copy the fixture so in-place marker mutation cannot change the oracle.
			if tc.rows != nil {
				st.worktrees[rec.ID] = append([]domain.SessionWorktreeRecord(nil), tc.rows...)
			}
			for pass := 1; pass <= 2; pass++ {
				if err := m.ReconcileStartupSafety(context.Background()); err != nil {
					t.Fatalf("startup pass %d: %v", pass, err)
				}
				after, ok := st.sessions[rec.ID]
				if !ok || !reflect.DeepEqual(after, rec) {
					t.Fatalf("startup pass %d changed recovery row: %+v, exists=%v", pass, after, ok)
				}
				if after.ControllerOwner() != rec.ControllerOwner() {
					t.Fatalf("startup pass %d changed original controller ownership", pass)
				}
				if !reflect.DeepEqual(st.worktrees[rec.ID], tc.rows) {
					t.Fatalf("startup pass %d changed recovery markers: %+v", pass, st.worktrees[rec.ID])
				}
				if lcm.terminated[rec.ID] != 0 {
					t.Fatalf("startup pass %d prematurely called MarkTerminated: %v", pass, lcm.terminated)
				}
			}
		})
	}
}
