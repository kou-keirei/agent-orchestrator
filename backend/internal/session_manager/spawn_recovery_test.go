package sessionmanager

import (
	"context"
	"reflect"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

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
