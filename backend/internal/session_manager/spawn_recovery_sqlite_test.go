package sessionmanager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type spawnRecoveryCrashRuntime struct{ *fakeRuntime }

func (r *spawnRecoveryCrashRuntime) Create(ctx context.Context, cfg ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	if _, err := r.fakeRuntime.Create(ctx, cfg); err != nil {
		return ports.RuntimeHandle{}, err
	}
	// The native runtime was born, but the daemon never received its handle.
	panic("spawn recovery crash after Create")
}

func TestInterruptedSpawn_ColdStartsPreserveUnacknowledgedRuntime(t *testing.T) {
	for _, phase := range []string{"workspace", "launch", "provider birth", "stopped TUI recovery crash"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			st, err := sqlitetest.Open(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if st != nil {
					if err := st.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			workspace := newManagerGitRepo(t)
			uniquePath := filepath.Join(workspace, "unique-untracked-work.txt")
			unique := []byte("unfinished work must survive\r\n")
			if err := os.WriteFile(uniquePath, unique, 0600); err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertProject(ctx, domain.ProjectRecord{
				ID: "mer", Path: workspace, Config: testRoleAgents(), RegisteredAt: time.Now(),
			}); err != nil {
				t.Fatal(err)
			}
			metadata := domain.SessionMetadata{
				Branch: "main", WorkspacePath: workspace, WorkspaceRepoPath: workspace,
				Prompt: "unique unfinished task",
			}
			if phase != "workspace" {
				metadata.RuntimeLaunchID = "lost-ack-launch"
			}
			if phase == "provider birth" || phase == "stopped TUI recovery crash" {
				metadata.AgentSessionID = "native-1"
				metadata.AgentSessionIDLaunchID = metadata.RuntimeLaunchID
				metadata.ProviderConversationID = "provider-thread-1"
				metadata.NativeTranscriptPath = filepath.Join(dataDir, "native-1.jsonl")
				if err := os.WriteFile(metadata.NativeTranscriptPath, []byte("native unfinished history\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			mode := domain.SessionModeTUI
			if phase == "stopped TUI recovery crash" {
				mode = domain.SessionModeChat
			}
			rec, err := st.CreateSession(ctx, domain.SessionRecord{
				ProjectID: "mer", Kind: domain.KindOrchestrator, Mode: mode,
				Harness: domain.HarnessClaudeCode, Metadata: metadata,
				Activity:  domain.Activity{State: domain.ActivityIdle, LastActivityAt: time.Now()},
				CreatedAt: time.Now(), UpdatedAt: time.Now(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if phase == "stopped TUI recovery crash" {
				// Start at the committed Chat target checkpoint. Startup recovery
				// must conclusively stop it and restore the original TUI epoch.
				rec.Metadata.LatestUserPrompt = "poisoned user checkpoint"
				rec.Metadata.LatestAssistantUpdate = "poisoned assistant checkpoint"
				rec.Metadata.ConversationCheckpointState = domain.ConversationCheckpointLegacy
				if err := st.UpdateSession(ctx, rec); err != nil {
					t.Fatal(err)
				}
				now := time.Now()
				if _, inserted, err := st.CreateSessionInterfaceTransition(ctx, domain.SessionInterfaceTransition{
					ID: "interrupted-target", SessionID: rec.ID,
					SourceMode: domain.SessionModeTUI, TargetMode: domain.SessionModeChat,
					Policy: domain.SessionInterfaceTransitionDrain, HistoryPolicy: domain.SessionInterfaceTransitionHistoryStrict,
					Phase: domain.SessionInterfaceTransitionTargetStarting, NativeConversationID: metadata.AgentSessionID,
					CreatedAt: now, UpdatedAt: now,
				}); err != nil || !inserted {
					t.Fatalf("seed handoff: inserted=%v err=%v", inserted, err)
				}
				log := &[]string{}
				born := &spawnRecoveryCrashRuntime{fakeRuntime: &fakeRuntime{}}
				m := New(Deps{
					Store: st, Lifecycle: lifecycle.New(st, nil), Runtime: born, Workspace: &fakeWorkspace{path: workspace},
					Agents: singleAgent{agent: transitionAgent{}}, Messenger: &fakeMessenger{}, DataDir: dataDir,
					Chat: &transitionChat{log: log}, LookPath: func(string) (string, error) { return "/bin/true", nil },
				})
				if _, err := m.recoverInterruptedInterfaceTransitions(ctx); err != nil {
					t.Fatal(err)
				}
				source, ok, err := st.GetSession(ctx, rec.ID)
				if err != nil || !ok || source.Mode != domain.SessionModeTUI {
					t.Fatalf("restore source epoch: %+v exists=%v err=%v", source, ok, err)
				}
				func() {
					defer func() {
						if got := recover(); got != "spawn recovery crash after Create" {
							t.Fatalf("crash boundary = %v", got)
						}
					}()
					if err := m.reconcileLive(ctx, source); err != nil {
						t.Fatalf("recovery failed before crash: %v", err)
					}
				}()
				if born.created != 1 || born.destroyed != 0 {
					t.Fatalf("crash did not leave exactly one native runtime: %+v", born)
				}
				checkpoint, ok, err := st.GetSessionInterfaceTransition(ctx, "interrupted-target")
				if err != nil || !ok || checkpoint.ErrorCode != "SOURCE_RESTORE_UNCONFIRMED" {
					t.Fatalf("runtime birth was not durably fenced: %+v err=%v", checkpoint, err)
				}
			}
			if err := st.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{
				SessionID: rec.ID, RepoName: domain.RootWorkspaceRepoName, Branch: "main",
				WorktreePath: workspace, PreservedRef: "refs/ao/preserved/unique-work", State: "active",
			}); err != nil {
				t.Fatal(err)
			}
			conversation, err := st.CreateConversation(ctx, "spawn-history", domain.ConversationScopeProject, "mer", rec.ID, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			before, ok, err := st.GetSession(ctx, rec.ID)
			if err != nil || !ok {
				t.Fatalf("read baseline: exists=%v err=%v", ok, err)
			}
			if phase == "stopped TUI recovery crash" && (before.Metadata.RuntimeHandleID != "" ||
				before.Metadata.LatestUserPrompt != "poisoned user checkpoint" ||
				before.Metadata.LatestAssistantUpdate != "poisoned assistant checkpoint" ||
				before.Metadata.ConversationCheckpointState != domain.ConversationCheckpointLegacy) {
				t.Fatalf("crash lost the original checkpoint: %+v", before)
			}
			rows, err := st.ListSessionWorktrees(ctx, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			history, err := st.LoadConversationSnapshot(ctx, conversation.ID)
			if err != nil {
				t.Fatal(err)
			}
			handoff, hasHandoff, err := st.GetLatestSessionInterfaceTransition(ctx, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			for pass := 1; pass <= 2; pass++ {
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				st, err = sqlite.OpenPreMigrated(dataDir)
				if err != nil {
					t.Fatal(err)
				}
				rt, ws := &fakeRuntime{}, &fakeWorkspace{}
				m := New(Deps{
					Store: st, Lifecycle: lifecycle.New(st, nil), Runtime: rt, Workspace: ws,
					Agents: fakeAgents{}, Messenger: &fakeMessenger{}, DataDir: dataDir,
					LookPath: func(string) (string, error) { return "/bin/true", nil },
				})
				startupCtx, cancelStartup := context.WithCancel(ctx)
				err = m.Reconcile(startupCtx)
				cancelStartup()
				if err != nil {
					t.Fatalf("cold startup %d: %v", pass, err)
				}
				if err := m.reconcileLive(ctx, before); !errors.Is(err, ErrSpawnRecoveryUncertain) {
					t.Fatalf("cold startup %d recovery outcome = %v", pass, err)
				}
				after, ok, err := st.GetSession(ctx, rec.ID)
				if err != nil || !ok || !reflect.DeepEqual(after, before) {
					t.Fatalf("cold startup %d changed session: %+v exists=%v err=%v", pass, after, ok, err)
				}
				afterRows, err := st.ListSessionWorktrees(ctx, rec.ID)
				if err != nil || !reflect.DeepEqual(afterRows, rows) {
					t.Fatalf("cold startup %d changed worktree rows: %+v err=%v", pass, afterRows, err)
				}
				afterHistory, err := st.LoadConversationSnapshot(ctx, conversation.ID)
				if err != nil || !reflect.DeepEqual(afterHistory, history) {
					t.Fatalf("cold startup %d changed native conversation projection: err=%v", pass, err)
				}
				afterHandoff, found, err := st.GetLatestSessionInterfaceTransition(ctx, rec.ID)
				if err != nil || found != hasHandoff || !reflect.DeepEqual(afterHandoff, handoff) {
					t.Fatalf("cold startup %d lost handoff: %+v err=%v", pass, afterHandoff, err)
				}
				if contents, err := os.ReadFile(uniquePath); err != nil || string(contents) != string(unique) {
					t.Fatalf("cold startup %d lost unique work: %q err=%v", pass, contents, err)
				}
				if metadata.NativeTranscriptPath != "" {
					if contents, err := os.ReadFile(metadata.NativeTranscriptPath); err != nil || string(contents) != "native unfinished history\n" {
						t.Fatalf("cold startup %d changed native transcript: %q err=%v", pass, contents, err)
					}
				}
				if rt.created != 0 || rt.destroyed != 0 || ws.destroyed != 0 || len(ws.restoreConfigs) != 0 || ws.stashCalls != 0 {
					t.Fatalf("cold startup %d mutated provider or workspace", pass)
				}
			}
		})
	}
}
