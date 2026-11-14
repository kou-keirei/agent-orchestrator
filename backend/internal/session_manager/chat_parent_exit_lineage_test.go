package sessionmanager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// These are disposable owner-contract fixtures, not native authentication or
// real-model acceptance. The production Chat launcher intentionally lacks this
// owner adapter until that separate boundary is implemented and reviewed.
type parentExitOwnerFixture struct {
	*recordingLauncher
	binding                  ChatParentExitBinding
	lease                    *parentExitLeaseFixture
	sourceCalls              int
	skipSource, replaySource bool
	ignoreErrors             bool
}

func (f *parentExitOwnerFixture) WithChatParentExitLineage(_ context.Context, _ ports.ExactReportMessage, fn func(ChatParentExitBinding, ChatParentExitLease) error) error {
	f.sourceCalls++
	if f.skipSource {
		return nil
	}
	err := fn(f.binding, f.lease)
	if f.replaySource {
		err = errors.Join(err, fn(f.binding, f.lease))
	}
	if f.ignoreErrors {
		return nil
	}
	return err
}

type parentExitLeaseFixture struct {
	exit                                             ports.ChatProviderExitEvidence
	exitErr                                          error
	afterExit                                        func()
	validate                                         func(int) error
	validations, destinations                        int
	observed                                         ports.ExactReportParent
	skipDestination, replayDestination, ignoreErrors bool
	authErr                                          error
	beforeDestination                                func()
}

func (f *parentExitLeaseFixture) PositiveExitEvidence(context.Context) (ports.ChatProviderExitEvidence, error) {
	if f.afterExit != nil {
		f.afterExit()
	}
	return f.exit, f.exitErr
}

func (f *parentExitLeaseFixture) Validate(_ context.Context) error {
	f.validations++
	if f.validate != nil {
		return f.validate(f.validations)
	}
	return nil
}

func (f *parentExitLeaseFixture) WithDestination(_ context.Context, owner domain.SessionControllerOwner, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	f.destinations++
	if f.authErr != nil {
		return f.authErr
	}
	if f.beforeDestination != nil {
		f.beforeDestination()
	}
	if owner.ControllerGeneration != "destination-generation" {
		return errors.New("fixture owner mismatch")
	}
	if f.skipDestination {
		return nil
	}
	err := fn(f.observed, parentExitChatFixture{})
	if f.replayDestination {
		err = errors.Join(err, fn(f.observed, parentExitChatFixture{}))
	}
	if f.ignoreErrors {
		return nil
	}
	return err
}

type parentExitChatFixture struct{}

func (parentExitChatFixture) SendReport(context.Context, string, string) (bool, error) {
	return false, errors.New("fixture must not send")
}
func (parentExitChatFixture) ReadReportReconciliation(context.Context, string) (ports.ExactReportReconciliation, error) {
	return ports.ExactReportReconciliation{}, errors.New("fixture must not ACK")
}

func newParentExitFixture(t *testing.T) (*Manager, *historicalChatRestoreStore, *parentExitOwnerFixture, ports.ExactReportMessage) {
	t.Helper()
	launcher := &parentExitOwnerFixture{recordingLauncher: &recordingLauncher{}}
	m, base, _ := newChatManager(launcher)
	rec := domain.SessionRecord{
		ID: "mer-parent", ProjectID: chatTestProject, Kind: domain.KindOrchestrator,
		Mode: domain.SessionModeChat, Harness: domain.HarnessCodex,
		Activity: domain.Activity{State: domain.ActivityExited},
		Metadata: domain.SessionMetadata{WorkspacePath: t.TempDir(), Branch: "ao/parent", ProviderConversationID: "thread-1", ControllerGeneration: "source-generation"},
	}
	base.sessions[rec.ID] = rec
	transitions := newTransitionStore()
	transitions.fakeStore = base
	st := &historicalChatRestoreStore{
		transitionStore: transitions,
		conversation:    domain.ConversationRecord{ID: "conversation", ProjectID: chatTestProject, SessionID: rec.ID, ActiveBranchID: "branch"},
		activeBranch:    domain.ConversationBranch{ID: "branch", ConversationID: "conversation", SessionID: rec.ID, ProviderConversationID: "thread-1", ProviderScopeID: "scope"},
	}
	m.store = st
	m.newLaunchID = func() string { return "destination-generation" }
	message := ports.ExactReportMessage{ReportID: "report", EventID: "event", SourceRevision: "source", LeaseID: "lease", LeaseRevision: 7, Payload: "immutable bytes",
		Parent: ports.ExactReportParent{WorkItemID: "work", AttemptID: "attempt", SessionID: rec.ID, NativeSessionID: "thread-1", ProviderConversationID: "thread-1", ProviderScopeID: "scope", ControllerGeneration: "source-generation"},
	}
	launcher.binding = ChatParentExitBinding{Message: message, Owner: rec.ControllerOwner(), ConversationID: "conversation", BranchID: "branch", RuntimeGeneration: "runtime-generation", OwnerID: "native-owner", AttachmentGeneration: 4}
	launcher.lease = &parentExitLeaseFixture{
		exit:     ports.ChatProviderExitEvidence{SessionID: rec.ID, ProviderConversationID: "thread-1", ProviderScopeID: "scope", ControllerGeneration: "source-generation", RuntimeGeneration: "runtime-generation", OwnerID: "native-owner", AttachmentGeneration: 4, ExitCode: 23, ObservedAt: time.Now()},
		observed: message.Parent,
	}
	launcher.lease.observed.ControllerGeneration = "destination-generation"
	return m, st, launcher, message
}

func TestChatParentExitLineageRestoresSameThreadWithSeparateEpoch(t *testing.T) {
	m, st, owner, message := newParentExitFixture(t)
	before := st.sessions[message.Parent.SessionID]
	calls := 0
	err := m.WithResumedChatParentAfterExit(t.Context(), message, func(lineage ChatParentExitLineage, _ ports.ExactReportChat) error {
		calls++
		if lineage.Source.Message != message || lineage.Exit != owner.lease.exit || lineage.Destination != owner.lease.observed || lineage.Source.Owner.ControllerGeneration == lineage.Destination.ControllerGeneration {
			t.Fatalf("source relabeled or destination changed: %+v", lineage)
		}
		if !m.SessionMutationInProgress(message.Parent.SessionID) {
			t.Fatal("resume fence released before destination callback")
		}
		return nil
	})
	if err != nil || calls != 1 || len(owner.started) != 1 || owner.lease.destinations != 1 {
		t.Fatalf("err=%v calls=%d starts=%d", err, calls, len(owner.started))
	}
	start := owner.started[0]
	after := st.sessions[message.Parent.SessionID]
	if start.HistoryMode != ports.ChatHistoryRequired || start.ProviderConversationID != "thread-1" || start.ControllerGeneration != "destination-generation" || start.ExpectedControllerOwner != before.ControllerOwner() ||
		after.ID != before.ID || after.Metadata.WorkspacePath != before.Metadata.WorkspacePath || after.IsTerminated || len(owner.turns) != 0 || len(owner.relayed) != 0 || m.SessionMutationInProgress(before.ID) {
		t.Fatalf("restore broke native/fence contract: start=%+v after=%+v", start, after)
	}
	// A stale issuance/source binding cannot restore again after epoch movement.
	if err := m.WithResumedChatParentAfterExit(t.Context(), message, func(ChatParentExitLineage, ports.ExactReportChat) error { t.Fatal("replayed delivery"); return nil }); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || len(owner.started) != 1 {
		t.Fatalf("replay=%v starts=%d", err, len(owner.started))
	}
}

func TestChatParentExitLineageRejectsUnprovenOrForeignExit(t *testing.T) {
	for _, name := range []string{"stopped", "EOF", "session", "thread", "scope", "controller", "runtime", "owner", "attachment", "time", "canceled"} {
		t.Run(name, func(t *testing.T) {
			m, _, owner, message := newParentExitFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "stopped", "EOF":
				owner.lease.exitErr = ports.ErrChatProviderExitUnproven
			case "session":
				owner.lease.exit.SessionID = "foreign"
			case "thread":
				owner.lease.exit.ProviderConversationID = "foreign"
			case "scope":
				owner.lease.exit.ProviderScopeID = "foreign"
			case "controller":
				owner.lease.exit.ControllerGeneration = "foreign"
			case "runtime":
				owner.lease.exit.RuntimeGeneration = "foreign"
			case "owner":
				owner.lease.exit.OwnerID = "foreign"
			case "attachment":
				owner.lease.exit.AttachmentGeneration++
			case "time":
				owner.lease.exit.ObservedAt = time.Time{}
			case "canceled":
				owner.lease.afterExit = cancel
			}
			err := m.WithResumedChatParentAfterExit(ctx, message, func(ChatParentExitLineage, ports.ExactReportChat) error { t.Fatal("foreign exit admitted"); return nil })
			if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || len(owner.started) != 0 || owner.lease.destinations != 0 || m.SessionMutationInProgress(message.Parent.SessionID) {
				t.Fatalf("err=%v starts=%d destinations=%d", err, len(owner.started), owner.lease.destinations)
			}
		})
	}
}

func TestChatParentExitLineageRejectsSourceAndLeaseDrift(t *testing.T) {
	for _, name := range []string{"unbound", "binding", "lease", "lease_after_wait", "terminated", "worker", "blocked", "waiting", "checkpoint", "history", "branch", "owner_after_wait", "live", "transition", "operation", "generation", "restore_failure"} {
		t.Run(name, func(t *testing.T) {
			m, st, owner, message := newParentExitFixture(t)
			rec := st.sessions[message.Parent.SessionID]
			switch name {
			case "unbound":
				m.chat = owner.recordingLauncher
			case "binding":
				owner.binding.Message.LeaseRevision++
			case "lease", "lease_after_wait":
				owner.lease.validate = func(n int) error {
					if name == "lease" || n >= 2 {
						return errors.New("stale lease")
					}
					return nil
				}
			case "terminated":
				rec.IsTerminated = true
			case "worker":
				rec.Kind = domain.KindWorker
			case "blocked":
				rec.Activity.State = domain.ActivityBlocked
			case "waiting":
				rec.Activity.State = domain.ActivityWaitingInput
			case "checkpoint":
				rec.Metadata.ConversationCheckpointUnsettled = true
			case "history":
				st.activeBranch.ProviderScopeID = "foreign"
			case "branch":
				st.conversation.ActiveBranchID = "foreign"
			case "owner_after_wait":
				owner.lease.afterExit = func() {
					changed := st.sessions[rec.ID]
					changed.Metadata.ControllerGeneration = "foreign"
					st.sessions[rec.ID] = changed
				}
			case "live":
				owner.live = true
			case "transition":
				st.activeErr = errors.New("transition unavailable")
			case "operation":
				if err := m.beginAgentOperation(t.Context(), rec.ID, agentOperationKill); err != nil {
					t.Fatal(err)
				}
				defer m.endAgentOperation(rec.ID, agentOperationKill)
			case "generation":
				m.newLaunchID = func() string { return "source-generation" }
			case "restore_failure":
				owner.startErr = errors.New("native resume failed")
			}
			st.sessions[rec.ID] = rec
			err := m.WithResumedChatParentAfterExit(t.Context(), message, func(ChatParentExitLineage, ports.ExactReportChat) error {
				t.Fatal("unsafe source admitted")
				return nil
			})
			wantStarts := 0
			if name == "restore_failure" {
				wantStarts = 1
			}
			var held *ports.ReportDeliveryHold
			if !errors.As(err, &held) || len(owner.started) != wantStarts || owner.lease.destinations != 0 {
				t.Fatalf("err=%v starts=%d destinations=%d", err, len(owner.started), owner.lease.destinations)
			}
		})
	}
}

func TestChatParentExitLineageRejectsDestinationAndCallbackReplay(t *testing.T) {
	for _, name := range []string{"auth", "generation", "thread", "scope", "work", "lease", "missing_source", "missing_destination", "replay_source", "replay_destination", "swallowed_callback_error", "restore_thread", "destination_drift", "destination_expired"} {
		t.Run(name, func(t *testing.T) {
			m, st, owner, message := newParentExitFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "auth":
				owner.lease.authErr = errors.New("destination authentication unavailable")
			case "generation":
				owner.lease.observed.ControllerGeneration = "source-generation"
			case "thread":
				owner.lease.observed.ProviderConversationID = "foreign"
			case "scope":
				owner.lease.observed.ProviderScopeID = "foreign"
			case "work":
				owner.lease.observed.WorkItemID = "foreign"
			case "lease":
				owner.lease.validate = func(n int) error {
					if n >= 3 {
						return errors.New("lease changed after launch")
					}
					return nil
				}
			case "missing_source":
				owner.skipSource = true
			case "missing_destination":
				owner.lease.skipDestination = true
			case "replay_source":
				owner.replaySource, owner.ignoreErrors = true, true
			case "replay_destination":
				owner.lease.replayDestination, owner.lease.ignoreErrors = true, true
			case "swallowed_callback_error":
				owner.lease.ignoreErrors, owner.ignoreErrors = true, true
			case "restore_thread":
				owner.afterReady = func() {
					rec := st.sessions[message.Parent.SessionID]
					rec.Metadata.ProviderConversationID = "foreign"
					st.sessions[rec.ID] = rec
				}
			case "destination_drift":
				owner.lease.beforeDestination = func() {
					rec := st.sessions[message.Parent.SessionID]
					rec.Metadata.ControllerGeneration = "foreign"
					st.sessions[rec.ID] = rec
				}
			case "destination_expired":
				owner.lease.beforeDestination = cancel
				owner.lease.ignoreErrors, owner.ignoreErrors = true, true
			}
			calls := 0
			err := m.WithResumedChatParentAfterExit(ctx, message, func(ChatParentExitLineage, ports.ExactReportChat) error {
				calls++
				if name == "swallowed_callback_error" {
					return &ports.ReportDeliveryHold{Reason: "consumer refused"}
				}
				return nil
			})
			wantCalls := 0
			if name == "replay_source" || name == "replay_destination" || name == "swallowed_callback_error" {
				wantCalls = 1
			}
			if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || calls != wantCalls || len(owner.started) > 1 || m.SessionMutationInProgress(message.Parent.SessionID) {
				t.Fatalf("err=%v callbacks=%d starts=%d", err, calls, len(owner.started))
			}
		})
	}
}
