package chat_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// This provider records its own synthetic native history when Chat sends. It
// does not read AO rows to manufacture provider continuity or semantic ACKs.
type reportParentProvider struct {
	*fakeConversation
	historyMu  sync.Mutex
	history    []ports.ChatEvent
	historyErr error
	holdTurn   bool
}

func (p *reportParentProvider) SendTurn(ctx context.Context, msg ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	ref, err := p.fakeConversation.SendTurn(ctx, msg)
	if err != nil {
		return ref, err
	}
	id := ref.ProviderTurnID
	events := []ports.ChatEvent{
		{Kind: ports.ChatEventTurnStarted, ProviderEventID: id + ":start", ProviderTurnID: id},
		{Kind: ports.ChatEventUserMessageCompleted, ProviderEventID: id + ":user", ProviderTurnID: id, Text: msg.Text},
		{Kind: ports.ChatEventMessageCompleted, ProviderEventID: id + ":assistant", ProviderTurnID: id, ProviderItemID: id + ":item", Text: "fixture parent answer"},
		{Kind: ports.ChatEventTurnCompleted, ProviderEventID: id + ":done", ProviderTurnID: id, TurnState: domain.TurnStateCompleted},
	}
	p.historyMu.Lock()
	p.history = append(p.history, events...)
	p.historyMu.Unlock()
	if !p.holdTurn {
		p.emit(events...)
	}
	return ref, nil
}

func (p *reportParentProvider) ReadHistory(context.Context) ([]ports.ChatEvent, error) {
	p.historyMu.Lock()
	defer p.historyMu.Unlock()
	return append([]ports.ChatEvent(nil), p.history...), p.historyErr
}

type reportParentFixture struct {
	store                   *sqlite.Store
	dir                     string
	service                 *chatsvc.Service
	provider                *reportParentProvider
	receipt                 domain.WorkerReportReceipt
	parent                  ports.ExactReportParent
	lease                   domain.WorkItemLease
	starts, resumes, inputs atomic.Int32
}

func newReportParentFixture(t *testing.T, wrap ...func(*sqlite.Store) chatsvc.Store) *reportParentFixture {
	t.Helper()
	f := &reportParentFixture{dir: t.TempDir(), provider: &reportParentProvider{fakeConversation: newFakeConversation()}}
	st, err := sqlitetest.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	f.store = st
	t.Cleanup(func() {
		if f.service != nil {
			_ = f.service.Stop(context.Background(), testSession)
		}
		_ = f.store.Close()
	})
	ctx := t.Context()
	if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: string(testProject), Path: f.dir, RegisteredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateSession(ctx, domain.SessionRecord{ID: testSession, ProjectID: testProject, Kind: domain.KindOrchestrator,
		Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	worker, err := st.CreateSession(ctx, domain.SessionRecord{ProjectID: testProject, Kind: domain.KindWorker,
		Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	i := domain.WorkItemIdentity{ID: "native-report-work", AttemptID: "native-report-attempt", ProjectID: testProject,
		Repository: domain.RootWorkspaceRepoName, ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: testSession,
		OriginalWorkerSessionID: worker.ID, OwnerSessionID: worker.ID, OwnerGeneration: 1, WorktreePath: t.TempDir(), Branch: "ao/fixture/root"}
	parentPath := t.TempDir()
	for _, id := range []domain.SessionID{testSession, worker.ID} {
		path := i.WorktreePath
		if id == testSession {
			path = parentPath
		}
		if err := st.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{SessionID: id, RepoName: i.Repository,
			WorktreePath: path, Branch: i.Branch, BaseSHA: i.ExecutionBaseSHA}); err != nil {
			t.Fatal(err)
		}
	}
	var next atomic.Int32
	var chatStore chatsvc.Store = st
	if len(wrap) != 0 {
		chatStore = wrap[0](st)
	}
	f.service = chatsvc.New(chatsvc.Options{Store: chatStore, Sessions: st, Reader: fullSnapshotReader(st),
		Drivers: fakeRegistry{driver: fakeDriver{
			start: func(ports.ChatStartConfig) (ports.ChatConversation, error) { f.starts.Add(1); return f.provider, nil },
			resume: func(ports.ChatResumeConfig) (ports.ChatConversation, error) {
				f.resumes.Add(1)
				return nil, ports.ErrChatResumeFailed
			},
		}}, NewID: func() string { return fmt.Sprintf("report-parent-%d", next.Add(1)) }})
	_, err = f.service.Start(ctx, chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject, Kind: domain.KindOrchestrator,
		Harness: domain.HarnessCodex, WorkspacePath: parentPath, ControllerReady: func(r chatsvc.StartResult) (chatsvc.ControllerCommit, error) {
			metadata := domain.SessionMetadata{WorkspacePath: parentPath, ProviderConversationID: r.ProviderConversationID, ControllerGeneration: r.ControllerGeneration}
			return chatsvc.ControllerCommit{Conversation: r.Conversation}, lifecycle.New(st, nil).MarkSpawned(ctx, testSession, metadata)
		}})
	if err != nil {
		t.Fatal(err)
	}
	f.service.SetInputAdmission(func(context.Context, domain.SessionID, string) (func(), error) {
		f.inputs.Add(1)
		return func() {}, nil
	})
	// A real Chat-owned issuer captures the controller; worker-authored native
	// provenance is deliberately different from the parent provider handle.
	issuer, err := f.service.OriginalParentIssuer(testSession)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.IssueWorkItemFromOriginalParent(ctx, i, "distinct-worker-native"); err != nil {
		t.Fatal(err)
	}
	f.parent, err = st.ReadReportOriginalParent(ctx, i.ID, i.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = st.AcquireWorkItemLease(ctx, i, domain.WorkLeaseGrant{ID: "native-report-lease", Duration: time.Hour, AllowedEffects: []string{"report"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReserveWorkItemAdmission(ctx, i, f.lease); err != nil {
		t.Fatal(err)
	}
	content := "fixture accepted report"
	sum := sha256.Sum256([]byte(content))
	f.receipt, err = st.AcceptWorkerReport(ctx, domain.WorkerReport{ID: "native-parent-report", Type: "checkpoint", Version: 1,
		Identity: i, LeaseID: f.lease.Grant.ID, LeaseRevision: f.lease.Revision, WorkerSessionID: worker.ID,
		NativeSessionID: "distinct-worker-native", OriginalNativeSessionID: "distinct-worker-native", NativeTurnID: "worker-turn",
		Content: content, Digest: hex.EncodeToString(sum[:])})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Faults occur only AFTER the real append transaction has committed. The read
// fault is the old append/pre-reservation read window; reservation now belongs
// to the append transaction, so every such durable admission is already held.
type reportAdmissionFaultStore struct {
	*sqlite.Store
	fault     string
	cancel    context.CancelFunc
	admitted  atomic.Bool
	readFault atomic.Bool
	drains    chan error
}

func (s *reportAdmissionFaultStore) AppendReservedUserMessage(ctx context.Context, owner domain.ConversationSendOwner, msg domain.ConversationMessage, now time.Time) (bool, error) {
	created, err := s.Store.AppendReservedUserMessage(ctx, owner, msg, now)
	if err != nil || !created {
		return created, err
	}
	s.admitted.Store(true)
	switch s.fault {
	case "read":
		s.readFault.Store(true)
	case "cancel":
		s.cancel()
	case "lost-response":
		return false, errors.New("injected lost append response before dispatch")
	}
	return created, nil
}

func (s *reportAdmissionFaultStore) OrdinarySendAttempt(ctx context.Context, turnID string) (domain.ConversationSendAttempt, error) {
	if s.readFault.CompareAndSwap(true, false) {
		return domain.ConversationSendAttempt{}, errors.New("injected post-append read failure")
	}
	return s.Store.OrdinarySendAttempt(ctx, turnID)
}

func (s *reportAdmissionFaultStore) NextQueuedTurn(ctx context.Context, conversationID string) (domain.QueuedTurn, error) {
	queued, err := s.Store.NextQueuedTurn(ctx, conversationID)
	select {
	case s.drains <- err:
	default:
	}
	return queued, err
}

// Exercise real turn-completion drain, then acquire the same dispatch mutex via
// ArmHandoff. This barrier makes zero provider calls a deterministic assertion,
// including if a regression selected pending work before signaling the probe.
func assertReportOrdinaryDrainHeld(t *testing.T, f *reportParentFixture, probe *reportAdmissionFaultStore, id string) {
	t.Helper()
	for len(probe.drains) > 0 {
		<-probe.drains
	}
	f.provider.emit(
		ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderEventID: id + ":start", ProviderTurnID: id},
		ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderEventID: id + ":done", ProviderTurnID: id, TurnState: domain.TurnStateCompleted},
	)
	select {
	case err := <-probe.drains:
		if !errors.Is(err, domain.ErrConversationSendUncertain) {
			t.Fatalf("ordinary drain selected stranded report: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("turn completion did not exercise ordinary drain")
	}
	ctrl, err := f.service.Controller(testSession)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.ArmHandoff(t.Context(), domain.SessionInterfaceTransitionDrain); err != nil {
		t.Fatal(err)
	}
	defer ctrl.AbortHandoff()
	if f.provider.sendCallCount() != 0 {
		t.Fatal("ordinary drain dispatched report outside its attempt")
	}
}

func TestReportAdmissionFaultCannotEscapeAfterRestartAndAuthorityChange(t *testing.T) {
	for _, fault := range []string{"read", "cancel", "lost-response"} {
		for _, change := range []string{"owner", "lease-revision", "lease-release", "lease-expiry"} {
			t.Run(fault+"/"+change, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var probe *reportAdmissionFaultStore
				f := newReportParentFixture(t, func(st *sqlite.Store) chatsvc.Store {
					probe = &reportAdmissionFaultStore{Store: st, fault: fault, cancel: cancel, drains: make(chan error, 16)}
					return probe
				})
				if err := f.engine().Deliver(ctx, testProject, f.receipt.Report.ID); err == nil {
					t.Fatal("fault acknowledged delivery")
				}
				if !probe.admitted.Load() || f.provider.sendCallCount() != 0 {
					t.Fatal("fault did not occur after durable append and before provider dispatch")
				}
				conversation, err := f.store.ConversationForSession(t.Context(), testSession)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := f.store.LoadConversationSnapshot(t.Context(), conversation.ID)
				if err != nil || len(rows.Messages) != 1 {
					t.Fatalf("durable report append: %+v %v", rows, err)
				}
				message := rows.Messages[0]
				attempt, err := f.store.OrdinarySendAttempt(t.Context(), message.TurnID)
				if err != nil || attempt.State != domain.ConversationSendReserved || attempt.ReservedAt == nil ||
					attempt.ControllerGeneration != f.parent.ControllerGeneration || attempt.BranchID != conversation.ActiveBranchID {
					t.Fatalf("append left drain-eligible or foreign admission: %+v %v", attempt, err)
				}
				switch change {
				case "owner":
					work, found, err := f.store.GetWorkItem(t.Context(), f.receipt.Report.Identity.ID)
					if err != nil || !found {
						t.Fatalf("work readback: %v %v", found, err)
					}
					if _, err := f.store.ClaimWorkItemOwner(t.Context(), work, testSession); err != nil {
						t.Fatal(err)
					}
				case "lease-revision":
					if _, err := f.store.RenewWorkItemLease(t.Context(), f.receipt.Report.Identity, f.lease, f.lease.ExpiresAt.Add(time.Minute)); err != nil {
						t.Fatal(err)
					}
				case "lease-release":
					if _, err := f.store.ReleaseWorkItemLease(t.Context(), f.receipt.Report.Identity, f.lease); err != nil {
						t.Fatal(err)
					}
				case "lease-expiry":
					// Deterministic expiry of this disposable fixture only, no clock sleep.
					db, err := sql.Open("sqlite", filepath.Join(f.dir, "ao.db"))
					if err != nil {
						t.Fatal(err)
					}
					_, updateErr := db.ExecContext(t.Context(), `UPDATE work_item_leases SET expires_at_ms = 1 WHERE work_item_id = ?`, string(f.receipt.Report.Identity.ID))
					closeErr := db.Close()
					if err := errors.Join(updateErr, closeErr); err != nil {
						t.Fatal(err)
					}
				}
				assertReportOrdinaryDrainHeld(t, f, probe, "unrelated-before-restart")
				if err := f.service.Stop(t.Context(), testSession); err != nil {
					t.Fatal(err)
				}
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				st, err := sqlite.Open(f.dir)
				if err != nil {
					t.Fatal(err)
				}
				f.store = st
				probe = &reportAdmissionFaultStore{Store: st, drains: make(chan error, 16)}
				f.provider = &reportParentProvider{fakeConversation: newFakeConversation()}
				var next atomic.Int32
				f.service = chatsvc.New(chatsvc.Options{Store: probe, Sessions: st, Reader: fullSnapshotReader(st),
					Drivers: fakeRegistry{driver: fakeDriver{conv: f.provider}},
					NewID:   func() string { return fmt.Sprintf("report-replacement-%d", next.Add(1)) }})
				// Explicit ordinary restoration of the same fake native handle. This
				// does not call or qualify the decision-held report Resume adapter.
				ctrl, err := f.service.Start(t.Context(), chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject,
					Harness: domain.HarnessCodex, ProviderConversationID: f.parent.ProviderConversationID, HistoryMode: ports.ChatHistoryDeferred})
				if err != nil || ctrl.Generation() == f.parent.ControllerGeneration {
					t.Fatalf("replacement generation: %v", err)
				}
				assertReportOrdinaryDrainHeld(t, f, probe, "unrelated-after-restart")
				if _, err := f.service.Send(t.Context(), testSession, ports.ChatUserMessage{ClientMessageID: message.ClientMessageID, Text: message.Text, Origin: message.Origin}); err == nil {
					t.Fatal("replacement acknowledged stranded report")
				}
				reopened, err := st.OrdinarySendAttempt(t.Context(), message.TurnID)
				if err != nil || reopened.ConversationSendOwner != attempt.ConversationSendOwner || reopened.State != attempt.State ||
					reopened.ReservedAt == nil || !reopened.ReservedAt.Equal(*attempt.ReservedAt) || f.provider.sendCallCount() != 0 {
					t.Fatalf("restart changed or dispatched reservation: %+v %v", reopened, err)
				}
			})
		}
	}
}

func (f *reportParentFixture) engine() *orchestrationevents.ExactParentReports {
	return &orchestrationevents.ExactParentReports{Store: f.store, Parents: f.store, Native: f.service, Resume: f.service}
}

func (f *reportParentFixture) event(t *testing.T) domain.OrchestrationEvent {
	t.Helper()
	events, err := f.store.ListOrchestrationEvents(t.Context(), testProject, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.ID == f.receipt.EventID {
			return event
		}
	}
	t.Fatal("missing report event")
	return domain.OrchestrationEvent{}
}

func (f *reportParentFixture) settle(t *testing.T) {
	t.Helper()
	conversation, err := f.store.ConversationForSession(t.Context(), testSession)
	if err != nil {
		t.Fatal(err)
	}
	awaitStoreSnapshot(t, f.store, conversation.ID, func(s store.ConversationSnapshot) bool {
		return len(s.Turns) > 0 && s.Turns[len(s.Turns)-1].State == domain.TurnStateCompleted
	})
	// Durable projection precedes afterProject releasing sendMu. Wait for the
	// real adapter gate as well, without sending or reading semantic assertions.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		err := f.service.WithLiveReportParent(ctx, f.parent, func(ports.ExactReportParent, ports.ExactReportChat) error { return nil })
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReportParentAdapterDeliveryIsNotSemanticACK(t *testing.T) {
	f := newReportParentFixture(t)
	d := f.engine()
	d.Source = f.store
	runtime := &orchestrationevents.Dispatcher{Store: f.store, Reports: d}
	if err := runtime.DispatchProject(t.Context(), testProject); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	f.settle(t)
	event := f.event(t)
	message, err := ports.ParseExactReportMessage(event.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	sent := f.provider.sentMessages()
	if event.State != domain.OrchestrationSubmitted || !event.AcknowledgedAt.IsZero() || event.DestinationSessionID != testSession ||
		message.Parent != f.parent || message.Parent.NativeSessionID == f.receipt.Report.NativeSessionID || len(sent) != 1 ||
		sent[0].Text != message.Payload || sent[0].ClientMessageID != message.ClientMessageID() || f.inputs.Load() == 0 {
		t.Fatalf("incorrect destination or admission-as-ACK: %+v sent=%+v", event, sent)
	}
	if n, err := f.store.AcknowledgeOrchestrationBatchAccepted(t.Context(), testSession, message.Token(), time.Now()); err != nil || n != 0 {
		t.Fatalf("generic ACK advanced report: %d %v", n, err)
	}
	var expired ports.ExactReportChat
	if err := f.service.WithLiveReportParent(t.Context(), f.parent, func(_ ports.ExactReportParent, chat ports.ExactReportChat) error {
		expired = chat
		admitted, err := chat.SendReport(t.Context(), message.ClientMessageID(), message.Payload)
		if err != nil || !admitted {
			t.Fatalf("durable duplicate delivery: %v %v", admitted, err)
		}
		if _, err := chat.SendReport(t.Context(), message.ClientMessageID(), "changed payload"); err == nil {
			t.Fatal("duplicate key accepted changed payload")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if admitted, err := expired.SendReport(t.Context(), "outside-gate", "must never reach provider"); admitted || !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatalf("report adapter escaped its callback gate: %v %v", admitted, err)
	}
	// The real adapter has no trusted semantic producer. Native completion and
	// an asserted event reference must leave the report unresolved.
	err = d.Reconcile(t.Context(), testProject, f.receipt.Report.ID, "asserted-provider-completion")
	if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !strings.Contains(err.Error(), "report_parent_semantic_producer_unbound") {
		t.Fatal(err)
	}
	if got := f.event(t); got.State != domain.OrchestrationSubmitted || !got.AcknowledgedAt.IsZero() || got.AttentionRequiredAt.IsZero() {
		t.Fatalf("false semantic ACK: %+v", got)
	}
	for range 2 {
		if err := runtime.DispatchProject(t.Context(), testProject); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
			t.Fatal(err)
		}
	}
	if f.provider.sendCallCount() != 1 || f.resumes.Load() != 0 || f.starts.Load() != 1 {
		t.Fatal("submitted report caused another provider operation")
	}
}

func TestReportParentAdapterRefusalsNeverSend(t *testing.T) {
	for _, change := range []string{"wrong-parent", "worker-native", "generation", "thread", "scope", "stale-owner", "lease-revision", "released-lease", "busy", "handoff", "stopped", "history-unavailable", "history-mismatch", "canceled"} {
		t.Run(change, func(t *testing.T) {
			f := newReportParentFixture(t)
			ctx := t.Context()
			parent := f.parent
			control, err := f.service.Controller(testSession)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "wrong-parent":
				parent.SessionID = f.receipt.Report.WorkerSessionID
			case "worker-native":
				parent.NativeSessionID = f.receipt.Report.NativeSessionID
			case "thread":
				parent.ProviderConversationID = "foreign-thread"
			case "scope":
				parent.ProviderScopeID = "foreign-scope"
			case "generation":
				if err := f.store.ClaimChatControllerGeneration(ctx, testSession, "new-controller-generation"); err != nil {
					t.Fatal(err)
				}
			case "stale-owner":
				work, found, err := f.store.GetWorkItem(ctx, f.receipt.Report.Identity.ID)
				if err != nil || !found {
					t.Fatalf("work readback: %v %v", found, err)
				}
				if _, err := f.store.ClaimWorkItemOwner(ctx, work, testSession); err != nil {
					t.Fatal(err)
				}
			case "lease-revision":
				if _, err := f.store.RenewWorkItemLease(ctx, f.receipt.Report.Identity, f.lease, f.lease.ExpiresAt.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			case "released-lease":
				if _, err := f.store.ReleaseWorkItemLease(ctx, f.receipt.Report.Identity, f.lease); err != nil {
					t.Fatal(err)
				}
			case "busy":
				// No report is allowed into the ordinary queue behind this prompt.
				f.provider.holdTurn = true
				if _, err := f.service.Send(ctx, testSession, ports.ChatUserMessage{Text: "busy unrelated turn", ClientMessageID: "unrelated"}); err != nil {
					t.Fatal(err)
				}
			case "handoff":
				if err := control.ArmHandoff(ctx, domain.SessionInterfaceTransitionDrain); err != nil {
					t.Fatal(err)
				}
			case "stopped":
				if err := f.service.Stop(ctx, testSession); err != nil {
					t.Fatal(err)
				}
			case "history-unavailable":
				f.provider.historyErr = ports.ErrChatHistoryUnavailable
			case "history-mismatch":
				if _, err := f.service.Send(ctx, testSession, ports.ChatUserMessage{Text: "prior parent context", ClientMessageID: "prior"}); err != nil {
					t.Fatal(err)
				}
				f.settle(t)
				f.provider.historyMu.Lock()
				f.provider.history = nil
				f.provider.historyMu.Unlock()
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before := f.provider.sendCallCount()
			d := f.engine()
			// Immutable-source substitution is tested at the adapter directly;
			// the engine itself always uses the validated SQLite parent reader.
			if change == "wrong-parent" || change == "worker-native" || change == "thread" || change == "scope" || change == "canceled" {
				err = f.service.WithLiveReportParent(ctx, parent, func(_ ports.ExactReportParent, chat ports.ExactReportChat) error {
					_, err := chat.SendReport(ctx, "wrong", "must never reach provider")
					return err
				})
			} else {
				err = d.Deliver(ctx, testProject, f.receipt.Report.ID)
			}
			if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || f.provider.sendCallCount() != before || f.resumes.Load() != 0 || f.starts.Load() != 1 {
				t.Fatalf("%s sent or escaped HOLD: %v", change, err)
			}
			if e := f.event(t); !e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() {
				t.Fatalf("refusal advanced report: %+v", e)
			}
		})
	}
}

func TestReportParentAdapterRestartLostACKAndResumeRefusal(t *testing.T) {
	f := newReportParentFixture(t)
	if err := f.engine().Deliver(t.Context(), testProject, f.receipt.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	f.settle(t)
	before := f.event(t)
	if err := f.service.Stop(t.Context(), testSession); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	f.store = st
	// A fresh service has no live owner. Outbox readback must prevent a second
	// send even after volatile controller state and an ACK response are lost.
	f.service = chatsvc.New(chatsvc.Options{Store: st, Sessions: st, Reader: fullSnapshotReader(st)})
	if err := f.engine().Deliver(t.Context(), testProject, f.receipt.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	if err := f.engine().Reconcile(t.Context(), testProject, f.receipt.Report.ID, "lost-ack"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	after := f.event(t)
	if after.LeaseToken != before.LeaseToken || after.DestinationSessionID != before.DestinationSessionID || after.AttemptCount != before.AttemptCount ||
		after.State != domain.OrchestrationSubmitted || !after.AcknowledgedAt.IsZero() || f.provider.sendCallCount() != 1 {
		t.Fatalf("restart retargeted/resubmitted/ACKed: %+v", after)
	}
	called := false
	err = f.service.WithResumedReportParent(t.Context(), f.parent, func(ports.ExactReportParent, ports.ExactReportChat) error { called = true; return nil })
	if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || !strings.Contains(err.Error(), "report_resume_generation_continuity_unavailable") || called {
		t.Fatal(err)
	}
	if f.starts.Load() != 1 || f.resumes.Load() != 0 {
		t.Fatal("Resume refusal started provider work")
	}
}

type reportAppendWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *reportAppendWaitContext) Err() error {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Err()
}

var errReportAppendWriterRollback = errors.New("roll back disposable contention writer")

type reportAppendContentionStore struct {
	*sqlite.Store
	entered      chan context.Context
	waiting      chan struct{}
	release      chan struct{}
	writerResult chan error
}

func (s *reportAppendContentionStore) AppendReservedUserMessage(ctx context.Context, owner domain.ConversationSendOwner, msg domain.ConversationMessage, now time.Time) (bool, error) {
	locked := make(chan struct{})
	// This public projection API holds the actual store writer in its callback.
	// Start it only at the append boundary, AFTER the engine's Prepare COMMIT.
	// Its transaction is rolled back, leaving no synthetic provider event.
	go func() {
		_, err := s.ProjectProviderEvent(context.WithoutCancel(ctx), owner.ConversationID, owner.SessionID,
			owner.ControllerGeneration, "w616-contention-writer", "fixture.writer", `{}`, now,
			func(context.Context) error {
				close(locked)
				<-s.release
				return errReportAppendWriterRollback
			})
		s.writerResult <- err
	}()
	select {
	case <-locked:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	s.entered <- ctx
	return s.Store.AppendReservedUserMessage(&reportAppendWaitContext{Context: ctx, waiting: s.waiting}, owner, msg, now)
}

func TestReportParentAppendWriterWaitReturnsBeforeRelease(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var probe *reportAppendContentionStore
			f := newReportParentFixture(t, func(st *sqlite.Store) chatsvc.Store {
				probe = &reportAppendContentionStore{Store: st, entered: make(chan context.Context, 1),
					waiting: make(chan struct{}), release: make(chan struct{}), writerResult: make(chan error, 1)}
				return probe
			})
			var inputReleases atomic.Int32
			f.service.SetInputAdmission(func(context.Context, domain.SessionID, string) (func(), error) {
				return func() { inputReleases.Add(1) }, nil
			})
			outer, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)
			consumed, writerConsumed := false, false
			var once sync.Once
			unlock := func() { once.Do(func() { close(probe.release) }) }
			defer func() {
				cancel()
				unlock()
				// Unblock an old bare Lock before reporting failure; join both
				// fixture goroutines even when a barrier/return check fails.
				if !consumed {
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Error("delivery caller did not exit after writer release")
					}
				}
				if !writerConsumed {
					select {
					case <-probe.writerResult:
					case <-time.After(time.Second):
						t.Error("disposable writer did not exit after release")
					}
				}
			}()
			go func() { result <- f.engine().Deliver(outer, testProject, f.receipt.Report.ID) }()
			var attemptCtx context.Context
			select {
			case attemptCtx = <-probe.entered:
			case err := <-result:
				consumed = true
				t.Fatalf("delivery did not reach prepared append boundary: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("append fixture did not acquire the store writer")
			}
			select {
			case <-probe.waiting:
			case <-time.After(time.Second):
				t.Fatal("report append did not enter cancellation-aware writer acquisition")
			}
			if err := attemptCtx.Err(); err != nil {
				t.Fatalf("attempt ended before writer contention was established: %v", err)
			}
			deadline, ok := attemptCtx.Deadline()
			if !ok || time.Until(deadline) > orchestrationevents.AttemptTimeout {
				t.Fatal("append escaped the engine's shared attempt deadline")
			}
			prepared := f.event(t)
			binding, err := ports.ParseExactReportMessage(prepared.LeaseToken)
			if err != nil || prepared.State != domain.OrchestrationLeased || prepared.AttemptCount != 1 || binding.Parent != f.parent {
				t.Fatalf("writer contention preceded successful Prepare: %+v %v", prepared, err)
			}
			wantErr := context.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				wantErr = context.Canceled
			}
			// Deadline mode uses the real shared engine timeout, with no outer
			// cancellation. The writer stays held throughout both modes.
			<-attemptCtx.Done()
			select {
			case err := <-result:
				consumed = true
				if !errors.Is(err, wantErr) || !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatalf("contended report: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("report callback retained parent gates beyond attempt cancellation/deadline")
			}
			if inputReleases.Load() != 1 || f.provider.sendCallCount() != 0 {
				t.Fatal("canceled append retained manager input admission or sent a provider turn")
			}
			// Re-enter the actual adapter while the writer is STILL held. This
			// verifies input/controller/send gates were released by the callback.
			checkCtx, checkCancel := context.WithTimeout(t.Context(), time.Second)
			called := false
			err = f.service.WithLiveReportParent(checkCtx, f.parent, func(ports.ExactReportParent, ports.ExactReportChat) error {
				called = true
				return nil
			})
			checkCancel()
			if err != nil || !called {
				t.Fatalf("parent gates retained after canceled writer acquisition: %v", err)
			}
			unlock()
			select {
			case err := <-probe.writerResult:
				writerConsumed = true
				if !errors.Is(err, errReportAppendWriterRollback) {
					t.Fatalf("fixture writer did not reach its rollback boundary: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("fixture writer did not release")
			}
			conversation, err := f.store.ConversationForSession(t.Context(), testSession)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := f.store.LoadConversationSnapshot(t.Context(), conversation.ID)
			if err != nil || len(rows.Messages) != 0 || len(rows.Turns) != 0 {
				t.Fatalf("canceled append admitted durable conversation rows: %+v %v", rows, err)
			}
			db, err := sql.Open("sqlite", filepath.Join(f.dir, "ao.db"))
			if err != nil {
				t.Fatal(err)
			}
			var attempts int
			queryErr := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM conversation_send_attempts WHERE conversation_id = ?`, conversation.ID).Scan(&attempts)
			if err := errors.Join(queryErr, db.Close()); err != nil || attempts != 0 {
				t.Fatalf("canceled append left pending/reserved intent: %d %v", attempts, err)
			}
			if _, err := f.store.NextQueuedTurn(t.Context(), conversation.ID); !errors.Is(err, domain.ErrNoQueuedTurn) || f.provider.sendCallCount() != 0 {
				t.Fatalf("canceled append became drainable or sent a provider turn: %v", err)
			}
		})
	}
}
