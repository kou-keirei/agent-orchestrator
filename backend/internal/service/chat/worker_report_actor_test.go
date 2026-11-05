package chat

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// This protocol fixture exercises the actual controller projection and issuer
// with a disposable DB. It does not invoke a provider or qualify public ingress.
type reportConversation struct{}

func (reportConversation) ProviderConversationID() string       { return "native-session" }
func (reportConversation) Capabilities() ports.ChatCapabilities { return ports.ChatCapabilities{} }
func (reportConversation) SendTurn(context.Context, ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	panic("report tests must not dispatch provider work")
}
func (reportConversation) Interrupt(context.Context, string) error { panic("unexpected interrupt") }
func (reportConversation) ResolveRequest(context.Context, string, ports.ChatDecision) error {
	panic("unexpected decision")
}
func (reportConversation) Events() <-chan ports.ChatEvent { panic("no provider stream") }
func (reportConversation) Close() error                   { return nil }

type reportLeaseReads struct {
	*sqlite.Store
	reads int
}

func (s *reportLeaseReads) ReadWorkerReportBinding(ctx context.Context, id domain.WorkItemID) (domain.WorkerReportActorBinding, bool, error) {
	s.reads++
	item, found, err := s.Store.GetWorkItem(ctx, id)
	if err != nil || !found {
		return domain.WorkerReportActorBinding{}, false, err
	}
	lease, found, err := s.Store.GetWorkItemLease(ctx, id)
	return domain.WorkerReportActorBinding{Identity: item.Identity, OriginalNativeSessionID: item.OriginalNativeSessionID, Lease: lease}, found, err
}

type reportSessionReads struct {
	SessionReader
	reads int
}

func (s *reportSessionReads) GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	s.reads++
	return s.SessionReader.GetSession(ctx, id)
}

type reportWrites struct {
	ports.WorkerReportStore
	writes int
	before func()
}

func (s *reportWrites) AcceptWorkerReport(ctx context.Context, r domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	s.writes++
	if s.before != nil {
		s.before()
	}
	return s.WorkerReportStore.AcceptWorkerReport(ctx, r)
}
func (*reportWrites) GetWorkerReport(context.Context, string) (domain.WorkerReportReceipt, bool, error) {
	panic("unverified receipt lookup")
}

type reportActorFixture struct {
	st       *sqlite.Store
	dir      string
	svc      *Service
	c        *Controller
	r        domain.WorkerReport
	b        domain.WorkerReportActorBinding
	leases   *reportLeaseReads
	sessions *reportSessionReads
	writes   *reportWrites
}

func newReportActorFixture(t *testing.T) *reportActorFixture {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	st, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	now := time.Now().UTC()
	if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: dir, RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	worker, err := st.CreateSession(ctx, domain.SessionRecord{ProjectID: "project", Kind: domain.KindWorker,
		Harness: domain.HarnessFake, Mode: domain.SessionModeChat, CreatedAt: now, UpdatedAt: now,
		Metadata: domain.SessionMetadata{ProviderConversationID: "native-session"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ClaimChatControllerGeneration(ctx, worker.ID, "controller-generation"); err != nil {
		t.Fatal(err)
	}
	conversation, err := st.CreateConversation(ctx, "conversation", domain.ConversationScopeSession, worker.ProjectID, worker.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.WorkItemIdentity{ID: "work", AttemptID: "attempt", ProjectID: worker.ProjectID, Repository: "repo",
		ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent", OriginalWorkerSessionID: worker.ID,
		OwnerSessionID: worker.ID, OwnerGeneration: 1, WorktreePath: dir, Branch: "branch"}
	admitted, err := st.CreateWorkItem(ctx, store.WorkItemRecord{Identity: identity, OriginalNativeSessionID: "native-session"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := st.AcquireWorkItemLease(ctx, admitted.Identity, domain.WorkLeaseGrant{ID: "lease", Duration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	sessions := &reportSessionReads{SessionReader: st}
	svc := New(Options{Store: st, Sessions: sessions})
	seq := 0
	c := newController(worker.ID, conversation, "controller-generation", domain.HarnessFake, ports.PermissionModeDefault,
		reportConversation{}, st, nil, svc.log, func() string { seq++; return fmt.Sprintf("id-%d", seq) }, svc.now, nil, nil)
	svc.controllers[worker.ID] = c
	sum := sha256.Sum256([]byte("explicit checkpoint"))
	r := domain.WorkerReport{ID: "report", Type: "checkpoint", Version: 1, Identity: admitted.Identity,
		LeaseID: lease.Grant.ID, LeaseRevision: lease.Revision, WorkerSessionID: worker.ID,
		NativeSessionID: "native-session", NativeTurnID: "native-turn", OriginalNativeSessionID: "native-session",
		Content: "explicit checkpoint", Digest: hex.EncodeToString(sum[:])}
	return &reportActorFixture{st: st, dir: dir, svc: svc, c: c, r: r,
		b:      domain.WorkerReportActorBinding{Identity: admitted.Identity, Lease: lease, OriginalNativeSessionID: admitted.OriginalNativeSessionID},
		leases: &reportLeaseReads{Store: st}, sessions: sessions, writes: &reportWrites{WorkerReportStore: st}}
}

func (f *reportActorFixture) start(t *testing.T) {
	t.Helper()
	projected, primary, err := f.c.projectEvent(t.Context(), ports.ChatEvent{Kind: ports.ChatEventTurnStarted,
		ProviderEventID: "native-start", ProviderTurnID: "scoped-turn", NativeTurnID: "native-turn", ProviderConversationID: "native-session"})
	if err != nil || !projected || !primary {
		t.Fatalf("native projection: %v %v %v", projected, primary, err)
	}
}

func (f *reportActorFixture) acceptance(t *testing.T) *reportactor.Acceptance {
	t.Helper()
	actor, origin, err := f.svc.BindWorkerReportActor(t.Context(), f.b, f.leases)
	if err != nil {
		t.Fatal(err)
	}
	if origin.SessionID != f.r.NativeSessionID || origin.TurnID != f.r.NativeTurnID {
		t.Fatalf("origin: %+v", origin)
	}
	a, err := reportactor.NewAcceptance(actor, f.writes)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *reportActorFixture) assertEvents(t *testing.T, count int) {
	t.Helper()
	events, err := f.st.ListOrchestrationEvents(t.Context(), f.r.Identity.ProjectID, 10)
	if err != nil || len(events) != count {
		t.Fatalf("outbox: %+v %v; want %d", events, err, count)
	}
	for _, event := range events {
		if event.State != domain.OrchestrationPending || !event.SubmittedAt.IsZero() || !event.AcknowledgedAt.IsZero() {
			t.Fatalf("acceptance advanced delivery/ACK: %+v", event)
		}
	}
}

func TestNativeReportActorPositiveInternalHandleAndRetry(t *testing.T) {
	f := newReportActorFixture(t)
	f.start(t)
	a := f.acceptance(t)
	// Mutating the issuer input after issuance cannot change the captured binding.
	f.b.Identity.OriginalParentSessionID = "altered"
	f.b.Lease.Revision++
	want, err := a.SubmitWorkerReport(t.Context(), f.r)
	if err != nil || want.EventID == "" {
		t.Fatalf("internal acceptance: %+v %v", want, err)
	}
	for range 2 {
		if got, err := a.SubmitWorkerReport(t.Context(), f.r); err != nil || got != want {
			t.Fatalf("retry: %+v %v", got, err)
		}
	}
	if f.leases.reads != 3 || f.sessions.reads != 3 || f.writes.writes != 3 {
		t.Fatalf("verification bypass: %+v %+v %+v", f.leases, f.sessions, f.writes)
	}
	f.assertEvents(t, 1)
}

func TestNativeReportActorRejectsClaimsBeforeAnyStoreRead(t *testing.T) {
	for name, mutate := range map[string]func(*domain.WorkerReport){
		"worker":                    func(r *domain.WorkerReport) { r.WorkerSessionID = "spoof" },
		"native-session":            func(r *domain.WorkerReport) { r.NativeSessionID = "spoof" },
		"native-turn":               func(r *domain.WorkerReport) { r.NativeTurnID = "spoof" },
		"scoped-turn-is-not-native": func(r *domain.WorkerReport) { r.NativeTurnID = "scoped-turn" },
		"work-item":                 func(r *domain.WorkerReport) { r.Identity.ID = "other" },
		"attempt":                   func(r *domain.WorkerReport) { r.Identity.AttemptID = "other" },
		"owner-generation":          func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ },
		"lease":                     func(r *domain.WorkerReport) { r.LeaseID = "other" },
		"lease-revision":            func(r *domain.WorkerReport) { r.LeaseRevision++ },
		"original-parent":           func(r *domain.WorkerReport) { r.Identity.OriginalParentSessionID = "other" },
		"original-native":           func(r *domain.WorkerReport) { r.OriginalNativeSessionID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newReportActorFixture(t)
			f.start(t)
			a := f.acceptance(t)
			if _, err := a.SubmitWorkerReport(t.Context(), f.r); err != nil {
				t.Fatal(err)
			}
			r := f.r
			mutate(&r)
			reads, sessions, writes := f.leases.reads, f.sessions.reads, f.writes.writes
			if receipt, err := a.SubmitWorkerReport(t.Context(), r); !errors.Is(err, domain.ErrWorkerReportActorMismatch) || receipt != (domain.WorkerReportReceipt{}) {
				t.Fatalf("spoof accepted: %+v %v", receipt, err)
			}
			if f.leases.reads != reads || f.sessions.reads != sessions || f.writes.writes != writes {
				t.Fatal("spoof reached storage")
			}
			f.assertEvents(t, 1)
		})
	}
}

func TestNativeReportActorRetiredHandleCannotReadReceipt(t *testing.T) {
	for _, change := range []string{"controller-replaced", "completed", "turn-replaced", "next-dispatch-before-native-start", "handoff", "stopped", "native-identity-reused"} {
		t.Run(change, func(t *testing.T) {
			f := newReportActorFixture(t)
			f.start(t)
			a := f.acceptance(t)
			if _, err := a.SubmitWorkerReport(t.Context(), f.r); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "controller-replaced":
				f.svc.controllers[f.r.WorkerSessionID] = &Controller{}
			case "completed", "native-identity-reused":
				if projected, _, err := f.c.projectEvent(t.Context(), ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderEventID: "complete", ProviderTurnID: "scoped-turn", ProviderConversationID: "native-session", TurnState: domain.TurnStateCompleted}); err != nil || !projected {
					t.Fatalf("complete: %v %v", projected, err)
				}
				if change == "native-identity-reused" {
					f.c.applyCommittedTurnLifecycle(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderTurnID: "scoped-turn", NativeTurnID: "native-turn", ProviderConversationID: "native-session"})
				}
			case "turn-replaced":
				f.c.reportTurn = &workerReportNativeTurn{session: "native-session", turn: "other"}
			case "next-dispatch-before-native-start":
				f.c.pendingTurnID = "new-scoped-turn"
			case "handoff":
				f.c.handoff = controllerHandoffInterfaceDrain
			case "stopped":
				f.c.state = ports.ChatControllerStopped
			}
			reads, sessions, writes := f.leases.reads, f.sessions.reads, f.writes.writes
			if receipt, err := a.SubmitWorkerReport(t.Context(), f.r); !errors.Is(err, domain.ErrWorkerReportActorMismatch) || receipt != (domain.WorkerReportReceipt{}) {
				t.Fatalf("retired handle read receipt: %+v %v", receipt, err)
			}
			if f.leases.reads != reads || f.sessions.reads != sessions || f.writes.writes != writes {
				t.Fatal("retired handle reached storage")
			}
			f.assertEvents(t, 1)
		})
	}
}

func TestNativeReportActorLeaseFreshnessIncludesAcceptedRetries(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		for _, change := range []string{"expired", "released", "renewed", "superseded", "owner-changed", "durable-controller-replaced"} {
			t.Run(fmt.Sprintf("%s/retry=%v", change, accepted), func(t *testing.T) {
				f := newReportActorFixture(t)
				f.start(t)
				a := f.acceptance(t)
				if accepted {
					if _, err := a.SubmitWorkerReport(t.Context(), f.r); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				switch change {
				case "expired":
					db, openErr := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(f.dir, "ao.db")))
					if openErr != nil {
						t.Fatal(openErr)
					}
					_, err = db.ExecContext(t.Context(), `UPDATE work_item_leases SET expires_at_ms=?`, time.Now().Add(-time.Second).UnixMilli())
					_ = db.Close()
				case "released":
					_, err = f.st.ReleaseWorkItemLease(t.Context(), f.b.Identity, f.b.Lease)
				case "renewed":
					_, err = f.st.RenewWorkItemLease(t.Context(), f.b.Identity, f.b.Lease, f.b.Lease.ExpiresAt.Add(time.Hour))
				case "superseded":
					_, err = f.st.SupersedeWorkItemLease(t.Context(), f.b.Identity, f.b.Lease, "new-owner", domain.WorkLeaseGrant{ID: "new-lease", Duration: time.Hour})
				case "owner-changed":
					_, err = f.st.ClaimWorkItemOwner(t.Context(), store.WorkItemRecord{Identity: f.b.Identity, OriginalNativeSessionID: f.b.OriginalNativeSessionID}, "new-owner")
				case "durable-controller-replaced":
					err = f.st.ClaimChatControllerGeneration(t.Context(), f.r.WorkerSessionID, "new-generation")
				}
				if err != nil {
					t.Fatal(err)
				}
				writes := f.writes.writes
				if receipt, err := a.SubmitWorkerReport(t.Context(), f.r); err == nil || receipt != (domain.WorkerReportReceipt{}) {
					t.Fatalf("stale authority accepted: %+v %v", receipt, err)
				}
				if f.writes.writes != writes {
					t.Fatal("stale lease reached report store")
				}
				count := 0
				if accepted {
					count = 1
				}
				f.assertEvents(t, count)
			})
		}
	}
}

func TestNativeReportActorRequiresCommittedNativeIdentity(t *testing.T) {
	for _, source := range []string{"idle", "generic-ack", "no-native-turn", "nested-thread", "uncommitted-generation"} {
		t.Run(source, func(t *testing.T) {
			f := newReportActorFixture(t)
			event := ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderEventID: "native-start", ProviderTurnID: "scoped-turn", NativeTurnID: "native-turn", ProviderConversationID: "native-session"}
			switch source {
			case "idle":
			case "generic-ack":
				f.c.pendingTurnID = "scoped-turn"
				f.c.ackedTurnID = "scoped-turn"
				f.c.state = ports.ChatControllerBusy
			case "no-native-turn":
				event.NativeTurnID = ""
				_, _, err := f.c.projectEvent(t.Context(), event)
				if err != nil {
					t.Fatal(err)
				}
			case "nested-thread":
				event.ProviderConversationID = "child"
				_, _, err := f.c.projectEvent(t.Context(), event)
				if err != nil {
					t.Fatal(err)
				}
			case "uncommitted-generation":
				if err := f.st.ClaimChatControllerGeneration(t.Context(), f.r.WorkerSessionID, "new-generation"); err != nil {
					t.Fatal(err)
				}
				projected, _, _ := f.c.projectEvent(t.Context(), event)
				if projected {
					t.Fatal("stale generation committed")
				}
			}
			actor, origin, err := f.svc.BindWorkerReportActor(t.Context(), f.b, f.leases)
			var block domain.WorkerReportNativeIngressBlock
			if !errors.As(err, &block) || !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || actor != nil || origin != (domain.WorkerReportNativeOrigin{}) {
				t.Fatalf("unproven origin issued: %+v %+v %v", actor, origin, err)
			}
			if f.leases.reads != 0 || f.sessions.reads != 0 || f.writes.writes != 0 {
				t.Fatal("unproven origin read storage")
			}
			f.assertEvents(t, 0)
		})
	}
}

func TestNativeReportActorHoldsExistingControllerGateThroughAcceptance(t *testing.T) {
	f := newReportActorFixture(t)
	f.start(t)
	a := f.acceptance(t)
	f.writes.before = func() {
		if f.svc.controllerGate(f.r.WorkerSessionID).tryLock() {
			f.svc.controllerGate(f.r.WorkerSessionID).unlock()
			t.Fatal("controller replacement was not excluded")
		}
		if f.c.sendMu.TryLock() {
			f.c.sendMu.Unlock()
			t.Fatal("turn retirement was not excluded")
		}
		// Ordinary provider projections take the SQLite writer and then c.mu.
		// Holding c.mu over report storage would invert those locks and deadlock.
		if !f.c.mu.TryLock() {
			t.Fatal("controller state mutex held across storage")
		}
		f.c.mu.Unlock()
		if projected, _, err := f.c.projectEvent(t.Context(), ports.ChatEvent{Kind: ports.ChatEventThreadState,
			ProviderEventID: "thread-update", ThreadState: &ports.ChatThreadState{Status: "active"}}); err != nil || !projected {
			t.Fatalf("ordinary projection blocked by report guard: %v %v", projected, err)
		}
	}
	if _, err := a.SubmitWorkerReport(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	if !f.svc.controllerGate(f.r.WorkerSessionID).tryLock() {
		t.Fatal("controller gate leaked")
	}
	f.svc.controllerGate(f.r.WorkerSessionID).unlock()
	if !f.c.sendMu.TryLock() {
		t.Fatal("turn mutex leaked")
	}
	f.c.sendMu.Unlock()
}

func TestNativeReportActorReleasesGuardAfterWriterFailureAndCancellation(t *testing.T) {
	f := newReportActorFixture(t)
	f.start(t)
	a := f.acceptance(t)
	invalid := f.r
	invalid.Digest = strings.Repeat("0", 64)
	if receipt, err := a.SubmitWorkerReport(t.Context(), invalid); !errors.Is(err, domain.ErrInvalidWorkerReport) || receipt != (domain.WorkerReportReceipt{}) {
		t.Fatalf("writer failure: %+v %v", receipt, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reads, sessions, writes := f.leases.reads, f.sessions.reads, f.writes.writes
	if receipt, err := a.SubmitWorkerReport(ctx, f.r); !errors.Is(err, context.Canceled) || receipt != (domain.WorkerReportReceipt{}) {
		t.Fatalf("cancelled invocation: %+v %v", receipt, err)
	}
	if f.leases.reads != reads || f.sessions.reads != sessions || f.writes.writes != writes {
		t.Fatal("cancelled invocation reached storage")
	}
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if receipt, err := a.SubmitWorkerReport(ctx, f.r); err != nil || receipt.EventID == "" {
		t.Fatalf("failed invocation leaked controller/turn guard: %+v %v", receipt, err)
	}
	f.assertEvents(t, 1)
}
