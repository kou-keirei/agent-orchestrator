package sessionmanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

const workSpawnBase = "f9db8771f96d9315af387011b152ac4a074929a9"

type workSpawnFixture struct {
	store   *sqlite.Store
	service *chatsvc.Service
	manager *Manager
	runtime *fakeRuntime
	ws      *fakeWorkspace
	parent  domain.SessionRecord
	auth    workItemSpawnAuthorization
	births  atomic.Int32
	dir     string
}

func newWorkSpawnFixture(t *testing.T) *workSpawnFixture {
	t.Helper()
	f := &workSpawnFixture{dir: t.TempDir()}
	f.store = sqlitetest.MustOpenAt(t, f.dir)
	if err := f.store.UpsertProject(t.Context(), domain.ProjectRecord{ID: "work-spawn", Path: f.dir,
		Config: testRoleAgents(), RegisteredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	var ids atomic.Int32
	f.service = chatsvc.New(chatsvc.Options{Store: f.store, Sessions: f.store,
		Drivers: integrationChatRegistry{domain.HarnessCodex: integrationChatDriver{harness: domain.HarnessCodex,
			start: func() ports.ChatConversation {
				return newIntegrationChatConversation(fmt.Sprintf("native-%d", f.births.Add(1)))
			}}},
		NewID: func() string { return fmt.Sprintf("work-spawn-%d", ids.Add(1)) }})
	t.Cleanup(func() { f.service.StopAll(context.Background()) })
	f.runtime = &fakeRuntime{}
	f.ws = &fakeWorkspace{}
	f.manager = f.newManager(f.store, f.runtime, f.ws)
	var err error
	f.parent, _, _, err = f.manager.Spawn(t.Context(), ports.SpawnConfig{ProjectID: "work-spawn",
		Kind: domain.KindOrchestrator, Harness: domain.HarnessCodex, RequestedMode: domain.SessionModeChat})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.UpsertSessionWorktree(t.Context(), domain.SessionWorktreeRecord{SessionID: f.parent.ID,
		RepoName: domain.RootWorkspaceRepoName, WorktreePath: f.parent.Metadata.WorkspacePath,
		Branch: f.parent.Metadata.Branch, BaseSHA: workSpawnBase, State: "active"}); err != nil {
		t.Fatal(err)
	}
	issuer, err := f.service.OriginalParentIssuer(f.parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Test-only trusted authorization. There is no production ID/base producer.
	f.auth = workItemSpawnAuthorization{work: "native-work", attempt: "native-attempt", project: "work-spawn",
		parent: f.parent.ID, base: workSpawnBase, issuer: issuer,
		grant: domain.WorkLeaseGrant{ID: "native-lease", Duration: time.Minute}}
	return f
}

func (f *workSpawnFixture) newManager(st Store, rt runtimeController, ws *fakeWorkspace) *Manager {
	return New(Deps{Store: st, Lifecycle: lifecycle.New(f.store, nil), Runtime: rt, Workspace: ws,
		Agents: fakeAgents{}, Messenger: &fakeMessenger{}, Chat: integrationChatLauncher{service: f.service},
		DataDir: f.dir, LookPath: func(string) (string, error) { return "test-agent", nil }})
}

func (f *workSpawnFixture) config(mode domain.SessionMode) ports.SpawnConfig {
	return ports.SpawnConfig{ProjectID: f.auth.project, ParentSessionID: f.auth.parent,
		Kind: domain.KindWorker, Harness: domain.HarnessCodex, RequestedMode: mode}
}

func TestWorkItemSpawnVerifiedChatAndTUIOneBirth(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		t.Run(string(mode), func(t *testing.T) {
			f := newWorkSpawnFixture(t)
			a := f.auth
			a.issuer = workSpawnIssuer{WorkItemParentIssuer: a.issuer, issue: func(ctx context.Context, i domain.WorkItemIdentity, native string, grant domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
				receipt, err := f.auth.issuer.(ports.WorkItemSpawnIssuer).IssueWorkItemSpawn(ctx, i, native, grant)
				if err == nil {
					f.requireAtomicReceipt(t, i, receipt)
					if f.births.Load() != 1 || f.runtime.created != 0 {
						t.Fatal("provider born before atomic admission")
					}
				}
				return receipt, err
			}}
			rec, err := f.manager.spawnWorkItem(t.Context(), f.config(mode), &a)
			if err != nil {
				t.Fatal(err)
			}
			work, found, err := f.store.GetWorkItem(t.Context(), f.auth.work)
			if err != nil || !found || work.Identity != f.auth.identity(rec.ID, workspaceInfo(rec)) || work.OriginalNativeSessionID != "" {
				t.Fatalf("work identity/readback: %+v found=%v err=%v", work, found, err)
			}
			target, err := f.store.GetWorkItemOriginalParent(t.Context(), f.auth.work, f.auth.attempt)
			if err != nil || target.OriginalParentSessionID != f.parent.ID ||
				target.ControllerGeneration != f.parent.Metadata.ControllerGeneration || target.NativeSessionID != f.parent.Metadata.ProviderConversationID {
				t.Fatalf("original live parent: %+v err=%v", target, err)
			}
			lease, found, err := f.store.GetWorkItemLease(t.Context(), f.auth.work)
			if err != nil || !found {
				t.Fatalf("lease: found=%v err=%v", found, err)
			}
			if _, err := f.store.ReserveWorkItemAdmission(t.Context(), work.Identity, lease); err != nil {
				t.Fatal(err)
			}
			// Reopening the database and constructing a new manager cannot turn an
			// exact receipt retry into another seed or provider launch.
			other, err := sqlite.Open(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			cold := f.newManager(other, f.runtime, &fakeWorkspace{})
			if _, err := cold.spawnWorkItem(t.Context(), f.config(mode), &f.auth); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
				t.Fatalf("cold retry: %v", err)
			}
			records, err := other.ListSessions(t.Context(), f.auth.project)
			if err != nil || len(records) != 2 {
				t.Fatalf("expected parent + one worker: %d err=%v", len(records), err)
			}
			wantChat, wantTUI := int32(1), 1
			if mode == domain.SessionModeChat {
				wantChat, wantTUI = 2, 0
			}
			if f.births.Load() != wantChat || f.runtime.created != wantTUI {
				t.Fatalf("births chat=%d tui=%d", f.births.Load(), f.runtime.created)
			}
			if events, err := f.store.ListOrchestrationEvents(t.Context(), f.auth.project, 10); err != nil || len(events) != 0 {
				t.Fatalf("issuance touched delivery: %+v err=%v", events, err)
			}
		})
	}
}

func TestWorkItemSpawnRefusesUnprovenAndForeignBeforeBirth(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		for _, kind := range []string{"nil", "no-issuer", "bare-store", "stale-parent", "missing-native", "foreign-parent", "foreign-project", "foreign-base", "no-base"} {
			t.Run(string(mode)+"/"+kind, func(t *testing.T) {
				f := newWorkSpawnFixture(t)
				a := f.auth
				cfg := f.config(mode)
				pa := &a
				switch kind {
				case "nil":
					pa = nil
				case "no-issuer":
					a.issuer = nil
				case "bare-store":
					a.issuer = f.store
				case "stale-parent":
					if err := f.store.ClaimChatControllerGeneration(t.Context(), f.parent.ID, "stale"); err != nil {
						t.Fatal(err)
					}
				case "missing-native":
					parent := f.parent
					parent.Metadata.ProviderConversationID = ""
					if err := f.store.UpdateSession(t.Context(), parent); err != nil {
						t.Fatal(err)
					}
				case "foreign-parent":
					a.parent, cfg.ParentSessionID = "foreign-parent", "foreign-parent"
				case "foreign-project":
					cfg.ProjectID = "foreign-project"
				case "foreign-base":
					cfg.ExecutionBaseSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				case "no-base":
					a.base = ""
				}
				if _, err := f.manager.spawnWorkItem(t.Context(), cfg, pa); err == nil {
					t.Fatal("unproven authorization launched")
				}
				if f.births.Load() != 1 || f.runtime.created != 0 {
					t.Fatalf("worker provider birth: chat=%d tui=%d", f.births.Load()-1, f.runtime.created)
				}
				if _, found, err := f.store.GetWorkItem(t.Context(), a.work); err != nil || found {
					t.Fatalf("rejected issuance created work: found=%v err=%v", found, err)
				}
			})
		}
	}
}

type workSpawnFaultStore struct {
	*sqlite.Store
	beforeReserve func(domain.WorkItemIdentity, domain.WorkItemLease)
	seedAllocator *sqlite.Store
}

func (s *workSpawnFaultStore) CreateSession(ctx context.Context, rec domain.SessionRecord) (domain.SessionRecord, error) {
	if s.seedAllocator != nil {
		return s.seedAllocator.CreateSession(ctx, rec)
	}
	return s.Store.CreateSession(ctx, rec)
}

func (s *workSpawnFaultStore) ReserveWorkItemAdmission(ctx context.Context, i domain.WorkItemIdentity, lease domain.WorkItemLease) (domain.WorkItemAdmission, error) {
	if s.beforeReserve != nil {
		s.beforeReserve(i, lease)
	}
	return s.Store.ReserveWorkItemAdmission(ctx, i, lease)
}

func TestWorkItemSpawnFreshOwnerAndLeaseFences(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		for _, kind := range []string{"owner", "generation", "lost-lease"} {
			t.Run(string(mode)+"/"+kind, func(t *testing.T) {
				f := newWorkSpawnFixture(t)
				st := &workSpawnFaultStore{Store: f.store}
				st.beforeReserve = func(i domain.WorkItemIdentity, lease domain.WorkItemLease) {
					if kind == "lost-lease" {
						if _, err := f.store.ReleaseWorkItemLease(t.Context(), i, lease); err != nil {
							t.Fatal(err)
						}
						return
					}
					work, _, err := f.store.GetWorkItem(t.Context(), i.ID)
					if err != nil {
						t.Fatal(err)
					}
					owner := i.OwnerSessionID
					if kind == "owner" {
						owner = "new-owner"
					}
					if _, err := f.store.ClaimWorkItemOwner(t.Context(), work, owner); err != nil {
						t.Fatal(err)
					}
				}
				f.manager.store = st
				if _, err := f.manager.spawnWorkItem(t.Context(), f.config(mode), &f.auth); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
					t.Fatalf("stale fence: %v", err)
				}
				if f.births.Load() != 1 || f.runtime.created != 0 || f.ws.destroyed != 0 {
					t.Fatal("stale work launched or reconciliation workspace removed")
				}
			})
		}
	}
}

func TestWorkItemSpawnCapacityCompetitorsCannotLaunch(t *testing.T) {
	f := newWorkSpawnFixture(t)
	if _, err := f.manager.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &f.auth); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for n := range 2 {
		// Independent SQLite opens exercise durable admission. Seed allocation
		// still uses AO's one daemon-owned allocator, as in the real manager.
		other, err := sqlite.Open(f.dir)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		rt := &fakeRuntime{}
		st := &workSpawnFaultStore{Store: other, seedAllocator: f.store}
		m := f.newManager(st, rt, &fakeWorkspace{path: filepath.Join(f.dir, fmt.Sprintf("competitor-%d", n))})
		a := f.auth
		a.work, a.attempt, a.grant.ID = domain.WorkItemID(fmt.Sprintf("competitor-%d", n)), domain.WorkAttemptID(fmt.Sprintf("attempt-%d", n)), fmt.Sprintf("lease-%d", n)
		wg.Go(func() {
			_, errs[n] = m.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &a)
			if rt.created != 0 {
				t.Error("capacity competitor launched")
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if !errors.Is(err, domain.ErrWorkAdmissionHeld) {
			t.Fatalf("capacity competitor: %v", err)
		}
	}
	if f.runtime.created != 1 || f.births.Load() != 1 {
		t.Fatal("unexpected provider births")
	}
}

type workSpawnIssuer struct {
	ports.WorkItemParentIssuer
	issue func(context.Context, domain.WorkItemIdentity, string, domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error)
}

func (f workSpawnIssuer) IssueWorkItemSpawn(ctx context.Context, i domain.WorkItemIdentity, native string, grant domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
	return f.issue(ctx, i, native, grant)
}

func TestWorkItemSpawnConcurrentSameAuthorizationOneBirth(t *testing.T) {
	f := newWorkSpawnFixture(t)
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	var seen sync.Map
	a := f.auth
	a.issuer = workSpawnIssuer{WorkItemParentIssuer: f.auth.issuer, issue: func(ctx context.Context, i domain.WorkItemIdentity, native string, grant domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
		if _, loaded := seen.LoadOrStore(i.OriginalWorkerSessionID, true); !loaded {
			ready <- struct{}{}
			<-start
		}
		return f.auth.issuer.(ports.WorkItemSpawnIssuer).IssueWorkItemSpawn(ctx, i, native, grant)
	}}
	results := make([]domain.SessionRecord, 2)
	errs := make([]error, 2)
	runtimes := []*fakeRuntime{{}, {}}
	var wg sync.WaitGroup
	for n := range 2 {
		// Share AO's seed allocator so BOTH workers materialize with distinct
		// IDs before contending at the real original-parent issuance boundary.
		m := f.newManager(f.store, runtimes[n], &fakeWorkspace{path: filepath.Join(f.dir, fmt.Sprintf("same-work-%d", n))})
		wg.Go(func() {
			results[n], errs[n] = m.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &a)
		})
	}
	<-ready
	<-ready
	close(start)
	wg.Wait()
	successes := 0
	var worker domain.SessionID
	for n, err := range errs {
		if err == nil {
			successes++
			worker = results[n].ID
		}
	}
	if successes != 1 || runtimes[0].created+runtimes[1].created != 1 {
		t.Fatalf("same authorization: successes=%d births=%d errors=%v", successes, runtimes[0].created+runtimes[1].created, errs)
	}
	work, found, err := f.store.GetWorkItem(t.Context(), f.auth.work)
	if err != nil || !found || work.Identity.OriginalWorkerSessionID != worker || work.Identity.AttemptID != f.auth.attempt {
		t.Fatalf("wrong authoritative birth: %+v found=%v err=%v", work, found, err)
	}
}

type workSpawnUncertainRuntime struct{ *fakeRuntime }

func (r workSpawnUncertainRuntime) Create(ctx context.Context, cfg ports.RuntimeConfig) (ports.RuntimeHandle, error) {
	_, err := r.fakeRuntime.Create(ctx, cfg)
	if err != nil {
		return ports.RuntimeHandle{}, err
	}
	return ports.RuntimeHandle{}, context.DeadlineExceeded
}

func TestWorkItemSpawnUncertainBirthRetainsCapacityAndNoRetryBirth(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		t.Run(string(mode), func(t *testing.T) {
			f := newWorkSpawnFixture(t)
			if mode == domain.SessionModeTUI {
				f.manager.runtime = workSpawnUncertainRuntime{f.runtime}
			} else {
				// Count provider birth, then lose its launch acknowledgement.
				f.manager.chat = &recordingLauncher{startErr: context.DeadlineExceeded, beforeStart: func(ChatStart) { f.births.Add(1) }}
			}
			if _, err := f.manager.spawnWorkItem(t.Context(), f.config(mode), &f.auth); !errors.Is(err, domain.ErrWorkAdmissionHeld) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("uncertain birth: %v", err)
			}
			if f.ws.destroyed != 0 || f.runtime.destroyed != 0 {
				t.Fatal("uncertain resources destroyed")
			}
			other, err := sqlite.Open(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			coldRT := &fakeRuntime{}
			cold := f.newManager(other, coldRT, &fakeWorkspace{})
			for range 2 {
				if _, err := cold.spawnWorkItem(t.Context(), f.config(mode), &f.auth); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
					t.Fatalf("uncertain retry: %v", err)
				}
			}
			work, _, err := other.GetWorkItem(t.Context(), f.auth.work)
			if err != nil {
				t.Fatal(err)
			}
			worker, _, err := other.GetSession(t.Context(), work.Identity.OriginalWorkerSessionID)
			if err != nil || worker.IsTerminated {
				t.Fatalf("lost durable HOLD seed: %+v err=%v", worker, err)
			}
			if err := cold.reconcileLive(t.Context(), worker); err != nil {
				t.Fatal(err)
			}
			if coldRT.created != 0 || (mode == domain.SessionModeTUI && f.runtime.created != 1) || (mode == domain.SessionModeChat && f.births.Load() != 2) {
				t.Fatal("retry/recovery created a second provider")
			}
			// A different work item also cannot consume the held slot.
			next := f.auth
			next.work, next.attempt, next.grant.ID = "next-work", "next-attempt", "next-lease"
			if _, err := cold.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &next); !errors.Is(err, domain.ErrWorkAdmissionHeld) || coldRT.created != 0 {
				t.Fatalf("uncertain capacity reclaimed: %v", err)
			}
		})
	}
}

func TestWorkItemSpawnCrashAfterBirthNeverReissues(t *testing.T) {
	f := newWorkSpawnFixture(t)
	f.manager.runtime = &spawnRecoveryCrashRuntime{fakeRuntime: f.runtime}
	func() {
		defer func() {
			if got := recover(); got != "spawn recovery crash after Create" {
				t.Fatalf("crash boundary: %v", got)
			}
		}()
		_, _ = f.manager.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &f.auth)
	}()
	other, err := sqlite.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	rt := &fakeRuntime{}
	cold := f.newManager(other, rt, &fakeWorkspace{})
	if _, err := cold.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &f.auth); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
		t.Fatalf("crash retry: %v", err)
	}
	work, _, err := other.GetWorkItem(t.Context(), f.auth.work)
	if err != nil {
		t.Fatal(err)
	}
	worker, _, err := other.GetSession(t.Context(), work.Identity.OriginalWorkerSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := cold.reconcileLive(t.Context(), worker); err != nil {
		t.Fatal(err)
	}
	if f.runtime.created != 1 || f.runtime.destroyed != 0 || rt.created != 0 || f.ws.destroyed != 0 {
		t.Fatal("crash created a second provider or destroyed uncertain resources")
	}
	lease, found, err := other.GetWorkItemLease(t.Context(), f.auth.work)
	if err != nil || !found {
		t.Fatalf("crash lost lease: found=%v err=%v", found, err)
	}
	if _, err := other.ReserveWorkItemAdmission(t.Context(), work.Identity, lease); err != nil {
		t.Fatalf("crash lost exact reservation: %v", err)
	}
}

func TestWorkItemSpawnCapturedParentCannotFollowReplacement(t *testing.T) {
	f := newWorkSpawnFixture(t)
	if _, err := f.manager.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &f.auth); err != nil {
		t.Fatal(err)
	}
	want, err := f.store.GetWorkItemOriginalParent(t.Context(), f.auth.work, f.auth.attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.service.Stop(t.Context(), f.parent.ID); err != nil {
		t.Fatal(err)
	}
	// Replace the live controller under the SAME AO parent session ID. The
	// retained issuer may not silently acquire this replacement's generation.
	if _, err := f.service.Start(t.Context(), chatsvc.StartConfig{SessionID: f.parent.ID,
		ProjectID: f.parent.ProjectID, Kind: domain.KindOrchestrator, Harness: domain.HarnessCodex,
		DataDir: f.dir, WorkspacePath: f.parent.Metadata.WorkspacePath,
		ControllerReady: func(started chatsvc.StartResult) (chatsvc.ControllerCommit, error) {
			metadata := f.parent.Metadata
			metadata.ProviderConversationID = started.ProviderConversationID
			metadata.ControllerGeneration = started.ControllerGeneration
			conversation, err := f.manager.markChatControllerSpawned(t.Context(), f.parent.ID, metadata,
				started.Conversation, started.ProviderBoundary, started.CommitProviderHistory, nil, started.LiveReconnect)
			return chatsvc.ControllerCommit{Conversation: conversation}, err
		}}); err != nil {
		t.Fatal(err)
	}
	next := f.auth
	next.work, next.attempt, next.grant.ID = "replacement-work", "replacement-attempt", "replacement-lease"
	if _, err := f.manager.spawnWorkItem(t.Context(), f.config(domain.SessionModeTUI), &next); !errors.Is(err, domain.ErrWorkItemParentHeld) {
		t.Fatalf("captured issuer followed replacement: %v", err)
	}
	got, err := f.store.GetWorkItemOriginalParent(t.Context(), f.auth.work, f.auth.attempt)
	if err != nil || got != want || f.runtime.created != 1 {
		t.Fatalf("original parent retargeted: %+v err=%v births=%d", got, err, f.runtime.created)
	}
}

// Inspect actual persisted rows through an independent reader, without reserving
// capacity as part of the assertion. This must already hold before provider birth.
func (f *workSpawnFixture) requireAtomicReceipt(t *testing.T, i domain.WorkItemIdentity, receipt ports.WorkItemSpawnReceipt) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(f.dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"work_items", "work_item_parents", "work_item_leases", "work_item_admissions"} {
		var count int
		if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
	work, found, err := f.store.GetWorkItem(t.Context(), i.ID)
	if err != nil || !found || work.Identity != i {
		t.Fatalf("identity: %+v found=%v err=%v", work, found, err)
	}
	parent, err := f.store.GetWorkItemOriginalParent(t.Context(), i.ID, i.AttemptID)
	if err != nil || parent != receipt.Parent {
		t.Fatalf("parent: %+v err=%v", parent, err)
	}
	lease, found, err := f.store.GetWorkItemLease(t.Context(), i.ID)
	if err != nil || !found || lease.Revision != receipt.Lease.Revision || !lease.ExpiresAt.Equal(receipt.Lease.ExpiresAt) || !sameSpawnWorkGrant(lease.Grant, receipt.Lease.Grant) {
		t.Fatalf("lease: %+v found=%v err=%v", lease, found, err)
	}
	var a domain.WorkItemAdmission
	err = db.QueryRowContext(t.Context(), `SELECT work_item_id, attempt_id, owner_session_id, owner_generation, lease_id, lease_revision, state FROM work_item_admissions`).Scan(&a.WorkItemID, &a.AttemptID, &a.OwnerSessionID, &a.OwnerGeneration, &a.LeaseID, &a.LeaseRevision, &a.State)
	if err != nil || a != receipt.Admission || a.State != domain.WorkAdmissionReserved {
		t.Fatalf("admission: %+v err=%v", a, err)
	}
}

func TestWorkItemSpawnIssuerCommitLostACKBlocksConcurrentIdentity(t *testing.T) {
	for _, mode := range []domain.SessionMode{domain.SessionModeChat, domain.SessionModeTUI} {
		t.Run(string(mode), func(t *testing.T) {
			f := newWorkSpawnFixture(t)
			committed := make(chan struct{})
			dropACK := make(chan struct{})
			defer close(dropACK)
			var identity domain.WorkItemIdentity
			var receipt ports.WorkItemSpawnReceipt
			a := f.auth
			a.issuer = workSpawnIssuer{WorkItemParentIssuer: a.issuer, issue: func(ctx context.Context, i domain.WorkItemIdentity, native string, grant domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
				got, err := f.auth.issuer.(ports.WorkItemSpawnIssuer).IssueWorkItemSpawn(ctx, i, native, grant)
				if err != nil {
					return got, err
				}
				identity, receipt = i, got
				close(committed) // Real controller-bound issuer and SQLite commit succeeded.
				select {
				case <-dropACK:
				case <-ctx.Done():
				}
				return ports.WorkItemSpawnReceipt{}, context.DeadlineExceeded
			}}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := f.manager.spawnWorkItem(ctx, f.config(mode), &a); result <- err }()
			select {
			case <-committed:
			case err := <-result:
				t.Fatalf("no positive commit: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			f.requireAtomicReceipt(t, identity, receipt)
			if f.births.Load() != 1 || f.runtime.created != 0 {
				t.Fatal("provider born before issuer ACK")
			}
			other, err := sqlite.Open(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			rt := &fakeRuntime{}
			cold := f.newManager(&workSpawnFaultStore{Store: other, seedAllocator: f.store}, rt, &fakeWorkspace{path: filepath.Join(f.dir, "competitor")})
			next := f.auth
			next.work, next.attempt, next.grant.ID = "lost-ack-competitor", "competitor-attempt", "competitor-lease"
			// The first request is still awaiting the issuer ACK as the second requests
			// another work identity through a separate manager/database handle.
			if _, err := cold.spawnWorkItem(ctx, f.config(mode), &next); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
				t.Fatalf("competitor: %v", err)
			}
			cancel() // Drop the first ACK after its successful commit.
			if err := <-result; !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, domain.ErrWorkAdmissionHeld) {
				t.Fatalf("lost ACK: %v", err)
			}
			for range 2 {
				if _, err := cold.spawnWorkItem(t.Context(), f.config(mode), &a); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
					t.Fatalf("retry: %v", err)
				}
				if _, err := cold.spawnWorkItem(t.Context(), f.config(mode), &next); !errors.Is(err, domain.ErrWorkAdmissionHeld) {
					t.Fatalf("capacity after timeout: %v", err)
				}
			}
			f.requireAtomicReceipt(t, identity, receipt)
			worker, found, err := other.GetSession(t.Context(), identity.OriginalWorkerSessionID)
			if err != nil || !found || worker.IsTerminated || f.ws.destroyed != 0 || f.runtime.destroyed != 0 {
				t.Fatalf("lost held seed: %+v found=%v err=%v", worker, found, err)
			}
			if f.births.Load() != 1 || f.runtime.created != 0 || rt.created != 0 {
				t.Fatal("lost ACK or competitor created a worker provider")
			}
		})
	}
}
