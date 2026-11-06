package chat

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type retirementConversation struct {
	reportConversation
	events chan ports.ChatEvent
	once   sync.Once
	acks   int
}

func (c *retirementConversation) Events() <-chan ports.ChatEvent { return c.events }
func (c *retirementConversation) Close() error {
	c.once.Do(func() { close(c.events) })
	return nil
}
func (c *retirementConversation) AcknowledgeProviderEvent(context.Context, string) error {
	c.acks++
	return nil
}

type retirementStore struct {
	Store
	ports.WorkItemParentPersistence
	failProjection bool
	projected      chan struct{}
	parentWrites   int
	beforeParent   func()
}

func (s *retirementStore) ProjectProviderEvent(ctx context.Context, conversationID string, session domain.SessionID,
	generation, eventID, method, payload string, now time.Time, project func(context.Context) error,
) (bool, error) {
	if s.failProjection {
		return false, errors.New("injected persistent projection failure")
	}
	committed, err := s.Store.ProjectProviderEvent(ctx, conversationID, session, generation, eventID, method, payload, now, project)
	if s.projected != nil && committed && err == nil {
		s.projected <- struct{}{}
	}
	return committed, err
}

func (s *retirementStore) PersistWorkItemOriginalParent(ctx context.Context, i domain.WorkItemIdentity,
	workerNativeID string, target domain.WorkItemParentTarget,
) (domain.WorkItemParentTarget, error) {
	s.parentWrites++
	if s.beforeParent != nil {
		s.beforeParent()
	}
	return s.WorkItemParentPersistence.PersistWorkItemOriginalParent(ctx, i, workerNativeID, target)
}

func retirementWait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("retirement regression deadlocked")
		var zero T
		return zero
	}
}

// Mutex waits are not durable blocking in testing/synctest (Go 1.25). Observe
// the projector's actual mutex wait after the channel delivery barrier instead
// of treating a sleep or an empty channel as proof that retirement tried to run.
// The deadline is only a deadlock watchdog, never a successful absence check.
func retirementWaitForDispatchLock(t *testing.T, c *Controller) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	stack := make([]byte, 256<<10)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		stopped := c.state == ports.ChatControllerStopped
		c.mu.Unlock()
		if stopped {
			t.Fatal("projector retired while guarded storage was still blocked")
		}
		n := runtime.Stack(stack, true)
		if n == len(stack) {
			t.Fatal("goroutine stack exceeded bounded regression buffer")
		}
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, fmt.Sprintf("(*Controller).project(%p)", c)) &&
				// Go 1.25's sync.Mutex wraps internal/sync.Mutex at offset zero.
				// Match this exact mutex, never a transient wait for c.mu or SQLite.
				strings.Contains(goroutine, fmt.Sprintf("internal/sync.(*Mutex).lockSlow(%p)", &c.sendMu)) &&
				!strings.Contains(goroutine, "(*Controller).projectEvent(") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("projector never reached retirement dispatch lock")
}

func retirementAssertUnlocked(t *testing.T, f *reportActorFixture) {
	t.Helper()
	gate := f.svc.controllerGate(f.c.sessionID)
	if !gate.tryLock() {
		t.Fatal("controller gate leaked")
	}
	gate.unlock()
	if !f.c.sendMu.TryLock() {
		t.Fatal("dispatch mutex leaked")
	}
	f.c.sendMu.Unlock()
}

func retirementStart(t *testing.T, f *reportActorFixture, mode string) (*retirementConversation, *retirementStore, func()) {
	t.Helper()
	conv := &retirementConversation{events: make(chan ports.ChatEvent)}
	st := &retirementStore{Store: f.st, WorkItemParentPersistence: f.st, failProjection: mode == "projection-failure"}
	f.c.conv, f.c.store, f.svc.store = conv, st, st
	go f.c.project()
	t.Cleanup(func() {
		_ = conv.Close()
		retirementWait(t, f.c.stopped)
	})
	trigger := func() {
		switch mode {
		case "stopped-event":
			conv.events <- ports.ChatEvent{Kind: ports.ChatEventControllerState, ControllerState: ports.ChatControllerStopped}
		case "stream-close":
			_ = conv.Close()
		case "projection-failure":
			// A non-lifecycle event must not acquire sendMu for ordinary projection,
			// but exhaustion of its persistent retries must acquire it to retire.
			conv.events <- ports.ChatEvent{Kind: ports.ChatEventThreadState}
		default:
			t.Fatalf("unknown retirement mode %q", mode)
		}
	}
	return conv, st, trigger
}

func retirementFinish(t *testing.T, f *reportActorFixture, conv *retirementConversation) {
	t.Helper()
	_ = conv.Close()
	retirementWait(t, f.c.stopped)
	f.c.mu.Lock()
	stopped := f.c.state == ports.ChatControllerStopped
	f.c.mu.Unlock()
	if !stopped {
		t.Fatal("projector exited without retiring")
	}
	if conv.acks != 0 {
		t.Fatal("retirement manufactured a provider acknowledgement")
	}
	retirementAssertUnlocked(t, f)
}

func TestControllerRetirementSerializesReportStorage(t *testing.T) {
	for _, mode := range []string{"stopped-event", "stream-close", "projection-failure"} {
		for _, retry := range []bool{false, true} {
			for _, operationFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/retry=%v/accept-first=%v", mode, retry, operationFirst), func(t *testing.T) {
					f := newReportActorFixture(t)
					f.start(t)
					a := f.acceptance(t)
					var prior domain.WorkerReportReceipt
					if retry {
						var err error
						prior, err = a.SubmitWorkerReport(t.Context(), f.r)
						if err != nil {
							t.Fatal(err)
						}
					}
					conv, _, trigger := retirementStart(t, f, mode)
					writes := f.writes.writes
					if operationFirst {
						entered, release := make(chan struct{}), make(chan struct{})
						f.writes.before = func() { close(entered); <-release }
						type result struct {
							receipt domain.WorkerReportReceipt
							err     error
						}
						done := make(chan result, 1)
						finished := make(chan struct{})
						defer func() { close(release); retirementWait(t, finished) }()
						go func() {
							defer close(finished)
							r, err := a.SubmitWorkerReport(t.Context(), f.r)
							done <- result{r, err}
						}()
						retirementWait(t, entered)
						trigger()
						retirementWaitForDispatchLock(t, f.c)
						if !f.c.mu.TryLock() {
							t.Fatal("state mutex held across guarded storage")
						}
						f.c.mu.Unlock()
						release <- struct{}{}
						got := retirementWait(t, done)
						if got.err != nil || got.receipt.EventID == "" || (retry && got.receipt != prior) {
							t.Fatalf("accept-first: %+v %v", got.receipt, got.err)
						}
						f.writes.before = nil
						retirementFinish(t, f, conv)
						writes++
					} else {
						trigger()
						retirementFinish(t, f, conv)
					}
					reads, sessions := f.leases.reads, f.sessions.reads
					got, err := a.SubmitWorkerReport(t.Context(), f.r)
					if !errors.Is(err, domain.ErrWorkerReportActorMismatch) || got != (domain.WorkerReportReceipt{}) {
						t.Fatalf("retired actor exposed receipt: %+v %v", got, err)
					}
					if f.writes.writes != writes || f.leases.reads != reads || f.sessions.reads != sessions {
						t.Fatal("retired actor reached storage")
					}
					count := 0
					if retry || operationFirst {
						count = 1
					}
					f.assertEvents(t, count)
					retirementAssertUnlocked(t, f)
				})
			}
		}
	}
}

func retirementParentIdentity(t *testing.T, f *reportActorFixture) domain.WorkItemIdentity {
	t.Helper()
	worker, err := f.st.CreateSession(t.Context(), domain.SessionRecord{ProjectID: f.r.Identity.ProjectID,
		Kind: domain.KindWorker, Harness: domain.HarnessFake, Mode: domain.SessionModeChat,
		CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	i := f.r.Identity
	i.ID, i.AttemptID = "issued-child", "issued-child-attempt"
	i.OriginalParentSessionID = f.c.sessionID
	i.OriginalWorkerSessionID, i.OwnerSessionID = worker.ID, worker.ID
	for _, id := range []domain.SessionID{f.c.sessionID, worker.ID} {
		if err := f.st.UpsertSessionWorktree(t.Context(), domain.SessionWorktreeRecord{SessionID: id,
			RepoName: i.Repository, WorktreePath: i.WorktreePath, Branch: i.Branch, BaseSHA: i.ExecutionBaseSHA}); err != nil {
			t.Fatal(err)
		}
	}
	return i
}

func TestControllerRetirementSerializesOriginalParentPersistence(t *testing.T) {
	for _, mode := range []string{"stopped-event", "stream-close", "projection-failure"} {
		for _, operationFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/issue-first=%v", mode, operationFirst), func(t *testing.T) {
				f := newReportActorFixture(t)
				i := retirementParentIdentity(t, f)
				issuer, err := f.svc.OriginalParentIssuer(f.c.sessionID)
				if err != nil {
					t.Fatal(err)
				}
				conv, st, trigger := retirementStart(t, f, mode)
				if operationFirst {
					entered, release := make(chan struct{}), make(chan struct{})
					st.beforeParent = func() { close(entered); <-release }
					type result struct {
						target domain.WorkItemParentTarget
						err    error
					}
					done := make(chan result, 1)
					finished := make(chan struct{})
					defer func() { close(release); retirementWait(t, finished) }()
					go func() {
						defer close(finished)
						target, err := issuer.IssueWorkItemFromOriginalParent(t.Context(), i, "distinct-worker-native")
						done <- result{target, err}
					}()
					retirementWait(t, entered)
					trigger()
					retirementWaitForDispatchLock(t, f.c)
					if !f.c.mu.TryLock() {
						t.Fatal("state mutex held across parent persistence")
					}
					f.c.mu.Unlock()
					release <- struct{}{}
					got := retirementWait(t, done)
					if got.err != nil || got.target.NativeSessionID != "native-session" ||
						got.target.NativeSessionID == "distinct-worker-native" {
						t.Fatalf("issue-first: %+v %v", got.target, got.err)
					}
					if target, err := f.st.GetWorkItemOriginalParent(t.Context(), i.ID, i.AttemptID); err != nil || target != got.target {
						t.Fatalf("immutable parent readback: %+v %v", target, err)
					}
					work, found, err := f.st.GetWorkItem(t.Context(), i.ID)
					if err != nil || !found || work.OriginalNativeSessionID != "distinct-worker-native" {
						t.Fatalf("worker identity lost: %+v %v %v", work, found, err)
					}
					st.beforeParent = nil
					retirementFinish(t, f, conv)
				} else {
					trigger()
					retirementFinish(t, f, conv)
				}
				writes := st.parentWrites
				if target, err := issuer.IssueWorkItemFromOriginalParent(t.Context(), i, "distinct-worker-native"); !errors.Is(err, domain.ErrWorkItemParentHeld) || target != (domain.WorkItemParentTarget{}) {
					t.Fatalf("retired parent issued work: %+v %v", target, err)
				}
				if st.parentWrites != writes || writes != map[bool]int{false: 0, true: 1}[operationFirst] {
					t.Fatal("retired parent reached persistence")
				}
				if _, found, err := f.st.GetWorkItem(t.Context(), i.ID); err != nil || found != operationFirst {
					t.Fatalf("unexpected WorkItem effects: found=%v err=%v", found, err)
				}
				f.assertEvents(t, 0)
				retirementAssertUnlocked(t, f)
			})
		}
	}
}

func TestControllerRetirementLeavesOrdinaryProjectionAvailable(t *testing.T) {
	f := newReportActorFixture(t)
	f.start(t)
	a := f.acceptance(t)
	conv, st, _ := retirementStart(t, f, "stream-close")
	st.projected = make(chan struct{})
	f.writes.before = func() {
		conv.events <- ports.ChatEvent{Kind: ports.ChatEventThreadState,
			ThreadState: &ports.ChatThreadState{Status: "active"}}
		retirementWait(t, st.projected)
	}
	if _, err := a.SubmitWorkerReport(t.Context(), f.r); err != nil {
		t.Fatal(err)
	}
	retirementFinish(t, f, conv)
}

func TestControllerRetirementDoesNotTakeStopGate(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop-all=%v", all), func(t *testing.T) {
			f := newReportActorFixture(t)
			conv, _, _ := retirementStart(t, f, "stream-close")
			done := make(chan error, 1)
			go func() {
				if all {
					f.svc.StopAll(t.Context())
					done <- nil
				} else {
					done <- f.svc.Stop(t.Context(), f.c.sessionID)
				}
			}()
			if err := retirementWait(t, done); err != nil {
				t.Fatal(err)
			}
			retirementFinish(t, f, conv)
			if _, err := f.svc.Controller(f.c.sessionID); !errors.Is(err, ErrNoController) {
				t.Fatalf("Stop retained retired controller: %v", err)
			}
		})
	}
}
