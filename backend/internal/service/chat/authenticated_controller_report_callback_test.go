package chat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// This is an explicit synthetic native source, not production provider proof.
type authoredReportConversation struct {
	reportConversation
	turn   string
	submit func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error)
	bind   func() error
}

// Retain every callback so a source cannot hide a duplicate by overwriting its
// own slot. The mutex also lets competing installations exercise the service.
type retainingReportConversation struct {
	reportConversation
	mu        sync.Mutex
	callbacks []func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error)
	bind      func(context.Context) error
}

func (c *retainingReportConversation) BindAuthenticatedControllerReportCallback(ctx context.Context, _ string,
	submit func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error),
) error {
	c.mu.Lock()
	c.callbacks = append(c.callbacks, submit)
	c.mu.Unlock()
	if c.bind != nil {
		return c.bind(ctx)
	}
	return nil
}

func TestAuthenticatedControllerReportCallbackRepeatedBinding(t *testing.T) {
	f := newReportActorFixture(t)
	source := &retainingReportConversation{}
	f.c.conv = source
	f.start(t)
	if err := f.connectAuthoredReport(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := f.connectAuthoredReport(t.Context()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
			t.Fatalf("repeated install was not held: %v", err)
		}
	}
	if len(source.callbacks) != 1 {
		t.Fatalf("source retained %d callbacks, want 1", len(source.callbacks))
	}
	want, err := source.callbacks[0](t.Context(), f.authoredContent())
	if err != nil || want.Report != f.r {
		t.Fatalf("original callback: %+v %v", want, err)
	}
	if got, err := source.callbacks[0](t.Context(), f.authoredContent()); err != nil || got != want {
		t.Fatalf("receipt retry after repeated install: %+v %v", got, err)
	}
	f.assertEvents(t, 1)
}

func TestAuthenticatedControllerReportCallbackConcurrentBinding(t *testing.T) {
	f := newReportActorFixture(t)
	source := &retainingReportConversation{}
	f.c.conv = source
	f.start(t)
	const attempts = 16
	start := make(chan struct{})
	results := make(chan error, attempts)
	for range attempts {
		go func() {
			<-start
			results <- f.connectAuthoredReport(t.Context())
		}()
	}
	close(start)
	successes := 0
	for range attempts {
		err := <-results
		if err == nil {
			successes++
		} else if !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
			t.Errorf("competing installation: %v", err)
		}
	}
	if successes != 1 || len(source.callbacks) != 1 {
		t.Fatalf("successful installs=%d retained callbacks=%d; want 1 each", successes, len(source.callbacks))
	}
	if got, err := source.callbacks[0](t.Context(), f.authoredContent()); err != nil || got.Report != f.r {
		t.Fatalf("winning callback: %+v %v", got, err)
	}
	f.assertEvents(t, 1)
}

func TestAuthenticatedControllerReportCallbackPendingBinding(t *testing.T) {
	f := newReportActorFixture(t)
	source := &retainingReportConversation{}
	f.c.conv = source
	f.start(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	source.bind = func(ctx context.Context) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var bindErr error
	go func() {
		defer close(done)
		bindErr = f.connectAuthoredReport(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	<-entered
	// Reentry during source I/O must be refused without waiting for that I/O or
	// handing out another callback. Cancellation bounds a broken implementation.
	retryCtx, cancelRetry := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelRetry()
	if err := f.connectAuthoredReport(retryCtx); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
		t.Fatalf("pending installation allowed reentry: %v", err)
	}
	source.mu.Lock()
	callback := source.callbacks[0]
	source.mu.Unlock()
	if got, err := callback(t.Context(), f.authoredContent()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got.EventID != "" {
		t.Fatalf("pending callback activated: %+v %v", got, err)
	}
	close(release)
	<-done
	if bindErr != nil {
		t.Fatalf("original installation: %v", bindErr)
	}
	if len(source.callbacks) != 1 {
		t.Fatalf("pending installation retained %d callbacks, want 1", len(source.callbacks))
	}
	if got, err := callback(t.Context(), f.authoredContent()); err != nil || got.Report != f.r {
		t.Fatalf("completed installation: %+v %v", got, err)
	}
	f.assertEvents(t, 1)
}

func TestAuthenticatedControllerReportCallbackFailedBindingCannotReinstall(t *testing.T) {
	for _, kind := range []string{"failed", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			f := newReportActorFixture(t)
			source := &retainingReportConversation{}
			f.c.conv = source
			f.start(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("retained callback before failure")
			source.bind = func(context.Context) error {
				if kind == "failed" {
					return failure
				}
				cancel()
				return nil
			}
			err := f.connectAuthoredReport(ctx)
			if kind == "failed" && !errors.Is(err, failure) || kind == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("first install: %v", err)
			}
			source.bind = nil
			if err := f.connectAuthoredReport(t.Context()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
				t.Fatalf("failed installation was retried: %v", err)
			}
			if len(source.callbacks) != 1 {
				t.Fatalf("failed install retained %d callbacks, want 1", len(source.callbacks))
			}
			if got, err := source.callbacks[0](t.Context(), f.authoredContent()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got.EventID != "" {
				t.Fatalf("failed callback activated: %+v %v", got, err)
			}
			f.assertEvents(t, 0)
		})
	}
}

func TestAuthenticatedControllerReportCallbackRetiredInstallation(t *testing.T) {
	for _, kind := range []string{"active", "pending"} {
		t.Run(kind, func(t *testing.T) {
			f := newReportActorFixture(t)
			source := &retainingReportConversation{}
			f.c.conv = source
			f.start(t)
			nextTurn := func() {
				f.c.sendMu.Lock()
				defer f.c.sendMu.Unlock()
				for _, event := range []ports.ChatEvent{
					{Kind: ports.ChatEventTurnCompleted, ProviderEventID: "done", ProviderTurnID: "scoped-turn",
						ProviderConversationID: "native-session", TurnState: domain.TurnStateCompleted},
					{Kind: ports.ChatEventTurnStarted, ProviderEventID: "next-start", ProviderTurnID: "next-scoped-turn",
						ProviderConversationID: "native-session", NativeTurnID: "next-native-turn"},
				} {
					if projected, primary, err := f.c.projectEvent(t.Context(), event); err != nil || !projected || !primary {
						t.Fatalf("next turn projection: %v %v %v", projected, primary, err)
					}
				}
			}
			if kind == "pending" {
				source.bind = func(context.Context) error {
					source.bind = nil
					nextTurn()
					if err := f.connectAuthoredReport(t.Context()); err != nil {
						t.Fatalf("new install while old source is pending: %v", err)
					}
					return nil
				}
				if err := f.connectAuthoredReport(t.Context()); !errors.Is(err, domain.ErrWorkerReportActorMismatch) {
					t.Fatalf("late retired install activated: %v", err)
				}
			} else {
				if err := f.connectAuthoredReport(t.Context()); err != nil {
					t.Fatal(err)
				}
				nextTurn()
				if err := f.connectAuthoredReport(t.Context()); err != nil {
					t.Fatalf("next turn install: %v", err)
				}
			}
			if len(source.callbacks) != 2 {
				t.Fatalf("retained %d callbacks, want one for each turn", len(source.callbacks))
			}
			reads, writes := f.leases.reads, f.writes.writes
			if got, err := source.callbacks[0](t.Context(), f.authoredContent()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got.EventID != "" {
				t.Fatalf("retired callback: %+v %v", got, err)
			}
			if f.leases.reads != reads || f.writes.writes != writes {
				t.Fatal("retired callback reached storage")
			}
			f.r.NativeTurnID = "next-native-turn"
			want, err := source.callbacks[1](t.Context(), f.authoredContent())
			if err != nil || want.Report != f.r {
				t.Fatalf("next turn callback: %+v %v", want, err)
			}
			if got, err := source.callbacks[1](t.Context(), f.authoredContent()); err != nil || got != want {
				t.Fatalf("next turn retry: %+v %v", got, err)
			}
			f.assertEvents(t, 1)
		})
	}
}

func (c *authoredReportConversation) BindAuthenticatedControllerReportCallback(_ context.Context, turn string,
	submit func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error),
) error {
	c.turn, c.submit = turn, submit
	if c.bind != nil {
		return c.bind()
	}
	return nil
}

func (f *reportActorFixture) connectAuthoredReport(ctx context.Context) error {
	return f.svc.BindAuthenticatedControllerReportCallback(ctx, f.b, f.leases,
		func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
			return reportactor.NewAcceptance(actor, f.writes)
		})
}

func (f *reportActorFixture) authoredContent() ports.AuthoredWorkerReportContent {
	return ports.AuthoredWorkerReportContent{ID: f.r.ID, Type: f.r.Type, Version: f.r.Version, Content: f.r.Content, Digest: f.r.Digest}
}

func TestAuthenticatedControllerReportCallbackNativeSourceAndRetry(t *testing.T) {
	f := newReportActorFixture(t)
	source := &authoredReportConversation{}
	f.c.conv = source
	f.start(t)
	if err := f.connectAuthoredReport(t.Context()); err != nil {
		t.Fatal(err)
	}
	if source.turn != "scoped-turn" || source.submit == nil {
		t.Fatalf("wrong native source binding: %+v", source)
	}
	want, err := source.submit(t.Context(), f.authoredContent())
	if err != nil || want.Report != f.r {
		t.Fatalf("native callback envelope: %+v %v", want, err)
	}
	for range 2 {
		if got, err := source.submit(t.Context(), f.authoredContent()); err != nil || got != want {
			t.Fatalf("callback retry: %+v %v", got, err)
		}
	}
	f.assertEvents(t, 1)
	// A later controller with the same native identifiers cannot inherit it.
	f.svc.controllers[f.r.WorkerSessionID] = &Controller{}
	reads, writes := f.leases.reads, f.writes.writes
	if got, err := source.submit(t.Context(), f.authoredContent()); !errors.Is(err, domain.ErrWorkerReportActorMismatch) || got.EventID != "" {
		t.Fatalf("replacement reused callback: %+v %v", got, err)
	}
	if f.leases.reads != reads || f.writes.writes != writes {
		t.Fatal("retired callback reached storage")
	}
}

func TestAuthenticatedControllerReportCallbackMissingNativeSourceHolds(t *testing.T) {
	f := newReportActorFixture(t)
	f.start(t)
	var hold domain.WorkerReportNativeIngressBlock
	if err := f.connectAuthoredReport(t.Context()); !errors.As(err, &hold) {
		t.Fatalf("unsupported provider did not HOLD: %v", err)
	}
	if f.writes.writes != 0 {
		t.Fatal("unsupported provider wrote report")
	}
	f.assertEvents(t, 0)
}

func TestCommittedReportToolCannotSelectLaterControllerOrTurn(t *testing.T) {
	for _, kind := range []string{"controller", "turn"} {
		t.Run(kind, func(t *testing.T) {
			f := newReportActorFixture(t)
			source := &authoredReportConversation{}
			f.c.conv = source
			f.start(t)
			expected, turn := f.c, "scoped-turn"
			if kind == "controller" {
				expected = &Controller{}
			} else {
				turn = "retired-turn"
			}
			err := f.svc.bindAuthenticatedControllerReportCallback(t.Context(), f.b, f.leases,
				func(actor ports.WorkerReportActorVerifier) (ports.WorkerReportSubmission, error) {
					return reportactor.NewAcceptance(actor, f.writes)
				}, expected, turn)
			if !errors.Is(err, domain.ErrWorkerReportActorMismatch) || source.submit != nil {
				t.Fatalf("retired projection installed callback: %v", err)
			}
			f.assertEvents(t, 0)
		})
	}
}

func TestAuthenticatedControllerReportCallbackIncompleteBindingNeverActivates(t *testing.T) {
	for _, kind := range []string{"uncommitted", "bind-failure", "cancelled", "controller-replaced", "turn-completed", "lease-released"} {
		t.Run(kind, func(t *testing.T) {
			f := newReportActorFixture(t)
			source := &authoredReportConversation{}
			f.c.conv = source
			if kind != "uncommitted" {
				f.start(t)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("native binding failed after retaining callback")
			source.bind = func() error {
				// Never hold controller locks while calling the source, and never
				// permit a source to submit before installation succeeds.
				if !f.svc.controllerGate(f.r.WorkerSessionID).tryLock() {
					t.Fatal("binding held controller gate")
				}
				f.svc.controllerGate(f.r.WorkerSessionID).unlock()
				if got, err := source.submit(t.Context(), f.authoredContent()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got.EventID != "" {
					t.Fatalf("premature callback: %+v %v", got, err)
				}
				switch kind {
				case "bind-failure":
					return failure
				case "cancelled":
					cancel()
				case "controller-replaced":
					f.svc.controllers[f.r.WorkerSessionID] = &Controller{}
				case "turn-completed":
					_, _, err := f.c.projectEvent(t.Context(), ports.ChatEvent{Kind: ports.ChatEventTurnCompleted,
						ProviderEventID: "done", ProviderTurnID: "scoped-turn", ProviderConversationID: "native-session", TurnState: domain.TurnStateCompleted})
					if err != nil {
						t.Fatal(err)
					}
				case "lease-released":
					if _, err := f.st.ReleaseWorkItemLease(t.Context(), f.b.Identity, f.b.Lease); err != nil {
						t.Fatal(err)
					}
				}
				return nil
			}
			err := f.connectAuthoredReport(ctx)
			if err == nil || kind == "bind-failure" && !errors.Is(err, failure) || kind == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("incomplete binding: %v", err)
			}
			if kind == "uncommitted" && source.submit != nil {
				t.Fatal("uncommitted turn reached native source")
			}
			if source.submit != nil {
				if got, err := source.submit(t.Context(), f.authoredContent()); !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) || got.EventID != "" {
					t.Fatalf("failed installation left callable authority: %+v %v", got, err)
				}
			}
			if f.writes.writes != 0 {
				t.Fatal("failed installation wrote report")
			}
			f.assertEvents(t, 0)
		})
	}
}
