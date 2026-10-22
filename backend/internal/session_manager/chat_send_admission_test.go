package sessionmanager

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// admissionChatLauncher models the existing service ordering, with fake restore
// and acceptance hooks. It never starts a controller, provider or native process.
type admissionChatLauncher struct {
	*recordingLauncher
	beforeAdmission func(context.Context) error
	afterAdmission  func(context.Context, string) error
}

func (l *admissionChatLauncher) RelayChatTurnWithAdmission(ctx context.Context, id domain.SessionID, text, key string, admit func() (func(), error)) (string, error) {
	if l.beforeAdmission != nil {
		if err := l.beforeAdmission(ctx); err != nil {
			return "", err
		}
	}
	release, err := admit()
	if err != nil {
		return "", err
	}
	defer release()
	if l.afterAdmission != nil {
		if err := l.afterAdmission(ctx, text); err != nil {
			return "", err
		}
	}
	if key != "" {
		return l.RelayChatTurnWithID(ctx, id, text, key)
	}
	return l.RelayChatTurn(ctx, id, text)
}
func admissionFixture() (*Manager, *fakeStore, *admissionChatLauncher) {
	m, st, _, _ := newManager()
	l := &admissionChatLauncher{recordingLauncher: &recordingLauncher{}}
	m.chat = l
	st.sessions["chat"] = domain.SessionRecord{ID: "chat", Mode: domain.SessionModeChat}
	return m, st, l
}
func TestW168PlainChatAdmissionRestoresBeforeLease(t *testing.T) {
	m, st, l := admissionFixture()
	calls := 0
	l.beforeAdmission = func(ctx context.Context) error {
		calls++
		if err := m.beginAgentResume(ctx, "chat"); err != nil {
			return err
		}
		defer m.endAgentResume("chat")
		rec := st.sessions["chat"]
		rec.Metadata.ControllerGeneration = "restored"
		st.sessions["chat"] = rec
		return nil
	}
	l.afterAdmission = func(_ context.Context, _ string) error {
		m.agentOpMu.Lock()
		n := m.inputLeases["chat"]
		m.agentOpMu.Unlock()
		if n != 1 {
			t.Errorf("acceptance input leases=%d, want exactly one", n)
		}
		if st.sessions["chat"].Metadata.ControllerGeneration != "restored" {
			t.Error("acceptance preceded restoration")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Send(ctx, "chat", "plain", nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(l.relayed) != 1 {
		t.Fatalf("restore calls=%d relay=%v", calls, l.relayed)
	}
	if err := m.beginAgentResume(ctx, "chat"); err != nil {
		t.Fatalf("lease leaked: %v", err)
	}
	m.endAgentResume("chat")
}
func TestW168PlainChatAdmissionRevalidatesAfterRestore(t *testing.T) {
	for _, which := range []string{"missing", "mode", "terminated", "read error", "cancel"} {
		t.Run(which, func(t *testing.T) {
			m, st, l := admissionFixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("read failed")
			want := ErrSwitchInProgress
			l.beforeAdmission = func(context.Context) error {
				rec := st.sessions["chat"]
				switch which {
				case "missing":
					delete(st.sessions, "chat")
					want = ErrNotFound
				case "mode":
					rec.Mode = domain.SessionModeTUI
					st.sessions["chat"] = rec
				case "terminated":
					rec.IsTerminated = true
					st.sessions["chat"] = rec
					want = ErrTerminated
				case "read error":
					st.getSessionErr = sentinel
					want = sentinel
				case "cancel":
					cancel()
					want = context.Canceled
				}
				return nil
			}
			err := m.Send(ctx, "chat", "refuse", nil)
			if !errors.Is(err, want) {
				t.Fatalf("err=%v, want %v", err, want)
			}
			if len(l.relayed) != 0 {
				t.Fatalf("refused input relayed: %v", l.relayed)
			}
			m.agentOpMu.Lock()
			n := len(m.inputLeases)
			m.agentOpMu.Unlock()
			if n != 0 {
				t.Fatal("refusal leaked input lease")
			}
		})
	}
}
func TestW168PlainChatAdmissionKeepsManualSemanticsAndID(t *testing.T) {
	for _, state := range []domain.ActivityState{domain.ActivityActive, domain.ActivityWaitingInput, domain.ActivityIdle} {
		t.Run(string(state), func(t *testing.T) {
			m, st, l := admissionFixture()
			rec := st.sessions["chat"]
			rec.Activity.State = state
			st.sessions["chat"] = rec
			for range 2 {
				if err := m.send(context.Background(), "chat", "retry", "original-id"); err != nil {
					t.Fatal(err)
				}
			}
			if len(l.relayed) != 2 || len(l.relayIDs) != 2 || l.relayIDs[0] != "original-id" || l.relayIDs[1] != "original-id" {
				t.Fatalf("double relay or changed key: %v %v", l.relayed, l.relayIDs)
			}
			// Manager forwards the same key once per request. Durable dedup/queueing
			// remain the service's responsibility, not a second manager ledger.
		})
	}
}
func TestW168PlainChatAdmissionReleaseOnRelayFailure(t *testing.T) {
	m, _, l := admissionFixture()
	sentinel := errors.New("relay failed")
	l.turnErr = sentinel
	if err := m.Send(context.Background(), "chat", "once", nil); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if len(l.relayed) != 1 {
		t.Fatalf("relay calls=%d", len(l.relayed))
	}
	if err := m.beginAgentResume(context.Background(), "chat"); err != nil {
		t.Fatal(err)
	}
	m.endAgentResume("chat")
}

// Done signals the exact wait point after beginAgentOperation closes intake.
type admissionDrainContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *admissionDrainContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
func TestW168PlainChatAdmissionDrainsBeforeMutation(t *testing.T) {
	m, st, l := admissionFixture()
	st.sessions["other"] = domain.SessionRecord{ID: "other", Mode: domain.SessionModeChat}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered := make(chan struct{})
	finish := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(finish) })
	l.afterAdmission = func(ctx context.Context, text string) error {
		if text == "first" {
			close(entered)
			select {
			case <-finish:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	sent := make(chan error, 1)
	go func() { sent <- m.Send(ctx, "chat", "first", nil) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	opCtx := &admissionDrainContext{Context: ctx, waiting: make(chan struct{})}
	begun := make(chan error, 1)
	go func() { begun <- m.beginAgentOperation(opCtx, "chat", agentOperationReconcile) }()
	select {
	case <-opCtx.waiting:
	case err := <-begun:
		t.Fatalf("operation did not drain input: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := m.Send(ctx, "chat", "late", nil); !errors.Is(err, ErrSwitchInProgress) {
		t.Fatalf("late send=%v", err)
	}
	if err := m.Send(ctx, "other", "unrelated", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-begun:
		t.Fatalf("operation completed early: %v", err)
	default:
	}
	once.Do(func() { close(finish) })
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-begun:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer m.endAgentOperation("chat", agentOperationReconcile)
	if len(l.relayed) != 2 {
		t.Fatalf("unexpected relay calls: %v", l.relayed)
	}
}

// Hiding the optional method models an unconfigured legacy embedder. Its old
// behavior is preserved and cannot be cited as positive idle exclusion proof.
type legacyAdmissionLauncher struct{ ChatLauncher }

func TestW168PlainChatAdmissionPreservesLegacyFallback(t *testing.T) {
	m, _, l := admissionFixture()
	m.chat = legacyAdmissionLauncher{l.recordingLauncher}
	if err := m.beginAgentOperation(context.Background(), "chat", agentOperationReconcile); err != nil {
		t.Fatal(err)
	}
	defer m.endAgentOperation("chat", agentOperationReconcile)
	if err := m.Send(context.Background(), "chat", "legacy", nil); err != nil {
		t.Fatal(err)
	}
	if len(l.relayed) != 1 {
		t.Fatalf("legacy behavior changed: %v", l.relayed)
	}
}

type admissionOwnerReadStore struct {
	*fakeStore
	active bool
	reads  int
	mutate func(*domain.SessionRecord)
}

func (s *admissionOwnerReadStore) GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	rec, found, err := s.fakeStore.GetSession(ctx, id)
	if s.active {
		s.reads++
		if s.reads == 2 {
			s.mutate(&rec)
		}
	}
	return rec, found, err
}
func TestW168PlainChatAdmissionRechecksOwnerAcrossLease(t *testing.T) {
	for _, which := range []string{"id", "harness", "runtime launch", "native session", "native launch", "conversation", "controller", "runtime handle", "workspace", "repo", "revision only"} {
		t.Run(which, func(t *testing.T) {
			m, st, l := admissionFixture()
			s := &admissionOwnerReadStore{fakeStore: st}
			m.store = s
			l.beforeAdmission = func(context.Context) error { s.active = true; return nil }
			s.mutate = func(rec *domain.SessionRecord) {
				switch which {
				case "id":
					rec.ID = "foreign"
				case "harness":
					rec.Harness = domain.HarnessCodex
				case "runtime launch":
					rec.Metadata.RuntimeLaunchID = "new-launch"
				case "native session":
					rec.Metadata.AgentSessionID = "new-native"
				case "native launch":
					rec.Metadata.AgentSessionIDLaunchID = "new-native-launch"
				case "conversation":
					rec.Metadata.ProviderConversationID = "other-thread"
				case "controller":
					rec.Metadata.ControllerGeneration = "replacement"
				case "runtime handle":
					rec.Metadata.RuntimeHandleID = "other-handle"
				case "workspace":
					rec.Metadata.WorkspacePath = "/other/workspace"
				case "repo":
					rec.Metadata.WorkspaceRepoPath = "/other/repo"
				case "revision only":
					rec.Revision++
					rec.Activity.State = domain.ActivityWaitingInput
				}
			}
			err := m.Send(context.Background(), "chat", "owner-bound", nil)
			if which == "revision only" {
				if err != nil || len(l.relayed) != 1 {
					t.Fatalf("revalidated manual input rejected: %v %v", err, l.relayed)
				}
				return
			}
			if !errors.Is(err, ErrSwitchInProgress) || len(l.relayed) != 0 {
				t.Fatalf("changed %s accepted: err=%v relay=%v", which, err, l.relayed)
			}
			m.agentOpMu.Lock()
			n := len(m.inputLeases)
			m.agentOpMu.Unlock()
			if n != 0 {
				t.Fatal("owner refusal leaked lease")
			}
		})
	}
}
