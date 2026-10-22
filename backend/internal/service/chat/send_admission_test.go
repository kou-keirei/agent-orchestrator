package chat_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type w168SessionReader struct {
	*sqlite.Store
	mutate func(*domain.SessionRecord)
}

func (r *w168SessionReader) GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	rec, found, err := r.Store.GetSession(ctx, id)
	if r.mutate != nil {
		r.mutate(&rec)
	}
	return rec, found, err
}

func w168AdmissionFixture(t *testing.T) (*chatsvc.Service, *fakeConversation, *sqlite.Store, *w168SessionReader, *chatsvc.Controller) {
	t.Helper()
	st := openStore(t)
	reader := &w168SessionReader{Store: st}
	provider := newFakeConversation()
	var seq atomic.Int64
	svc := chatsvc.New(chatsvc.Options{
		Store: st, Sessions: reader, Reader: fullSnapshotReader(st),
		Drivers: fakeRegistry{driver: fakeDriver{conv: provider}},
		NewID:   func() string { return fmt.Sprintf("w168-%d", seq.Add(1)) },
	})
	workspace := t.TempDir()
	ctrl, err := svc.Start(context.Background(), chatsvc.StartConfig{
		SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex, WorkspacePath: workspace,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(context.Background(), testSession) })
	if err := lifecycle.New(st, nil).MarkSpawned(context.Background(), testSession, domain.SessionMetadata{
		WorkspacePath: workspace, ControllerGeneration: ctrl.Generation(), ProviderConversationID: ctrl.ProviderConversationID(),
	}); err != nil {
		t.Fatal(err)
	}
	return svc, provider, st, reader, ctrl
}

func w168Router(svc *chatsvc.Service) http.Handler {
	r := chi.NewRouter()
	(&controllers.ConversationsController{Svc: svc}).Register(r)
	return r
}

func TestW168HTTPAdmissionRejectsStaleSelectionWithoutDispatch(t *testing.T) {
	for _, which := range []string{"owner", "workspace", "repo", "generation", "runtime", "terminated", "cancel", "refused", "already stale"} {
		t.Run(which, func(t *testing.T) {
			svc, provider, st, reader, ctrl := w168AdmissionFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			acquired, released := 0, 0
			mutate := func(rec *domain.SessionRecord) {
				switch which {
				case "owner":
					rec.Harness = domain.HarnessClaudeCode
				case "workspace":
					rec.Metadata.WorkspacePath = "other-workspace"
				case "repo":
					rec.Metadata.WorkspaceRepoPath = "other-repo"
				case "generation", "already stale":
					rec.Metadata.ControllerGeneration = "replacement"
				case "runtime":
					rec.Metadata.RuntimeHandleID = "replacement-runtime"
				case "terminated":
					rec.IsTerminated = true
				}
			}
			if which == "already stale" {
				reader.mutate = mutate
			}
			svc.SetSessionInputAdmission(func(domain.SessionID) (func(), bool) {
				if which == "refused" {
					return nil, false
				}
				acquired++
				reader.mutate = mutate
				if which == "cancel" {
					cancel()
				}
				return func() { released++ }, true
			})
			request := httptest.NewRequest(http.MethodPost, "/sessions/p1-1/conversation/messages", strings.NewReader(`{"text":"refuse stale send","clientMessageId":"w168-stale"}`)).WithContext(ctx)
			response := httptest.NewRecorder()
			w168Router(svc).ServeHTTP(response, request)
			if response.Code < 400 {
				t.Fatalf("stale send accepted: %d %s", response.Code, response.Body.String())
			}
			if which != "cancel" && response.Code != http.StatusConflict {
				t.Fatalf("expected safe conflict, got %d: %s", response.Code, response.Body.String())
			}
			provider.mu.Lock()
			calls := provider.sendCalls
			provider.mu.Unlock()
			rows, err := st.LoadConversationSnapshot(context.Background(), ctrl.ConversationID())
			if err != nil || calls != 0 || len(rows.Messages) != 0 || len(rows.Turns) != 0 || acquired != released {
				t.Fatalf("refusal had effects: calls=%d rows=%+v lease=%d/%d err=%v", calls, rows, acquired, released, err)
			}
			if current, err := svc.Controller(testSession); err != nil || current != ctrl || current.ProviderConversationID() != provider.ProviderConversationID() {
				t.Fatal("refusal replaced or destroyed prior controller")
			}
		})
	}
}

func TestW168HTTPAdmissionDedupAndRelayUseOneLease(t *testing.T) {
	svc, provider, st, _, ctrl := w168AdmissionFixture(t)
	active, acquired, released := 0, 0, 0
	lease := func() (func(), error) {
		active++
		acquired++
		if active != 1 {
			t.Fatalf("double manager lease: %d", active)
		}
		return func() { active--; released++ }, nil
	}
	svc.SetSessionInputAdmission(func(domain.SessionID) (func(), bool) {
		release, _ := lease()
		return release, true
	})
	provider.onSend = func(string) {
		if active != 1 {
			t.Errorf("provider send has %d leases", active)
		}
	}
	router := w168Router(svc)
	for range 2 {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/sessions/p1-1/conversation/messages", strings.NewReader(`{"text":"same request","clientMessageId":"w168-dedup"}`)))
		if response.Code != http.StatusAccepted {
			t.Fatalf("send: %d %s", response.Code, response.Body.String())
		}
	}
	// Supplying manager admission must bypass the service's configured callback.
	if _, err := svc.RelayChatTurnWithAdmission(context.Background(), testSession, "same request", "w168-dedup", lease); err != nil {
		t.Fatal(err)
	}
	rows, err := st.LoadConversationSnapshot(context.Background(), ctrl.ConversationID())
	provider.mu.Lock()
	calls := provider.sendCalls
	provider.mu.Unlock()
	if err != nil || len(rows.Messages) != 1 || len(rows.Turns) != 1 || calls != 1 || active != 0 || acquired != 3 || released != 3 {
		t.Fatalf("dedup/lease: messages=%d turns=%d calls=%d leases=%d/%d active=%d err=%v", len(rows.Messages), len(rows.Turns), calls, acquired, released, active, err)
	}
}

func TestW168DirectAdmissionReleasesOnProviderFailure(t *testing.T) {
	svc, provider, _, _, ctrl := w168AdmissionFixture(t)
	provider.sendErr = errors.New("synthetic provider failure")
	active := 0
	svc.SetSessionInputAdmission(func(domain.SessionID) (func(), bool) {
		active++
		return func() { active-- }, true
	})
	_, err := svc.Send(context.Background(), testSession, ports.ChatUserMessage{Text: "fail", ClientMessageID: "w168-fail"})
	if err == nil || active != 0 {
		t.Fatalf("failure=%v active leases=%d", err, active)
	}
	if current, err := svc.Controller(testSession); err != nil || current != ctrl {
		t.Fatal("failure destroyed prior controller")
	}
}

func TestW168AdmissionRefusesControllerStoppedDuringLease(t *testing.T) {
	svc, provider, st, _, ctrl := w168AdmissionFixture(t)
	released := false
	svc.SetSessionInputAdmission(func(domain.SessionID) (func(), bool) {
		if err := provider.Close(); err != nil {
			t.Fatal(err)
		}
		ctrl.Wait()
		return func() { released = true }, true
	})
	_, err := svc.Send(context.Background(), testSession, ports.ChatUserMessage{Text: "stopped", ClientMessageID: "w168-stopped"})
	if !errors.Is(err, chatsvc.ErrControllerHandoff) || !released {
		t.Fatalf("stopped selection: err=%v released=%v", err, released)
	}
	rows, err := st.LoadConversationSnapshot(context.Background(), ctrl.ConversationID())
	provider.mu.Lock()
	calls := provider.sendCalls
	provider.mu.Unlock()
	if err != nil || calls != 0 || len(rows.Messages) != 0 || len(rows.Turns) != 0 {
		t.Fatalf("stopped selection dispatched: calls=%d messages=%d turns=%d err=%v", calls, len(rows.Messages), len(rows.Turns), err)
	}
}
