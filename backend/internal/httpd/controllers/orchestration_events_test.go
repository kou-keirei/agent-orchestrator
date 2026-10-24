package controllers_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/go-chi/chi/v5"
)

type orchestrationReaderFake struct {
	held                    bool
	outstanding, holdWrites int
	holdErr                 error
	project                 domain.ProjectID
	limit                   int
	retryProject            domain.ProjectID
}

func (f *orchestrationReaderFake) RetryDeadLetterOrchestrationEvent(_ context.Context, project domain.ProjectID, _ string, _ time.Time) (bool, error) {
	f.retryProject = project
	return true, nil
}

func (f *orchestrationReaderFake) ListOrchestrationEvents(_ context.Context, p domain.ProjectID, limit int) ([]domain.OrchestrationEvent, error) {
	f.project = p
	f.limit = limit
	now := time.Now().UTC()
	return []domain.OrchestrationEvent{{ID: "e", ProjectID: p, WorkerID: "w", Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "r", State: domain.OrchestrationSubmitted, AttemptCount: 1, EnqueuedAt: now, NextAttemptAt: now, DestinationSessionID: "o", SubmittedAt: now}}, nil
}

func TestRetryOrchestrationEventIsProjectScoped(t *testing.T) {
	f := &orchestrationReaderFake{}
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) { (&controllers.OrchestrationEventsController{Store: f}).Register(r) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/projects/p/orchestration-events/e/retry", nil))
	if w.Code != http.StatusOK || f.retryProject != "p" {
		t.Fatalf("status=%d project=%q body=%s", w.Code, f.retryProject, w.Body.String())
	}
}

func TestListOrchestrationEventsExposesDeliveryObservability(t *testing.T) {
	f := &orchestrationReaderFake{}
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) { (&controllers.OrchestrationEventsController{Store: f}).Register(r) })
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/p/orchestration-events?limit=7", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Events []controllers.OrchestrationEventResponse `json:"events"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if f.project != "p" || f.limit != 7 || len(body.Events) != 1 || body.Events[0].DestinationSessionID != "o" || body.Events[0].SubmittedAt == nil {
		t.Fatalf("project=%q limit=%d body=%+v", f.project, f.limit, body)
	}
}

func TestListOrchestrationEventsRejectsInvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Route("/api/v1", func(r chi.Router) {
		(&controllers.OrchestrationEventsController{Store: &orchestrationReaderFake{}}).Register(r)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/projects/p/orchestration-events?limit=zero", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func (f *orchestrationReaderFake) SetOrchestrationDispatchHold(_ context.Context, p domain.ProjectID, held bool) error {
	f.project = p
	f.held = held
	f.holdWrites++
	return f.holdErr
}
func (f *orchestrationReaderFake) ReadOrchestrationDispatchHold(_ context.Context, p domain.ProjectID) (bool, int, error) {
	f.project = p
	return f.held, f.outstanding, f.holdErr
}
func TestOrchestrationDispatchHoldExplicitControlAndReadback(t *testing.T) {
	f := &orchestrationReaderFake{}
	r := chi.NewRouter()
	(&controllers.OrchestrationEventsController{Store: f}).Register(r)
	for _, tc := range []struct {
		method, body string
		code         int
		held         bool
		outstanding  int
		state        string
	}{
		{"GET", "", 200, false, 0, "UNHELD"},
		{"PUT", "{}", 400, false, 0, ""},
		{"PUT", `{"held":null}`, 400, false, 0, ""},
		{"PUT", `{"held":"true"}`, 400, false, 0, ""},
		{"PUT", `{"held":true,"unknown":1}`, 400, false, 0, ""},
		{"PUT", `{"held":false} {"held":true}`, 400, false, 0, ""},
		{"PUT", `{"held":true}`, 200, true, 0, "HELD"},
		{"GET", "", 200, true, 2, "OUTSTANDING_DELIVERY"},
		{"PUT", `{"held":false}`, 200, false, 2, "UNHELD"},
	} {
		f.outstanding = tc.outstanding
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(tc.method, "/projects/p/orchestration-dispatch-hold", strings.NewReader(tc.body)))
		if w.Code != tc.code {
			t.Fatalf("%s %s: %d %s", tc.method, tc.body, w.Code, w.Body.String())
		}
		if tc.code == 200 {
			var got controllers.OrchestrationDispatchHoldResponse
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.ProjectID != "p" || got.Held != tc.held || got.State != tc.state || got.OutstandingEventCount != tc.outstanding {
				t.Fatalf("%+v", got)
			}
		}
	}
	if f.holdWrites != 2 {
		t.Fatalf("writes=%d", f.holdWrites)
	}
	f.holdErr = errors.New("storage unavailable")
	for _, method := range []string{"GET", "PUT"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(method, "/projects/missing/orchestration-dispatch-hold", strings.NewReader(`{"held":true}`)))
		if w.Code != 500 {
			t.Fatalf("storage error status=%d", w.Code)
		}
	}
}
