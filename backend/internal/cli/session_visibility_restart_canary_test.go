package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// This is a synthetic CLI boundary canary for a future disposable native
// roundtrip. Fresh CLI commands share fake HTTP state; no daemon restart,
// database persistence, provider process census, or installed-app acceptance
// is exercised. Worktree is backing-state-only: sessionDTO does not expose it.
type visibilityCanaryState struct {
	Session  sessionDTO
	Worktree string
}

type visibilityCanary struct {
	mu       sync.Mutex
	state    visibilityCanaryState
	requests []string
}

func newVisibilityCanary(t *testing.T, terminated bool) (*visibilityCanary, Deps) {
	t.Helper()
	cfg := setConfigEnv(t)
	f := &visibilityCanary{}
	if err := json.Unmarshal([]byte(sessionJSON("visibility-1", "demo", "worker", "idle", terminated)), &f.state.Session); err != nil {
		t.Fatal(err)
	}
	if terminated {
		f.state.Session.Status = "terminated"
	}
	f.state.Session.ContinuationReadback.Chat.ProviderConversationID = "synthetic-provider-1"
	f.state.Session.ContinuationReadback.Chat.ControllerGeneration = "synthetic-controller-1"
	f.state.Worktree = "/synthetic/worktrees/visibility-1"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if requestLogEntry(r) == cliInvokedRequest {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		f.requests = append(f.requests, requestLogEntry(r))
		path := "/api/v1/sessions/" + f.state.Session.ID
		switch {
		case r.Method == http.MethodGet && r.URL.Path == path:
			// Direct ID lookup deliberately includes presentation-hidden state.
		case r.Method == http.MethodPost && r.URL.Path == path+"/hide":
			hiddenAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			f.state.Session.HiddenAt = &hiddenAt
		case r.Method == http.MethodDelete && r.URL.Path == path+"/hide":
			f.state.Session.HiddenAt = nil
		case strings.HasPrefix(r.URL.Path, "/api/v1/sessions/missing"):
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(apiError{
				Code: "SESSION_NOT_FOUND", Message: "session not found", RequestID: "visibility-missing",
			})
			return
		default:
			// Any spawn/resume/restore/send or other unexpected call fails closed
			// and remains in the exact request log asserted by each test.
			t.Errorf("unexpected canary request: %s", requestLogEntry(r))
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(apiError{Code: "UNEXPECTED_CANARY_REQUEST"})
			return
		}
		_ = json.NewEncoder(w).Encode(sessionResponse{Session: f.state.Session})
	}))
	t.Cleanup(srv.Close)
	writeRunFileFor(t, cfg, srv)
	return f, Deps{
		ProcessAlive: func(int) bool { return true }, // Synthetic run-file probe only.
		StartProcess: func(processStartConfig) error {
			t.Error("visibility command attempted a process start")
			return fmt.Errorf("process start forbidden in visibility canary")
		},
	}
}

func (f *visibilityCanary) snapshot(t *testing.T) visibilityCanaryState {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	// Deep copy so pointer changes cannot silently mutate the comparison oracle.
	b, err := json.Marshal(f.state)
	if err != nil {
		t.Fatal(err)
	}
	var state visibilityCanaryState
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func (f *visibilityCanary) assertRequests(t *testing.T, want []string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if !reflect.DeepEqual(f.requests, want) {
		t.Fatalf("requests = %#v, want %#v", f.requests, want)
	}
}

func TestSessionVisibilityRestartCanary_SyntheticRoundtrip(t *testing.T) {
	for _, terminated := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminated=%t", terminated), func(t *testing.T) {
			f, deps := newVisibilityCanary(t, terminated)
			baseline := f.snapshot(t)
			get := func(hidden bool) {
				t.Helper()
				out, errOut, err := executeCLI(t, deps, "session", "get", "visibility-1", "--project", "demo", "--json")
				if err != nil {
					t.Fatalf("get failed: %v; stderr=%s", err, errOut)
				}
				var got sessionResponse
				if err := json.Unmarshal([]byte(out), &got); err != nil {
					t.Fatal(err)
				}
				if (got.Session.HiddenAt != nil) != hidden {
					t.Fatalf("hiddenAt = %v, want hidden=%t", got.Session.HiddenAt, hidden)
				}
				got.Session.HiddenAt = nil
				if !reflect.DeepEqual(got.Session, baseline.Session) {
					t.Fatalf("CLI changed identity or non-visibility fields: %#v", got.Session)
				}
				state := f.snapshot(t)
				if (state.Session.HiddenAt != nil) != hidden {
					t.Fatalf("backing state hiddenAt = %v, want hidden=%t", state.Session.HiddenAt, hidden)
				}
				state.Session.HiddenAt = nil
				if !reflect.DeepEqual(state, baseline) {
					t.Fatalf("fake backing state changed beyond visibility: %#v", state)
				}
			}
			get(false)
			for _, step := range []struct {
				command string
				message string
				hidden  bool
			}{
				{"hide", "session visibility-1 hidden\n", true},
				{"unhide", "session visibility-1 unhidden\n", false},
			} {
				out, errOut, err := executeCLI(t, deps, "session", step.command, "visibility-1", "--project", "demo")
				if err != nil || out != step.message {
					t.Fatalf("%s: output=%q err=%v stderr=%s", step.command, out, err, errOut)
				}
				get(step.hidden)
			}
			f.assertRequests(t, []string{
				"GET /api/v1/sessions/visibility-1",
				"GET /api/v1/sessions/visibility-1",
				"POST /api/v1/sessions/visibility-1/hide",
				"GET /api/v1/sessions/visibility-1",
				"GET /api/v1/sessions/visibility-1",
				"DELETE /api/v1/sessions/visibility-1/hide",
				"GET /api/v1/sessions/visibility-1",
			})
		})
	}
}

func TestSessionVisibilityRestartCanary_MissingAndForeign(t *testing.T) {
	for _, command := range []string{"get", "hide", "unhide"} {
		for _, foreign := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/foreign=%t", command, foreign), func(t *testing.T) {
				f, deps := newVisibilityCanary(t, false)
				baseline := f.snapshot(t)
				id, project := "missing", "demo"
				if foreign {
					id, project = "visibility-1", "other"
				}
				out, _, err := executeCLI(t, deps, "session", command, id, "--project", project)
				if err == nil || out != "" {
					t.Fatalf("expected failure without success output: out=%q err=%v", out, err)
				}
				if foreign {
					var scoped usageError
					if !errors.As(err, &scoped) || ExitCode(err) != 2 || !strings.Contains(err.Error(), "not in project other") {
						t.Fatalf("expected typed project-scope usage error, got %T: %v", err, err)
					}
				} else {
					var missing apiResponseError
					if !errors.As(err, &missing) || ExitCode(err) != 1 || missing.StatusCode != http.StatusNotFound ||
						missing.ErrorBody.Code != "SESSION_NOT_FOUND" || missing.ErrorBody.RequestID != "visibility-missing" {
						t.Fatalf("expected typed missing-session envelope, got %T: %v", err, err)
					}
				}
				if got := f.snapshot(t); !reflect.DeepEqual(got, baseline) {
					t.Fatalf("rejected command changed fake backing state: %#v", got)
				}
				f.assertRequests(t, []string{"GET /api/v1/sessions/" + id})
			})
		}
	}
}
