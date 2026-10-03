package controllers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

// Exercise the persisted mode through the real store, service, and HTTP read
// model. A fake service misses storage normalization before readback validation.
func TestSessionsAPI_ContinuationReadbackPersistedMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode domain.SessionMode
	}{
		{"unknown", "voice"},
		{"empty", ""},
		{"whitespace", " \t\n"},
		{"wrong case", "TUI"},
		{"chat", domain.SessionModeChat},
		{"tui", domain.SessionModeTUI},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dataDir := t.TempDir()
			st := sqlitetest.MustOpenAt(t, dataDir)
			now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
			if err := st.UpsertProject(ctx, domain.ProjectRecord{ID: "readback", Path: dataDir, RegisteredAt: now}); err != nil {
				t.Fatal(err)
			}
			rec, err := st.CreateSession(ctx, domain.SessionRecord{
				ProjectID: "readback", Kind: domain.KindWorker, Harness: domain.HarnessCodex,
				Mode: domain.SessionModeTUI, CreatedAt: now, UpdatedAt: now,
				Activity: domain.Activity{State: domain.ActivityIdle, LastActivityAt: now},
				// Both modes have complete identity, so neither missing facts nor
				// a stale generation can accidentally mask an invalid mode.
				Metadata: domain.SessionMetadata{
					ProviderConversationID: "provider-thread", ControllerGeneration: "controller-generation",
					AgentSessionID: "native-session", RuntimeLaunchID: "runtime-launch", AgentSessionIDLaunchID: "runtime-launch",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			// CreateSession deliberately defaults invalid requested modes. Write
			// the persisted value directly to model a legacy/newer-build row.
			db, err := sql.Open("sqlite", filepath.Join(dataDir, "ao.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			result, err := db.ExecContext(ctx, "UPDATE sessions SET session_mode = ? WHERE id = ?", tc.mode, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("persist mode: affected=%d err=%v", n, err)
			}
			var persisted domain.SessionMode
			if err := db.QueryRowContext(ctx, "SELECT session_mode FROM sessions WHERE id = ?", rec.ID).Scan(&persisted); err != nil || persisted != tc.mode {
				t.Fatalf("persisted mode=%q want=%q err=%v", persisted, tc.mode, err)
			}
			svc := sessionsvc.NewWithDeps(sessionsvc.Deps{Store: st, Clock: func() time.Time { return now }})
			router := httpd.NewRouterWithControl(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
				httpd.APIDeps{Sessions: svc}, httpd.ControlDeps{})
			for _, path := range []string{
				"/api/v1/sessions/" + string(rec.ID),
				"/api/v1/sessions?projectId=readback",
				"/api/v1/sessions",
			} {
				t.Run(path, func(t *testing.T) {
					response := httptest.NewRecorder()
					router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
					if response.Code != http.StatusOK {
						t.Fatalf("GET %s = %d: %s", path, response.Code, response.Body.String())
					}
					var body struct {
						Session  controllers.SessionView   `json:"session"`
						Sessions []controllers.SessionView `json:"sessions"`
					}
					if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
						t.Fatal(err)
					}
					view := body.Session
					if path != "/api/v1/sessions/"+string(rec.ID) {
						if len(body.Sessions) != 1 {
							t.Fatalf("sessions = %d, want 1", len(body.Sessions))
						}
						view = body.Sessions[0]
					}
					got := view.ContinuationReadback
					available := tc.mode.Valid()
					reason := "missing_or_unknown_mode"
					chatReason, tuiReason := reason, reason
					if available {
						reason = "available"
						chatReason, tuiReason = "mode_not_applicable", "mode_not_applicable"
						if tc.mode == domain.SessionModeChat {
							chatReason = reason
						} else {
							tuiReason = reason
						}
					}
					if got.Available != available || got.Reason != reason ||
						got.Chat.Available != (tc.mode == domain.SessionModeChat) || got.Chat.Reason != chatReason ||
						got.TUI.Available != (tc.mode == domain.SessionModeTUI) || got.TUI.Reason != tuiReason {
						t.Errorf("persisted mode=%q readback=%+v, want available=%v reason=%s chat=%s tui=%s", persisted, got, available, reason, chatReason, tuiReason)
					}
					if view.Mode != persisted || got.Mode != persisted || got.SessionID != rec.ID || got.ProjectID != rec.ProjectID || got.Harness != rec.Harness ||
						got.Chat.ProviderConversationID != rec.Metadata.ProviderConversationID || got.Chat.ControllerGeneration != rec.Metadata.ControllerGeneration ||
						got.TUI.AgentSessionID != rec.Metadata.AgentSessionID || got.TUI.RuntimeLaunchID != rec.Metadata.RuntimeLaunchID || got.TUI.AgentSessionIDLaunchID != rec.Metadata.AgentSessionIDLaunchID {
						t.Errorf("readback lost persisted identity: mode=%q readback=%+v", view.Mode, got)
					}
				})
			}
		})
	}
}
