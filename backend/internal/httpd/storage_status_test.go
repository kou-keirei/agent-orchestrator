package httpd

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	storagesvc "github.com/aoagents/agent-orchestrator/backend/internal/service/storage"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type storageFixtureSource struct {
	err   error
	calls *int
}

func TestStorageAPIIsolatedSQLiteSmoke(t *testing.T) {
	dataDir := t.TempDir()
	store := sqlitetest.MustOpenAt(t, dataDir)
	now := time.Now().UTC()
	rec, err := store.CreateSession(context.Background(), domain.SessionRecord{
		Kind: domain.KindWorker, Harness: domain.HarnessFake,
		Activity:  domain.Activity{State: domain.ActivityActive, LastActivityAt: now},
		Metadata:  domain.SessionMetadata{WorkspacePath: "/unproven/scratch"},
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouterWithControl(config.Config{DataDir: dataDir}, nil, nil, APIDeps{Storage: store}, ControlDeps{})
	for _, route := range []string{"status", "cleanup-plan"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/storage/"+route, nil))
		var body storagesvc.Status
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusOK || len(body.Rows) != 4 || body.ExecutableCandidates != 0 {
			t.Fatalf("native read: %d %s", w.Code, w.Body.String())
		}
		row := body.Rows[3]
		if row.OwnerSession != string(rec.ID) || !row.Retain || row.LogicalBytes != nil || row.PhysicalBytes != nil {
			t.Fatalf("unsafe native row: %+v", row)
		}
	}
	after, found, err := store.GetSession(context.Background(), rec.ID)
	if err != nil || !found || after.Revision != rec.Revision {
		t.Fatalf("read changed session revision: %+v %v", after, err)
	}
}

func (s storageFixtureSource) ListStorageWorkspaces(context.Context, string, int) ([]storagesvc.WorkspaceRecord, error) {
	if s.calls != nil {
		*s.calls++
	}
	return []storagesvc.WorkspaceRecord{{ID: "workspace:01", SessionID: "owner", Path: "/never-probed"}}, s.err
}

func TestStorageAPIRejectsInvalidQueryBeforeSourceRead(t *testing.T) {
	for _, route := range []string{"status", "cleanup-plan"} {
		for _, query := range []string{
			"after=%GG", "limit=1&after=%GG", "limit=1&limit=%GG",
			"after=workspace:01;ignored=value", "limit=1&after=bad;cursor",
			"limit=257", "limit=0", "limit=bad", "limit=1&limit=2",
			"after=a&after=b", "after=%00", "apply=true",
		} {
			t.Run(route+"/"+query, func(t *testing.T) {
				calls := 0
				router := NewRouterWithControl(config.Config{}, nil, nil, APIDeps{Storage: storageFixtureSource{calls: &calls}}, ControlDeps{})
				w := httptest.NewRecorder()
				router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/storage/"+route+"?"+query, nil))
				var body struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if w.Code != http.StatusBadRequest || body.Code != "INVALID_STORAGE_PAGE" || calls != 0 {
					t.Fatalf("invalid query: status=%d body=%s source calls=%d", w.Code, w.Body.String(), calls)
				}
			})
		}
	}
}

func TestStorageReadOnlyAPISmoke(t *testing.T) {
	router := NewRouterWithControl(config.Config{DataDir: t.TempDir()}, nil, nil, APIDeps{Storage: storageFixtureSource{}}, ControlDeps{})
	for _, path := range []string{"/api/v1/storage/status", "/api/v1/storage/cleanup-plan"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path+"?limit=2", nil))
		var body storagesvc.Status
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusOK || body.State != "SOURCE_GAP" || len(body.Rows) != 2 || body.NextAfter == "" || body.ExecutableCandidates != 0 {
			t.Fatalf("%d: %s", w.Code, w.Body.String())
		}
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			w = httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(method, path, nil))
			if w.Code != http.StatusMethodNotAllowed {
				t.Fatalf("mutation accepted: %s %d", method, w.Code)
			}
		}
	}
	for _, query := range []string{"limit=257", "limit=0", "limit=bad", "limit=1&limit=2", "apply=true"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/storage/status?"+query, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("query accepted: %s %d", query, w.Code)
		}
	}
	for _, path := range []string{"/api/v1/storage/apply", "/api/v1/storage/prune", "/api/v1/storage/reap"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unexpected execution surface: %s", path)
		}
	}
}

func TestStorageAPIErrors(t *testing.T) {
	for _, deps := range []APIDeps{{}, {Storage: storageFixtureSource{err: fs.ErrPermission}}} {
		router := NewRouterWithControl(config.Config{}, nil, nil, deps, ControlDeps{})
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/storage/status", nil))
		want := http.StatusInternalServerError
		if deps.Storage == nil {
			want = http.StatusNotImplemented
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != want || body["code"] == nil || body["requestId"] == nil {
			t.Fatalf("lost envelope: %d %s", w.Code, w.Body.String())
		}
	}
}
