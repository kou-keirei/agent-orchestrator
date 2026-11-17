package controllers_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
)

func TestSessionExactBaseRequestAndPersistedReadback(t *testing.T) {
	svc := newFakeSessionService()
	srv := newSessionTestServer(t, svc)
	sha := strings.Repeat("a", 40)
	body, status, _ := doRequest(t, srv, "POST", "/api/v1/sessions", `{"projectId":"ao","harness":"codex","executionBaseSha":"`+sha+`","sourceRef":"origin/main","branch":"ao/worker","model":"requested-model","permissions":"default"}`)
	if status != http.StatusCreated {
		t.Fatalf("spawn=%d %s", status, body)
	}
	if svc.lastSpawn.ExecutionBaseSHA != sha || svc.lastSpawn.SourceRef != "origin/main" || svc.lastSpawn.Branch != "ao/worker" || svc.lastSpawn.AgentConfig.Model != "requested-model" {
		t.Fatalf("request binding: %+v", svc.lastSpawn)
	}
	// Readback comes from persisted native facts, not an echo of caller fields.
	sess := svc.sessions["ao-2"]
	sess.Metadata = domain.SessionMetadata{ExecutionBaseSHA: sha, SourceRef: "refs/remotes/origin/main", Branch: "ao/worker", WorkspacePath: "/managed/ao-2", Model: "configured-model", Permissions: domain.PermissionModeDefault}
	svc.sessions[sess.ID] = sess
	body, status, _ = doRequest(t, srv, "GET", "/api/v1/sessions/ao-2", "")
	if status != http.StatusOK {
		t.Fatalf("get=%d %s", status, body)
	}
	var got controllers.SessionResponse
	mustJSON(t, body, &got)
	if got.Session.ExecutionBaseSHA != sha || got.Session.SourceRef != "refs/remotes/origin/main" || got.Session.WorktreePath != "/managed/ao-2" || got.Session.Branch != "ao/worker" || got.Session.Model != "configured-model" || got.Session.Permissions != domain.PermissionModeDefault {
		t.Fatalf("readback=%+v", got.Session)
	}
	if got.Session.ContinuationReadback.Available {
		t.Fatal("source/configuration facts fabricated provider continuity")
	}
}
