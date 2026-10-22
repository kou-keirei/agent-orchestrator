package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// These are wire-classification fixtures, not a reproduction of the historical
// agent-orchestrator-767 failure or proof of provider-native resume.
func TestW168SessionResumeNativeRefusalsRemainTypedAtWireBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cause  error
		code   string
		status int
	}{
		{"resume", ports.ErrChatResumeFailed, "CHAT_RESUME_FAILED", http.StatusConflict},
		{"ownership", ports.ErrChatRecoveryInconclusive, "CHAT_RECOVERY_INCONCLUSIVE", http.StatusConflict},
		{"history-unavailable", ports.ErrChatHistoryUnavailable, "CHAT_HISTORY_UNAVAILABLE", http.StatusConflict},
		{"history-unsettled", ports.ErrChatHistoryUnsettled, "CHAT_HISTORY_UNSETTLED", http.StatusConflict},
		{"history-dimensions", &ports.ChatHistoryUnsettledError{Dimensions: []ports.ChatHistoryMismatchDimension{ports.ChatHistoryMismatchNativeIdentity}}, "CHAT_HISTORY_UNSETTLED", http.StatusConflict},
		{"ambiguous-history-cleanup", errors.Join(ports.ErrChatHistoryUnsettled, ports.ErrChatRecoveryInconclusive), "CHAT_RECOVERY_INCONCLUSIVE", http.StatusConflict},
		{"unexpected", errors.New("internal-native-diagnostic"), "INTERNAL_ERROR", http.StatusInternalServerError},
		{"deadline", context.DeadlineExceeded, "SERVICE_UNAVAILABLE", http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := &fakeCommander{restoreErr: fmt.Errorf("resume internal-native-diagnostic: %w", tc.cause)}
			svc := &Service{manager: manager}
			const id domain.SessionID = "ao-w168-fixture-session"
			outcome, err := svc.ResumeAgent(context.Background(), id)
			if err == nil || outcome.Session.ID != "" || outcome.Mode != "" {
				t.Fatalf("refused resume became a successful outcome: %+v %v", outcome, err)
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/sessions/ao-w168-fixture-session/resume-agent", nil)
			envelope.WriteError(response, request, err)
			var body struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if decodeErr := json.Unmarshal(response.Body.Bytes(), &body); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if response.Code != tc.status || body.Code != tc.code {
				t.Fatalf("native refusal: status=%d code=%q, want %d %q", response.Code, body.Code, tc.status, tc.code)
			}
			if strings.Contains(body.Message, "internal-native-diagnostic") {
				t.Fatal("native diagnostic leaked into public refusal")
			}
			if len(manager.resumed) != 1 || manager.resumed[0] != id || manager.spawned || len(manager.killed) != 0 || len(manager.exited) != 0 || len(manager.sent) != 0 {
				t.Fatalf("refusal changed session or caused fallback/delivery: %+v", manager)
			}
		})
	}
}
