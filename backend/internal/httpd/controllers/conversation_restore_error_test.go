package controllers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestConversationRestoreRefusalDoesNotRecommendRetry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
		retry  bool
	}{
		{"missing native identity", ports.ErrChatResumeFailed, http.StatusConflict, "CHAT_RESUME_FAILED", false},
		{"transient restore", ports.ErrChatControllerRestore, http.StatusServiceUnavailable, "CHAT_CONTROLLER_RESTORE_FAILED", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeConversationError(w, httptest.NewRequest(http.MethodPost, "/", nil), fmt.Errorf("restore: %w", tc.err))
			body := w.Body.String()
			if w.Code != tc.status || !strings.Contains(body, tc.code) || strings.Contains(body, "retry your message") != tc.retry {
				t.Fatalf("restore response = %d %s", w.Code, body)
			}
		})
	}
}
