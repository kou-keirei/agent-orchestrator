package httpd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type reportStoreStub struct {
	calls int
	err   error
}

func (s *reportStoreStub) AcceptWorkerReport(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	s.calls++
	return domain.WorkerReportReceipt{}, s.err
}
func (s *reportStoreStub) GetWorkerReport(context.Context, string) (domain.WorkerReportReceipt, bool, error) {
	return domain.WorkerReportReceipt{}, false, nil
}

func TestWorkerReportHTTPBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, peer, host, origin, body string
		status                                       int
	}{
		{"remote", "POST", "/api/v1/worker/reports", "192.0.2.1:123", "127.0.0.1", "", "{}", 403},
		{"origin", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "https://foreign.invalid", "{}", 403},
		{"host", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "foreign.invalid", "", "{}", 403},
		{"method", "GET", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", "{}", 405},
		{"path", "POST", "/api/v1/worker/reports/extra", "127.0.0.1:123", "127.0.0.1", "", "{}", 404},
		{"json", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", "{", 400},
		{"unknown", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", `{"typo":1}`, 400},
		{"trailing", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", "{} {}", 400},
		{"size", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", strings.Repeat(" ", (1<<20)+1), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &reportStoreStub{}
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.RemoteAddr = tc.peer
			r.Host = tc.host
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			NewWorkerReportHandler(s).ServeHTTP(w, r)
			if w.Code != tc.status || s.calls != 0 {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, s.calls, w.Body.String())
			}
		})
	}
}

func TestWorkerReportHTTPErrorEnvelope(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{domain.ErrInvalidWorkerReport, 400, "INVALID_WORKER_REPORT"},
		{domain.ErrWorkLeaseExpired, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrWorkIdentityMismatch, 409, "WORKER_REPORT_CONFLICT"},
		{errors.New("private store failure"), 500, "INTERNAL"},
	} {
		s := &reportStoreStub{err: tc.err}
		r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/worker/reports", strings.NewReader("{}"))
		r.RemoteAddr = "127.0.0.1:123"
		r.Header.Set("X-Request-ID", "report-request")
		w := httptest.NewRecorder()
		middleware.RequestID(NewWorkerReportHandler(s)).ServeHTTP(w, r)
		if w.Code != tc.status || s.calls != 1 || !strings.Contains(w.Body.String(), tc.code) || !strings.Contains(w.Body.String(), "report-request") || strings.Contains(w.Body.String(), "private store failure") {
			t.Fatalf("%s", w.Body.String())
		}
	}
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/worker/reports", strings.NewReader("{}"))
	r.RemoteAddr = "127.0.0.1:123"
	w := httptest.NewRecorder()
	NewWorkerReportHandler(nil).ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("unwired: %d", w.Code)
	}
}
