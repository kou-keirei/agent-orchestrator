package httpd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type reportSubmissionStub struct {
	calls int
	err   error
}

func (s *reportSubmissionStub) SubmitWorkerReport(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	s.calls++
	// Even an erroneous submission returning data must not expose a receipt.
	return domain.WorkerReportReceipt{EventID: "private-event"}, s.err
}

func TestWorkerReportHTTPBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, peer, host, origin, body string
		status                                       int
	}{
		{"remote", "POST", "/api/v1/worker/reports", "192.0.2.1:123", "127.0.0.1", "", "{}", 403},
		{"malformed-peer", "POST", "/api/v1/worker/reports", "127.0.0.1", "127.0.0.1", "", "{}", 403},
		{"origin", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "https://foreign.invalid", "{}", 403},
		{"local-origin", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "http://127.0.0.1", "{}", 403},
		{"host", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "foreign.invalid", "", "{}", 403},
		{"method", "GET", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", "{}", 405},
		{"path", "POST", "/api/v1/worker/reports/extra", "127.0.0.1:123", "127.0.0.1", "", "{}", 404},
		{"json", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", "{", 400},
		{"unknown", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", `{"typo":1}`, 400},
		{"trailing", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", "{} {}", 400},
		{"size", "POST", "/api/v1/worker/reports", "127.0.0.1:123", "127.0.0.1", "", strings.Repeat(" ", (1<<20)+1), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &reportSubmissionStub{}
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.RemoteAddr = tc.peer
			r.Host = tc.host
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			NewWorkerReportHandler(s).ServeHTTP(w, r)
			if w.Code != tc.status || s.calls != 0 {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, s.calls, w.Body.String())
			}
			if tc.status == http.StatusMethodNotAllowed && w.Header().Get("Allow") != http.MethodPost {
				t.Fatal("missing POST Allow header")
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
		{domain.ErrInvalidWorkIdentity, 400, "INVALID_WORKER_REPORT"},
		{domain.ErrWorkerReportConflict, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrWorkLeaseExpired, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrWorkLeaseConflict, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrStaleWorkOwner, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrWorkIdentityMismatch, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrWorkerReportActorBindingUnavailable, 503, "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE"},
		{fmt.Errorf("private binding detail: %w", domain.WorkerReportNativeIngressBlock{}), 503, "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE"},
		{fmt.Errorf("private actor detail: %w", domain.ErrWorkerReportActorMismatch), 409, "WORKER_REPORT_ACTOR_MISMATCH"},
		{errors.New("private store failure"), 500, "INTERNAL_ERROR"},
	} {
		s := &reportSubmissionStub{err: tc.err}
		w := workerReportHTTPSubmit(s, "{}")
		requireWorkerReportHTTPRefusal(t, w, tc.status, tc.code)
		if s.calls != 1 {
			t.Fatalf("submission calls=%d", s.calls)
		}
	}
	requireWorkerReportHTTPRefusal(t, workerReportHTTPSubmit(nil, "{}"), 503, "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE")
}

func workerReportHTTPSubmit(submission ports.WorkerReportSubmission, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/worker/reports", strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:123"
	r.Header.Set("X-Request-ID", "report-request")
	w := httptest.NewRecorder()
	middleware.RequestID(NewWorkerReportHandler(submission)).ServeHTTP(w, r)
	return w
}

func requireWorkerReportHTTPRefusal(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var got envelope.APIError
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != status || got.Code != code || got.RequestID != "report-request" || got.Error == "" || got.Message == "" {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"Report", "EventID", "AcceptedAtMS"} {
		if _, found := fields[key]; found {
			t.Fatalf("refusal exposed receipt: %s", w.Body.String())
		}
	}
	if strings.Contains(w.Body.String(), "private") {
		t.Fatalf("refusal exposed private submission detail: %s", w.Body.String())
	}
}

// This store counts fixture effects only; it is not SQLite or a delivery proof.
type workerReportHTTPCountingStore struct {
	writes, lookups, outbox int
	receipt                 domain.WorkerReportReceipt
}

func (s *workerReportHTTPCountingStore) AcceptWorkerReport(_ context.Context, r domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	s.writes++
	if s.outbox == 0 {
		s.outbox++
		s.receipt = domain.WorkerReportReceipt{Report: r, EventID: "fixture-event", AcceptedAtMS: 1}
	}
	return s.receipt, nil
}

func (s *workerReportHTTPCountingStore) GetWorkerReport(context.Context, string) (domain.WorkerReportReceipt, bool, error) {
	s.lookups++
	return s.receipt, s.outbox != 0, nil
}

// Injected ordering/refusal fixture, never a daemon-issued native actor.
type workerReportHTTPVerifierFunc func(context.Context, domain.WorkerReport) error

func (f workerReportHTTPVerifierFunc) VerifyWorkerReportActor(ctx context.Context, r domain.WorkerReport) error {
	return f(ctx, r)
}

func workerReportHTTPClaim(t *testing.T) (domain.WorkerReport, string) {
	t.Helper()
	sum := sha256.Sum256([]byte("checkpoint"))
	r := domain.WorkerReport{
		ID: "report", Type: "checkpoint", Version: 1,
		Identity: domain.WorkItemIdentity{
			ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo",
			ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent",
			OriginalWorkerSessionID: "worker", OwnerSessionID: "worker", OwnerGeneration: 1,
			WorktreePath: "/work", Branch: "branch",
		},
		LeaseID: "lease", LeaseRevision: 1, WorkerSessionID: "worker",
		NativeSessionID: "native", NativeTurnID: "claimed-turn", OriginalNativeSessionID: "native",
		Content: "checkpoint", Digest: hex.EncodeToString(sum[:]),
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("malformed fixture claim: %v", err)
	}
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, string(body)
}

func TestWorkerReportHTTPMissingBindingHasNoEffects(t *testing.T) {
	_, body := workerReportHTTPClaim(t)
	for _, tc := range []struct {
		name     string
		verifier ports.WorkerReportActorVerifier
	}{
		{"nil-issuer", nil},
		{"explicit-unavailable", reportactor.Unavailable{}},
		{"native-ingress-block", workerReportHTTPVerifierFunc(func(context.Context, domain.WorkerReport) error {
			return domain.WorkerReportNativeIngressBlock{}
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &workerReportHTTPCountingStore{}
			acceptance, err := reportactor.NewAcceptance(tc.verifier, store)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				requireWorkerReportHTTPRefusal(t, workerReportHTTPSubmit(acceptance, body), 503, "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE")
			}
			if store.writes != 0 || store.lookups != 0 || store.outbox != 0 || store.receipt != (domain.WorkerReportReceipt{}) {
				t.Fatalf("missing binding reached storage: %+v", store)
			}
		})
	}
}

func TestWorkerReportHTTPActorMismatchHasNoEffects(t *testing.T) {
	want, _ := workerReportHTTPClaim(t)
	for name, mutate := range map[string]func(*domain.WorkerReport){
		"spoofed-worker":      func(r *domain.WorkerReport) { r.WorkerSessionID, r.Identity.OwnerSessionID = "spoof", "spoof" },
		"stale-generation":    func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ },
		"spoofed-native-turn": func(r *domain.WorkerReport) { r.NativeTurnID = "spoof" },
	} {
		t.Run(name, func(t *testing.T) {
			claim := want
			mutate(&claim)
			if err := claim.Validate(); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(claim)
			if err != nil {
				t.Fatal(err)
			}
			store := &workerReportHTTPCountingStore{}
			verified := 0
			acceptance, err := reportactor.NewAcceptance(workerReportHTTPVerifierFunc(func(_ context.Context, got domain.WorkerReport) error {
				verified++
				if got != claim || got == want {
					t.Fatal("mismatch fixture did not receive the spoofed claim")
				}
				return fmt.Errorf("fixture mismatch: %w", domain.ErrWorkerReportActorMismatch)
			}), store)
			if err != nil {
				t.Fatal(err)
			}
			requireWorkerReportHTTPRefusal(t, workerReportHTTPSubmit(acceptance, string(body)), 409, "WORKER_REPORT_ACTOR_MISMATCH")
			if verified != 1 || store.writes != 0 || store.lookups != 0 || store.outbox != 0 || store.receipt != (domain.WorkerReportReceipt{}) {
				t.Fatalf("verified=%d store=%+v", verified, store)
			}
		})
	}
}

func TestWorkerReportHTTPInjectedFixtureReverifiesExactRetries(t *testing.T) {
	claim, body := workerReportHTTPClaim(t)
	store := &workerReportHTTPCountingStore{}
	verified := 0
	var verificationErr error
	acceptance, err := reportactor.NewAcceptance(workerReportHTTPVerifierFunc(func(_ context.Context, got domain.WorkerReport) error {
		verified++
		if got != claim {
			t.Fatal("HTTP changed the authored claim")
		}
		// Earlier successful requests cannot authorize this invocation.
		if verified <= 2 && store.writes != verified-1 {
			t.Fatal("writer ran before this invocation's verification")
		}
		return verificationErr
	}), store)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		w := workerReportHTTPSubmit(acceptance, body)
		var got domain.WorkerReportReceipt
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusOK || got != store.receipt || got.Report != claim || got.EventID != "fixture-event" {
			t.Fatalf("fixture submission: status=%d body=%s", w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{domain.ErrWorkerReportActorMismatch, 409, "WORKER_REPORT_ACTOR_MISMATCH"},
		{domain.ErrStaleWorkOwner, 409, "WORKER_REPORT_CONFLICT"},
		{domain.ErrWorkerReportActorBindingUnavailable, 503, "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE"},
	} {
		verificationErr = tc.err
		requireWorkerReportHTTPRefusal(t, workerReportHTTPSubmit(acceptance, body), tc.status, tc.code)
	}
	if verified != 5 || store.writes != 2 || store.lookups != 0 || store.outbox != 1 {
		t.Fatalf("retry verification=%d store=%+v", verified, store)
	}
}
