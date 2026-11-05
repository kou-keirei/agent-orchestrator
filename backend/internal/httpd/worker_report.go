package httpd

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// NewWorkerReportHandler is an independent loopback ingress, not yet mounted
// by the production daemon. Mount only on the primary loopback listener, with
// its existing request-ID middleware. The daemon must inject guarded submission;
// request fields never issue caller authority. Acceptance is not provider delivery
// or an original-parent semantic ACK. Native references remain authored claims.
func NewWorkerReportHandler(submission ports.WorkerReportSubmission) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/worker/reports" {
			http.NotFound(w, r)
			return
		}
		peer, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || !net.ParseIP(peer).IsLoopback() || !localControlRequest(r) {
			envelope.WriteAPIError(w, r, http.StatusForbidden, "forbidden", "LOOPBACK_REQUIRED", "Local CLI requests only", nil)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			envelope.WriteAPIError(w, r, http.StatusMethodNotAllowed, "bad_request", "METHOD_NOT_ALLOWED", "POST required", nil)
			return
		}
		if submission == nil {
			envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE", "Worker report caller binding unavailable", nil)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var report domain.WorkerReport
		if err := dec.Decode(&report); err != nil {
			envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid report JSON", nil)
			return
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Expected one report object", nil)
			return
		}
		receipt, err := submission.SubmitWorkerReport(r.Context(), report)
		switch {
		case err == nil:
			envelope.WriteJSON(w, http.StatusOK, receipt)
		case errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable):
			envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE", "Worker report caller binding unavailable", nil)
		case errors.Is(err, domain.ErrWorkerReportActorMismatch):
			envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "WORKER_REPORT_ACTOR_MISMATCH", "Worker report caller binding mismatch", nil)
		case errors.Is(err, domain.ErrInvalidWorkerReport), errors.Is(err, domain.ErrInvalidWorkIdentity):
			envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_WORKER_REPORT", "Invalid report or digest", nil)
		case errors.Is(err, domain.ErrWorkerReportConflict), errors.Is(err, domain.ErrWorkIdentityMismatch), errors.Is(err, domain.ErrStaleWorkOwner), errors.Is(err, domain.ErrWorkLeaseConflict), errors.Is(err, domain.ErrWorkLeaseExpired):
			envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "WORKER_REPORT_CONFLICT", "Report conflicts with accepted identity, lease, or report", nil)
		default:
			envelope.WriteError(w, r, err)
		}
	})
}
