package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidWorkerReport rejects incomplete authorship or an invalid digest.
	ErrInvalidWorkerReport = errors.New("invalid worker report")
	// ErrWorkerReportConflict rejects a changed ID payload or a second done.
	ErrWorkerReportConflict = errors.New("worker report conflict")
)

// OrchestrationWorkerReport identifies an explicitly authored report, never a
// settled turn or an inference from runtime activity.
const OrchestrationWorkerReport OrchestrationEventKind = "worker_report"

// WorkerReport is a versioned, immutable worker assertion. Native references
// are opaque authored provenance, not proof of provider execution/continuity.
// Digest is lowercase SHA-256 of Content bytes, or of the referenced artifact
// bytes (which this boundary does not fetch). Exactly one content form is used.
type WorkerReport struct {
	ID                      string
	Type                    string
	Version                 int
	Identity                WorkItemIdentity
	LeaseID                 string
	LeaseRevision           int64
	WorkerSessionID         SessionID
	NativeSessionID         string
	NativeTurnID            string
	OriginalNativeSessionID string
	Content                 string
	ArtifactReference       string
	Digest                  string
}

// Validate checks explicit authorship, shape and inline content integrity.
// Durable identity/lease freshness must be checked inside the accepting writer.
func (r WorkerReport) Validate() error {
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	for _, value := range []string{r.ID, r.LeaseID, string(r.WorkerSessionID), r.NativeSessionID, r.NativeTurnID} {
		if !validLeaseText(value) || !utf8.ValidString(value) {
			return ErrInvalidWorkerReport
		}
	}
	// JSON persistence must preserve every authored byte, including identity.
	for _, value := range []string{string(r.Identity.ID), string(r.Identity.AttemptID), string(r.Identity.ProjectID),
		r.Identity.Repository, r.Identity.ExecutionBaseSHA, string(r.Identity.OriginalParentSessionID),
		string(r.Identity.OriginalWorkerSessionID), string(r.Identity.OwnerSessionID), r.Identity.WorktreePath,
		r.Identity.Branch, r.OriginalNativeSessionID} {
		if !utf8.ValidString(value) {
			return ErrInvalidWorkerReport
		}
	}
	if strings.TrimSpace(r.OriginalNativeSessionID) != r.OriginalNativeSessionID || strings.ContainsAny(r.OriginalNativeSessionID, "\x00\r\n") {
		return ErrInvalidWorkerReport
	}
	if r.Version != 1 || r.LeaseRevision <= 0 || r.WorkerSessionID != r.Identity.OwnerSessionID {
		return ErrInvalidWorkerReport
	}
	switch r.Type {
	case "checkpoint", "needs_input", "stuck", "done", "failure", "artifact":
	default:
		return ErrInvalidWorkerReport
	}
	if (r.Content == "") == (r.ArtifactReference == "") || !utf8.ValidString(r.Content) ||
		(r.ArtifactReference != "" && (!validLeaseText(r.ArtifactReference) || !utf8.ValidString(r.ArtifactReference))) ||
		(r.Type == "artifact" && r.ArtifactReference == "") {
		return ErrInvalidWorkerReport
	}
	if len(r.Digest) != 64 || strings.ToLower(r.Digest) != r.Digest {
		return ErrInvalidWorkerReport
	}
	if _, err := hex.DecodeString(r.Digest); err != nil {
		return ErrInvalidWorkerReport
	}
	if r.Content != "" {
		digest := sha256.Sum256([]byte(r.Content))
		if r.Digest != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("%w: content digest", ErrInvalidWorkerReport)
		}
	}
	return nil
}

// WorkerReportReceipt proves durable acceptance and initial outbox enqueue.
// It does not assert current queue state, provider delivery, or parent semantic
// ACK. Those require separate #7 evidence; an outbox transport ACK is not one.
type WorkerReportReceipt struct {
	Report       WorkerReport
	EventID      string
	AcceptedAtMS int64
}
