package domain

import (
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/pkg/contract"
)

// ErrDuplicateReviewRun is returned by InsertReviewRun when a run already exists
// for the same worker session and target commit (the partial unique index from
// migration 0013). It lets the review engine fall back to the recorded run
// instead of surfacing a raw storage error after a reviewer may have launched.
var ErrDuplicateReviewRun = errors.New("domain: review run already exists for session and target sha")

// Review is the per-worker, per-reviewer-harness code-review record. A repeat
// trigger for the same harness reuses this row; the per-pass facts live on
// ReviewRun.
type Review struct {
	ID        string          `json:"id"`
	SessionID SessionID       `json:"sessionId"`
	ProjectID ProjectID       `json:"projectId"`
	Harness   ReviewerHarness `json:"harness"`
	PRURL     string          `json:"prUrl"`
	// ReviewerHandleID is the runtime handle of the live reviewer pane, reused
	// across passes and exposed so the UI can attach its terminal.
	ReviewerHandleID string `json:"reviewerHandleId"`
	AgentSessionID   string `json:"agentSessionId"`
	// ReviewerLaunchID is the AO runtime generation that owns the live reviewer
	// pane on this row. It fences delayed hooks from an older replaced reviewer.
	ReviewerLaunchID string `json:"-"`
	// ReviewerActivityState is the latest activity hook reported by the review
	// pane itself. It is separate from ReviewRun.Status so the UI can distinguish
	// "review pass exists" from "reviewer is actively working right now".
	ReviewerActivityState ActivityState `json:"reviewerActivityState,omitempty"`
	CreatedAt             time.Time     `json:"createdAt"`
	UpdatedAt             time.Time     `json:"updatedAt"`
}

// ReviewRun is one review pass against a worker's PR.
type ReviewRun struct {
	ID        string    `json:"id"`
	ReviewID  string    `json:"reviewId"`
	SessionID SessionID `json:"sessionId"`
	// BatchID groups review runs created by one trigger so worker feedback can
	// be delivered once after the whole trigger batch is terminal. Empty marks
	// legacy/single-run delivery.
	BatchID string          `json:"batchId"`
	Harness ReviewerHarness `json:"harness"`
	// TriggerSource records whether this pass was requested by a user or by the
	// daemon auto-review coordinator.
	TriggerSource ReviewTriggerSource `json:"triggerSource" enum:"manual,auto"`
	PRURL         string              `json:"prUrl"`
	// TargetSHA is the PR head commit this pass reviewed.
	TargetSHA string `json:"targetSha"`
	// TargetBinding is the immutable AO observation that authorizes this pass.
	// A zero value means the row predates reviewer evidence closure and is
	// therefore unknown/unproven, never an inferred clean binding.
	TargetBinding ReviewTargetBinding `json:"targetBinding"`
	// Provenance keeps requested/configured/AO-resolved settings distinct from
	// provider-effective and provider-native facts. AO never fills the latter
	// from request or launch configuration alone.
	Provenance ReviewerProvenance `json:"provenance"`
	// LaunchID fences hooks and native evidence to the exact AO reviewer launch
	// generation that owned this pass. It is immutable once the run is created.
	LaunchID string          `json:"launchId"`
	Status   ReviewRunStatus `json:"status"`
	Verdict  ReviewVerdict   `json:"verdict"`
	// Body is the review text the reviewer submitted. It is recorded for AO's
	// own tracking; the reviewer also posts the review to the PR itself.
	Body string `json:"body"`
	// GithubReviewID is the id of the GitHub PR review the reviewer posted for
	// this pass (the `gh api .../pulls/{n}/reviews` object id), recorded at
	// submit time. It is empty when the reviewer could not post to the provider.
	// When the pass requests changes, AO includes it in the message to the
	// worker so the worker knows exactly which review to address and reply to.
	GithubReviewID string     `json:"githubReviewId"`
	CreatedAt      time.Time  `json:"createdAt"`
	DeliveredAt    *time.Time `json:"deliveredAt,omitempty"`
	// AutoInjectReview snapshots the session policy when this result is first
	// recorded. Later toggle changes must not rewrite or deliver this run.
	AutoInjectReview bool `json:"autoInjectReview"`
}

// ReviewerCleanupObligation is a durable, non-authoritative record of a
// reviewer runtime that AO may have created but could not prove destroyed.
// It deliberately lives outside Review and ReviewRun: an unresolved handle is
// cleanup evidence, never live reviewer authority or provider-native evidence.
type ReviewerCleanupObligation struct {
	ID             string                          `json:"id"`
	SessionID      SessionID                       `json:"sessionId"`
	ReviewID       string                          `json:"reviewId,omitempty"`
	RunID          string                          `json:"runId,omitempty"`
	Harness        ReviewerHarness                 `json:"harness"`
	HandleID       string                          `json:"handleId"`
	LaunchID       string                          `json:"launchId,omitempty"`
	AgentSessionID string                          `json:"agentSessionId,omitempty"`
	Status         ReviewerCleanupObligationStatus `json:"status"`
	CreatedAt      time.Time                       `json:"createdAt"`
	UpdatedAt      time.Time                       `json:"updatedAt"`
	ResolvedAt     *time.Time                      `json:"resolvedAt,omitempty"`
}

// ReviewerCleanupObligationStatus is the lifecycle of a cleanup-only record.
type ReviewerCleanupObligationStatus string

const (
	// ReviewerCleanupUnresolved marks a runtime whose cleanup is not yet confirmed.
	ReviewerCleanupUnresolved ReviewerCleanupObligationStatus = "unresolved"
	// ReviewerCleanupCleared marks a reviewer cleanup obligation as resolved.
	ReviewerCleanupCleared ReviewerCleanupObligationStatus = "cleared"
)

// ReviewTargetBinding is the immutable launch-time binding for one review pass.
// It deliberately records both the execution authority and the observed
// checkout state: neither may be reconstructed later from mutable session or
// workspace metadata.
type ReviewTargetBinding struct {
	TargetSHA           string    `json:"targetSha"`
	ExecutionBaseSHA    string    `json:"executionBaseSha"`
	DiffBaseSHA         string    `json:"diffBaseSha,omitempty"`
	DiffBaseRef         string    `json:"diffBaseRef,omitempty"`
	WorkspacePath       string    `json:"workspacePath"`
	ObservedHeadSHA     string    `json:"observedHeadSha"`
	ObservedBranch      string    `json:"observedBranch,omitempty"`
	WorkspaceDirty      bool      `json:"workspaceDirty"`
	WorkspaceDirtyKnown bool      `json:"workspaceDirtyKnown"`
	IndexStaged         bool      `json:"indexStaged"`
	IndexStagedKnown    bool      `json:"indexStagedKnown"`
	ObservationPhase    string    `json:"observationPhase"`
	ObservedAt          time.Time `json:"observedAt"`
}

// ReviewEvidenceLevel names the authority of a reviewer setting or identity.
// The values intentionally mirror the control-plane reconciliation vocabulary.
type ReviewEvidenceLevel string

const (
	// ReviewEvidenceRequested identifies settings requested for a review.
	ReviewEvidenceRequested ReviewEvidenceLevel = "REQUESTED"
	// ReviewEvidenceConfigured identifies AO-configured settings, not provider-native evidence.
	ReviewEvidenceConfigured        ReviewEvidenceLevel = "CONFIGURED"
	ReviewEvidenceAOResolved        ReviewEvidenceLevel = "AO_RESOLVED"
	ReviewEvidenceProviderEffective ReviewEvidenceLevel = "PROVIDER_EFFECTIVE"
	ReviewEvidenceProviderNative    ReviewEvidenceLevel = "PROVIDER_NATIVE"
)

// ReviewerSettings is one truth-level snapshot of reviewer execution settings.
// Observed is true only for a fact established at that level; it is not derived
// from the presence of a requested/configured value.
type ReviewerSettings struct {
	Harness    ReviewerHarness `json:"harness,omitempty"`
	Model      string          `json:"model,omitempty"`
	Effort     string          `json:"effort,omitempty"`
	Mode       string          `json:"mode,omitempty"`
	Permission PermissionMode  `json:"permission,omitempty"`
	Observed   bool            `json:"observed"`
}

// ReviewerNativeEvidence is append-only provider evidence correlated by AO.
// Empty provider-effective/native fields remain explicitly unproven.
type ReviewerNativeEvidence struct {
	SessionID    string    `json:"sessionId,omitempty"`
	FirstTurnID  string    `json:"firstTurnId,omitempty"`
	CompletionID string    `json:"completionId,omitempty"`
	ObservedAt   time.Time `json:"observedAt,omitempty"`
}

// ReviewerProvenance preserves every requested/configured/resolved boundary
// without allowing a reviewer submission to assert provider-owned facts.
type ReviewerProvenance struct {
	Requested         ReviewerSettings       `json:"requested"`
	Configured        ReviewerSettings       `json:"configured"`
	AOResolved        ReviewerSettings       `json:"aoResolved"`
	ProviderEffective ReviewerSettings       `json:"providerEffective"`
	ProviderNative    ReviewerNativeEvidence `json:"providerNative"`
}

// ReviewTriggerSource identifies who initiated a review pass.
type ReviewTriggerSource string

const (
	// ReviewTriggerManual marks a user-initiated review pass.
	ReviewTriggerManual ReviewTriggerSource = "manual"
	// ReviewTriggerAuto marks a daemon-initiated review pass.
	ReviewTriggerAuto ReviewTriggerSource = "auto"
)

// ReviewRunStatus is the lifecycle state of a single review pass.
type ReviewRunStatus = contract.AOReviewRunStatus

// Review run statuses.
const (
	ReviewRunRunning   = contract.AOReviewRunRunning
	ReviewRunComplete  = contract.AOReviewRunComplete
	ReviewRunDelivered = contract.AOReviewRunDelivered
	ReviewRunFailed    = contract.AOReviewRunFailed
	ReviewRunCancelled = contract.AOReviewRunCancelled
)

// ReviewVerdict is the outcome a reviewer reports. The empty verdict marks a
// run that has not produced an outcome yet.
type ReviewVerdict = contract.AOReviewVerdict

// Review verdicts.
const (
	VerdictNone             = contract.AOReviewVerdictNone
	VerdictApproved         = contract.AOReviewVerdictApproved
	VerdictChangesRequested = contract.AOReviewVerdictChangesRequested
)

// CurrentHeadReviewRun is one AO review pass recorded against a PR's current
// head commit, reduced to the fields a derived read model needs.
type CurrentHeadReviewRun struct {
	SessionID SessionID
	Harness   ReviewerHarness
	PRURL     string
	Status    ReviewRunStatus
	Verdict   ReviewVerdict
	ID        string
	CreatedAt time.Time
}
