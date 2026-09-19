package ports

import "github.com/aoagents/agent-orchestrator/backend/internal/domain"

// ChatDispatchConformanceStatus is the fail-closed classification of one
// provider dispatch. It is an evidence result, not a provider or session
// lifecycle state.
type ChatDispatchConformanceStatus string

const (
	ChatDispatchConformanceVerified       ChatDispatchConformanceStatus = "VERIFIED"
	ChatDispatchConfigMismatch            ChatDispatchConformanceStatus = "DISPATCH_CONFIG_MISMATCH"
	ChatDispatchEvidenceMissing           ChatDispatchConformanceStatus = "DISPATCH_EVIDENCE_MISSING"
	ChatDispatchEvidenceStale             ChatDispatchConformanceStatus = "DISPATCH_EVIDENCE_STALE"
	ChatDispatchEvidenceWrongSession      ChatDispatchConformanceStatus = "DISPATCH_EVIDENCE_WRONG_SESSION"
	ChatDispatchEvidenceWrongConversation ChatDispatchConformanceStatus = "DISPATCH_EVIDENCE_WRONG_CONVERSATION"
	ChatDispatchEvidenceWrongTurn         ChatDispatchConformanceStatus = "DISPATCH_EVIDENCE_WRONG_TURN"
	ChatDispatchEvidenceUncorrelated      ChatDispatchConformanceStatus = "DISPATCH_EVIDENCE_UNCORRELATED"
	ChatDispatchProviderRejected          ChatDispatchConformanceStatus = "DISPATCH_PROVIDER_REJECTED"
)

// ChatDispatchEvidenceProvenance identifies the surface that supplied a
// value. The Codex response values are authoritative; AO requested/configured
// and outbound values are retained for comparison only.
type ChatDispatchEvidenceProvenance string

const (
	ChatDispatchProvenanceAORequested               ChatDispatchEvidenceProvenance = "ao.requested"
	ChatDispatchProvenanceAOConfigured              ChatDispatchEvidenceProvenance = "ao.configured"
	ChatDispatchProvenanceAODispatched              ChatDispatchEvidenceProvenance = "ao.dispatched"
	ChatDispatchProvenanceCodexThreadStartResponse  ChatDispatchEvidenceProvenance = "codex.thread/start.response"
	ChatDispatchProvenanceCodexThreadResumeResponse ChatDispatchEvidenceProvenance = "codex.thread/resume.response"
	// ChatDispatchProvenanceCodexFirstTurn is the native turn/started identity
	// correlated with the model/effort returned by thread/start or thread/resume.
	ChatDispatchProvenanceCodexFirstTurn ChatDispatchEvidenceProvenance = "codex.first-turn.thread-response+turn/started"
	ChatDispatchProvenanceNotApplicable  ChatDispatchEvidenceProvenance = "not-applicable"
)

// ChatDispatchValues keeps exact provider strings. Callers must not trim,
// alias, normalize, or substitute defaults before verification.
type ChatDispatchValues struct {
	Model  string
	Effort string
}

// ChatDispatchEvidence is one stage in the dispatch evidence chain. Fresh and
// Correlated are explicit producer attestations: the verifier never treats a
// persisted setting, a catalog row, or a later unrelated turn as native proof.
type ChatDispatchEvidence struct {
	Values                 ChatDispatchValues
	Provenance             ChatDispatchEvidenceProvenance
	SessionID              domain.SessionID
	ProviderConversationID string
	ProviderTurnID         string
	Fresh                  bool
	Correlated             bool
	ProviderRejected       bool
}

// ChatDispatchVariantObservation records the one dispatch metadata value that
// this seam cannot observe from Codex. "none" is deliberately not promoted to
// provider evidence.
type ChatDispatchVariantObservation struct {
	Value        string
	Availability string
	Provenance   ChatDispatchEvidenceProvenance
}

const (
	ChatDispatchVariantNone     = "none"
	ChatDispatchNotApplicable   = "not-applicable"
	ChatDispatchVariantMetadata = "ao.dispatch-metadata"
)

// ChatDispatchConformanceInput is the bounded input to a deterministic
// provider-dispatch verification. Requested/configured/dispatched are AO
// observations; ProviderSelected and ObservedFirstTurn must be Codex-native,
// exact, and correlated to the same session, thread, and first turn.
type ChatDispatchConformanceInput struct {
	SessionID              domain.SessionID
	ProviderConversationID string
	FirstProviderTurnID    string
	// EffortOverride preserves the request-presence bit. When true, an empty
	// effort is an explicit provider-default request and must not be replaced by
	// an inherited AO/project effort at any intermediate stage.
	EffortOverride bool

	Requested         ChatDispatchEvidence
	Configured        ChatDispatchEvidence
	Dispatched        ChatDispatchEvidence
	ProviderSelected  ChatDispatchEvidence
	ObservedFirstTurn ChatDispatchEvidence
}

// ChatDispatchConformance is the complete evidence seam result. ChildResultValid
// is false for every non-VERIFIED status; callers must not infer completion from
// a dispatched request or from a provider error-free process alone.
type ChatDispatchConformance struct {
	Status           ChatDispatchConformanceStatus
	Reason           string
	ChildResultValid bool
	EffortOverride   bool

	Requested         ChatDispatchEvidence
	Configured        ChatDispatchEvidence
	Dispatched        ChatDispatchEvidence
	ProviderSelected  ChatDispatchEvidence
	ObservedFirstTurn ChatDispatchEvidence
	Variant           ChatDispatchVariantObservation
}

// ChatDispatchConformanceReporter is optional so existing Chat drivers remain
// source-compatible. Codex exposes it after its first native turn has been
// correlated; no durable result is introduced by this interface.
type ChatDispatchConformanceReporter interface {
	DispatchConformance() ChatDispatchConformance
}
