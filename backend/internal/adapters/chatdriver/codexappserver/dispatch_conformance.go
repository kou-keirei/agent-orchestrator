package codexappserver

import (
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func aoDispatchEvidence(model, effort string, provenance ports.ChatDispatchEvidenceProvenance) ports.ChatDispatchEvidence {
	return ports.ChatDispatchEvidence{
		Values:     ports.ChatDispatchValues{Model: model, Effort: effort},
		Provenance: provenance,
	}
}

func newDispatchConformanceInput(
	sessionID domain.SessionID,
	providerConversationID, requestedModel, requestedEffort string,
	effortOverride bool,
	providerProvenance ports.ChatDispatchEvidenceProvenance,
	providerValues ports.ChatDispatchValues,
) ports.ChatDispatchConformanceInput {
	requested := aoDispatchEvidence(requestedModel, requestedEffort, ports.ChatDispatchProvenanceAORequested)
	configured := aoDispatchEvidence(requestedModel, requestedEffort, ports.ChatDispatchProvenanceAOConfigured)
	dispatched := aoDispatchEvidence(requestedModel, requestedEffort, ports.ChatDispatchProvenanceAODispatched)
	for _, evidence := range []*ports.ChatDispatchEvidence{&requested, &configured, &dispatched} {
		evidence.SessionID = sessionID
		evidence.ProviderConversationID = providerConversationID
		evidence.Fresh = true
	}
	providerEvidence := providerProvenance == ports.ChatDispatchProvenanceCodexThreadStartResponse ||
		providerProvenance == ports.ChatDispatchProvenanceCodexThreadResumeResponse
	return ports.ChatDispatchConformanceInput{
		SessionID:              sessionID,
		ProviderConversationID: providerConversationID,
		EffortOverride:         effortOverride,
		Requested:              requested,
		Configured:             configured,
		Dispatched:             dispatched,
		ProviderSelected: ports.ChatDispatchEvidence{
			Values:                 providerValues,
			Provenance:             providerProvenance,
			SessionID:              sessionID,
			ProviderConversationID: providerConversationID,
			Fresh:                  providerEvidence,
			Correlated:             providerEvidence,
		},
	}
}

// VerifyDispatchConformance checks one exact model/effort dispatch against
// Codex's native thread response and the first native turn identity. It is pure
// by design: no settings are read, no defaults are applied, and no result is
// persisted here.
func VerifyDispatchConformance(input ports.ChatDispatchConformanceInput) ports.ChatDispatchConformance {
	result := ports.ChatDispatchConformance{
		EffortOverride:    input.EffortOverride,
		Requested:         input.Requested,
		Configured:        input.Configured,
		Dispatched:        input.Dispatched,
		ProviderSelected:  input.ProviderSelected,
		ObservedFirstTurn: input.ObservedFirstTurn,
		Variant: ports.ChatDispatchVariantObservation{
			Value:        ports.ChatDispatchVariantNone,
			Availability: ports.ChatDispatchNotApplicable,
			Provenance:   ports.ChatDispatchVariantMetadata,
		},
	}

	invalid := func(status ports.ChatDispatchConformanceStatus, reason string) ports.ChatDispatchConformance {
		result.Status = status
		result.Reason = reason
		result.ChildResultValid = false
		return result
	}
	verified := func() ports.ChatDispatchConformance {
		result.Status = ports.ChatDispatchConformanceVerified
		result.Reason = "exact Codex model and effort evidence is fresh and correlated to the first turn"
		result.ChildResultValid = true
		return result
	}

	if input.ProviderSelected.ProviderRejected || input.ObservedFirstTurn.ProviderRejected {
		return invalid(ports.ChatDispatchProviderRejected,
			"Codex rejected the model or effort dispatch")
	}

	if input.SessionID == "" || input.ProviderConversationID == "" || input.FirstProviderTurnID == "" {
		return invalid(ports.ChatDispatchEvidenceMissing,
			"session, provider conversation, and first provider turn identifiers are required")
	}

	if reason := compareAOSelections(input); reason != "" {
		return invalid(ports.ChatDispatchConfigMismatch, reason)
	}

	if status, reason := validateAOEvidence(input); reason != "" {
		return invalid(status, reason)
	}

	if status, reason := validateNativeEvidence(
		"provider selection", input.ProviderSelected, input, true,
	); reason != "" {
		return invalid(status, reason)
	}
	if status, reason := validateNativeEvidence(
		"observed first turn", input.ObservedFirstTurn, input, false,
	); reason != "" {
		return invalid(status, reason)
	}

	if reason := compareNativeSelections(input); reason != "" {
		return invalid(ports.ChatDispatchConfigMismatch, reason)
	}
	return verified()
}

func compareAOSelections(input ports.ChatDispatchConformanceInput) string {
	if input.EffortOverride &&
		(input.Requested.Values.Effort != input.Configured.Values.Effort ||
			input.Configured.Values.Effort != input.Dispatched.Values.Effort) {
		return fmt.Sprintf(
			"explicit effort override presence/value changed across AO stages: requested=%q configured=%q dispatched=%q",
			input.Requested.Values.Effort, input.Configured.Values.Effort, input.Dispatched.Values.Effort,
		)
	}

	model, modelSource := firstDispatchValue(
		input.Requested.Values.Model, "requested model",
		input.Configured.Values.Model, "configured model",
		input.Dispatched.Values.Model, "dispatched model",
	)
	effort, effortSource := firstDispatchValue(
		input.Requested.Values.Effort, "requested effort",
		input.Configured.Values.Effort, "configured effort",
		input.Dispatched.Values.Effort, "dispatched effort",
	)

	for _, candidate := range []struct {
		value, name string
	}{
		{input.Requested.Values.Model, "requested model"},
		{input.Configured.Values.Model, "configured model"},
		{input.Dispatched.Values.Model, "dispatched model"},
	} {
		if candidate.value != "" && model != "" && candidate.value != model {
			return fmt.Sprintf("model mismatch: %s=%q, %s=%q", modelSource, model, candidate.name, candidate.value)
		}
	}
	for _, candidate := range []struct {
		value, name string
	}{
		{input.Requested.Values.Effort, "requested effort"},
		{input.Configured.Values.Effort, "configured effort"},
		{input.Dispatched.Values.Effort, "dispatched effort"},
	} {
		if candidate.value != "" && effort != "" && candidate.value != effort {
			return fmt.Sprintf("effort mismatch: %s=%q, %s=%q", effortSource, effort, candidate.name, candidate.value)
		}
	}

	// An explicit value that never crossed the provider boundary is a dispatch
	// mismatch, not permission to treat the provider default as equivalent.
	if model != "" && input.Dispatched.Values.Model == "" {
		return fmt.Sprintf("model %q was configured but not dispatched", model)
	}
	if effort != "" && input.Dispatched.Values.Effort == "" {
		return fmt.Sprintf("effort %q was configured but not dispatched", effort)
	}
	return ""
}

func firstDispatchValue(values ...string) (string, string) {
	for i := 0; i+1 < len(values); i += 2 {
		if values[i] != "" {
			return values[i], values[i+1]
		}
	}
	return "", ""
}

func validateAOEvidence(input ports.ChatDispatchConformanceInput) (ports.ChatDispatchConformanceStatus, string) {
	for _, stage := range []struct {
		name     string
		evidence ports.ChatDispatchEvidence
	}{
		{"requested", input.Requested},
		{"configured", input.Configured},
		{"dispatched", input.Dispatched},
	} {
		if (stage.evidence.Values.Model != "" || stage.evidence.Values.Effort != "") && stage.evidence.Provenance == "" {
			return ports.ChatDispatchEvidenceMissing, fmt.Sprintf("%s model/effort values have no provenance", stage.name)
		}
		if stage.evidence.SessionID == "" || stage.evidence.ProviderConversationID == "" {
			return ports.ChatDispatchEvidenceMissing, fmt.Sprintf("%s is missing session or provider conversation correlation", stage.name)
		}
		if stage.evidence.SessionID != input.SessionID {
			return ports.ChatDispatchEvidenceWrongSession, fmt.Sprintf("%s belongs to session %q, want %q", stage.name, stage.evidence.SessionID, input.SessionID)
		}
		if stage.evidence.ProviderConversationID != input.ProviderConversationID {
			return ports.ChatDispatchEvidenceWrongConversation, fmt.Sprintf("%s belongs to provider conversation %q, want %q", stage.name, stage.evidence.ProviderConversationID, input.ProviderConversationID)
		}
		if input.FirstProviderTurnID != "" {
			if stage.evidence.ProviderTurnID == "" {
				return ports.ChatDispatchEvidenceMissing, fmt.Sprintf("%s is missing first provider turn correlation", stage.name)
			}
			if stage.evidence.ProviderTurnID != input.FirstProviderTurnID {
				return ports.ChatDispatchEvidenceWrongTurn, fmt.Sprintf("%s belongs to provider turn %q, want first turn %q", stage.name, stage.evidence.ProviderTurnID, input.FirstProviderTurnID)
			}
			if !stage.evidence.Fresh {
				return ports.ChatDispatchEvidenceStale, fmt.Sprintf("%s is stale", stage.name)
			}
			if !stage.evidence.Correlated {
				return ports.ChatDispatchEvidenceUncorrelated, fmt.Sprintf("%s is not correlated to the first provider turn", stage.name)
			}
		}
	}
	return "", ""
}

func bindDispatchEvidence(evidence *ports.ChatDispatchEvidence, sessionID domain.SessionID, providerConversationID, providerTurnID string) {
	evidence.SessionID = sessionID
	evidence.ProviderConversationID = providerConversationID
	evidence.ProviderTurnID = providerTurnID
	evidence.Fresh = providerTurnID != ""
	evidence.Correlated = providerTurnID != ""
}

func validateNativeEvidence(
	stage string,
	evidence ports.ChatDispatchEvidence,
	input ports.ChatDispatchConformanceInput,
	providerSelection bool,
) (ports.ChatDispatchConformanceStatus, string) {
	if evidence.Values.Model == "" || evidence.Values.Effort == "" {
		return ports.ChatDispatchEvidenceMissing,
			fmt.Sprintf("%s is missing an exact model and effort", stage)
	}
	if evidence.SessionID == "" || evidence.ProviderConversationID == "" || evidence.ProviderTurnID == "" {
		return ports.ChatDispatchEvidenceMissing,
			fmt.Sprintf("%s is missing session, conversation, or turn correlation", stage)
	}
	if evidence.SessionID != input.SessionID {
		return ports.ChatDispatchEvidenceWrongSession,
			fmt.Sprintf("%s belongs to session %q, want %q", stage, evidence.SessionID, input.SessionID)
	}
	if evidence.ProviderConversationID != input.ProviderConversationID {
		return ports.ChatDispatchEvidenceWrongConversation,
			fmt.Sprintf("%s belongs to provider conversation %q, want %q", stage, evidence.ProviderConversationID, input.ProviderConversationID)
	}
	if evidence.ProviderTurnID != input.FirstProviderTurnID {
		return ports.ChatDispatchEvidenceWrongTurn,
			fmt.Sprintf("%s belongs to provider turn %q, want first turn %q", stage, evidence.ProviderTurnID, input.FirstProviderTurnID)
	}
	if !evidence.Fresh {
		return ports.ChatDispatchEvidenceStale,
			fmt.Sprintf("%s is stale", stage)
	}
	if !evidence.Correlated {
		return ports.ChatDispatchEvidenceUncorrelated,
			fmt.Sprintf("%s is not explicitly correlated to the AO child and first turn", stage)
	}
	if providerSelection {
		if evidence.Provenance != ports.ChatDispatchProvenanceCodexThreadStartResponse &&
			evidence.Provenance != ports.ChatDispatchProvenanceCodexThreadResumeResponse {
			return ports.ChatDispatchEvidenceUncorrelated,
				fmt.Sprintf("provider selection provenance %q is not a Codex thread/start or thread/resume response", evidence.Provenance)
		}
	} else if evidence.Provenance != ports.ChatDispatchProvenanceCodexFirstTurn {
		return ports.ChatDispatchEvidenceUncorrelated,
			fmt.Sprintf("first-turn provenance %q is not the Codex thread response correlated with turn/started", evidence.Provenance)
	}
	return "", ""
}

func compareNativeSelections(input ports.ChatDispatchConformanceInput) string {
	provider := input.ProviderSelected.Values
	observed := input.ObservedFirstTurn.Values

	if provider.Model != observed.Model {
		return fmt.Sprintf("provider-selected model %q differs from first-turn model %q", provider.Model, observed.Model)
	}
	if provider.Effort != observed.Effort {
		return fmt.Sprintf("provider-selected effort %q differs from first-turn effort %q", provider.Effort, observed.Effort)
	}

	model, _ := firstDispatchValue(
		input.Requested.Values.Model,
		input.Configured.Values.Model,
		input.Dispatched.Values.Model,
	)
	effort, _ := firstDispatchValue(
		input.Requested.Values.Effort,
		input.Configured.Values.Effort,
		input.Dispatched.Values.Effort,
	)
	if model != "" && provider.Model != model {
		return fmt.Sprintf("provider-selected model %q differs from exact dispatched model %q", provider.Model, model)
	}
	if effort != "" && provider.Effort != effort {
		return fmt.Sprintf("provider-selected effort %q differs from exact dispatched effort %q", provider.Effort, effort)
	}
	return ""
}
