package codexappserver

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func validDispatchInput() ports.ChatDispatchConformanceInput {
	const (
		session = "child-1"
		thread  = "thread-1"
		turn    = "turn-1"
	)
	input := ports.ChatDispatchConformanceInput{
		SessionID:              session,
		ProviderConversationID: thread,
		FirstProviderTurnID:    turn,
		Requested:              aoDispatchEvidence("gpt-5.6-luna", "high", ports.ChatDispatchProvenanceAORequested),
		Configured:             aoDispatchEvidence("gpt-5.6-luna", "high", ports.ChatDispatchProvenanceAOConfigured),
		Dispatched:             aoDispatchEvidence("gpt-5.6-luna", "high", ports.ChatDispatchProvenanceAODispatched),
		ProviderSelected: ports.ChatDispatchEvidence{
			Values:                 ports.ChatDispatchValues{Model: "gpt-5.6-luna", Effort: "high"},
			Provenance:             ports.ChatDispatchProvenanceCodexThreadStartResponse,
			SessionID:              session,
			ProviderConversationID: thread,
			ProviderTurnID:         turn,
			Fresh:                  true,
			Correlated:             true,
		},
		ObservedFirstTurn: ports.ChatDispatchEvidence{
			Values:                 ports.ChatDispatchValues{Model: "gpt-5.6-luna", Effort: "high"},
			Provenance:             ports.ChatDispatchProvenanceCodexFirstTurn,
			SessionID:              session,
			ProviderConversationID: thread,
			ProviderTurnID:         turn,
			Fresh:                  true,
			Correlated:             true,
		},
	}
	for _, evidence := range []*ports.ChatDispatchEvidence{&input.Requested, &input.Configured, &input.Dispatched} {
		bindDispatchEvidence(evidence, session, thread, turn)
	}
	return input
}

func TestVerifyDispatchConformanceAcceptsExactModelAndEffortEvidence(t *testing.T) {
	result := VerifyDispatchConformance(validDispatchInput())
	if result.Status != ports.ChatDispatchConformanceVerified || !result.ChildResultValid {
		t.Fatalf("result = %+v, want verified child result", result)
	}
	if result.ProviderSelected.Values.Model != "gpt-5.6-luna" || result.ProviderSelected.Values.Effort != "high" {
		t.Fatalf("provider evidence = %+v, want exact native values", result.ProviderSelected.Values)
	}
	if result.Variant.Value != ports.ChatDispatchVariantNone || result.Variant.Availability != ports.ChatDispatchNotApplicable {
		t.Fatalf("variant = %+v, want none/not-applicable", result.Variant)
	}
}

func TestVerifyDispatchConformanceRejectsExplicitMaxWhenProviderReportsXHigh(t *testing.T) {
	input := validDispatchInput()
	input.Requested.Values.Effort = "max"
	input.Configured.Values.Effort = "max"
	input.Dispatched.Values.Effort = "max"
	input.ProviderSelected.Values.Effort = "xhigh"
	input.ObservedFirstTurn.Values.Effort = "xhigh"

	result := VerifyDispatchConformance(input)
	if result.Status != ports.ChatDispatchConfigMismatch || result.ChildResultValid {
		t.Fatalf("result = %+v, want DISPATCH_CONFIG_MISMATCH and invalid child result", result)
	}
}

func TestVerifyDispatchConformanceRejectsExplicitEmptyInheritedEffort(t *testing.T) {
	input := validDispatchInput()
	input.EffortOverride = true
	input.Requested.Values.Effort = ""

	result := VerifyDispatchConformance(input)
	if result.Status != ports.ChatDispatchConfigMismatch || result.ChildResultValid {
		t.Fatalf("result = %+v, want explicit-empty mismatch against inherited effort", result)
	}
}

func TestVerifyDispatchConformanceAcceptsExplicitEmptyProviderDefault(t *testing.T) {
	input := validDispatchInput()
	input.EffortOverride = true
	input.Requested.Values.Effort = ""
	input.Configured.Values.Effort = ""
	input.Dispatched.Values.Effort = ""
	input.ProviderSelected.Values.Effort = "low"
	input.ObservedFirstTurn.Values.Effort = "low"

	result := VerifyDispatchConformance(input)
	if result.Status != ports.ChatDispatchConformanceVerified || !result.ChildResultValid {
		t.Fatalf("result = %+v, want provider-default verification", result)
	}
	if !result.EffortOverride {
		t.Fatal("result lost explicit-empty effort presence")
	}
}

func TestVerifyDispatchConformanceFailsClosedForUnusableEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ports.ChatDispatchConformanceInput)
		status ports.ChatDispatchConformanceStatus
	}{
		{
			name:   "missing",
			mutate: func(input *ports.ChatDispatchConformanceInput) { input.ProviderSelected = ports.ChatDispatchEvidence{} },
			status: ports.ChatDispatchEvidenceMissing,
		},
		{
			name: "stale",
			mutate: func(input *ports.ChatDispatchConformanceInput) {
				input.ProviderSelected.Fresh = false
			},
			status: ports.ChatDispatchEvidenceStale,
		},
		{
			name: "wrong session",
			mutate: func(input *ports.ChatDispatchConformanceInput) {
				input.ProviderSelected.SessionID = "other-child"
			},
			status: ports.ChatDispatchEvidenceWrongSession,
		},
		{
			name: "provider rejected",
			mutate: func(input *ports.ChatDispatchConformanceInput) {
				input.ProviderSelected.ProviderRejected = true
			},
			status: ports.ChatDispatchProviderRejected,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validDispatchInput()
			test.mutate(&input)
			result := VerifyDispatchConformance(input)
			if result.Status != test.status || result.ChildResultValid {
				t.Fatalf("result = %+v, want status %q and childResultValid=false", result, test.status)
			}
		})
	}
}

func TestVerifyDispatchConformanceRequiresTheExactFirstTurn(t *testing.T) {
	input := validDispatchInput()
	input.ObservedFirstTurn.ProviderTurnID = "turn-2"
	result := VerifyDispatchConformance(input)
	if result.Status != ports.ChatDispatchEvidenceWrongTurn || result.ChildResultValid {
		t.Fatalf("result = %+v, want wrong-turn invalid result", result)
	}

	input = validDispatchInput()
	input.ProviderSelected.ProviderTurnID = "turn-2"
	result = VerifyDispatchConformance(input)
	if result.Status != ports.ChatDispatchEvidenceWrongTurn || result.ChildResultValid {
		t.Fatalf("provider-selected result = %+v, want wrong-turn invalid result", result)
	}
}

func TestStartReportsConformanceOnlyAfterNativeFirstTurnCorrelation(t *testing.T) {
	d, srv := newTestDriver(t)
	workspace := t.TempDir()
	srv.reply("thread/start", `{"thread":{"id":"thread-1"},"model":"gpt-5.6-luna","reasoningEffort":"high","cwd":"/tmp/ws"}`)

	conv, err := d.Start(context.Background(), ports.ChatStartConfig{
		SessionID:     "child-1",
		WorkspacePath: workspace,
		Model:         "gpt-5.6-luna",
		Effort:        "high",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = conv.Close() }()

	reporter, ok := conv.(ports.ChatDispatchConformanceReporter)
	if !ok {
		t.Fatal("conversation does not expose dispatch conformance")
	}
	if got := reporter.DispatchConformance(); got.ChildResultValid {
		t.Fatalf("pre-first-turn result = %+v, want invalid until a first turn", got)
	}

	if _, err := conv.SendTurn(context.Background(), ports.ChatUserMessage{Text: "hello"}); err != nil {
		t.Fatalf("SendTurn: %v", err)
	}
	srv.push(`{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"inProgress","items":[]}}}`)
	_ = nextEvent(t, conv.Events(), ports.ChatEventTurnStarted)

	got := reporter.DispatchConformance()
	if got.Status != ports.ChatDispatchConformanceVerified || !got.ChildResultValid {
		t.Fatalf("post-first-turn result = %+v, want verified", got)
	}
	if got.ObservedFirstTurn.ProviderTurnID != "turn-1" {
		t.Fatalf("observed first turn = %+v, want turn-1", got.ObservedFirstTurn)
	}
}

func TestFirstTurnResponseRemainsTheCorrelationAnchorWhenNotificationRaces(t *testing.T) {
	d, srv := newTestDriver(t)
	workspace := t.TempDir()
	srv.reply("thread/start", `{"thread":{"id":"thread-1"},"model":"gpt-5.6-luna","reasoningEffort":"high"}`)

	conv, err := d.Start(context.Background(), ports.ChatStartConfig{
		SessionID:     "child-1",
		WorkspacePath: workspace,
		Model:         "gpt-5.6-luna",
		Effort:        "high",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = conv.Close() }()

	concrete := conv.(*conversation)
	concrete.observeFirstTurn([]byte(`{"threadId":"thread-1","turn":{"id":"turn-2"}}`))
	concrete.recordDispatchedFirstTurn("turn-1", ports.ChatTurnSettings{})

	got := conv.(ports.ChatDispatchConformanceReporter).DispatchConformance()
	if got.Status != ports.ChatDispatchEvidenceWrongTurn || got.ChildResultValid {
		t.Fatalf("racing result = %+v, want wrong-turn invalid result", got)
	}
}

func TestFirstTurnSettingsDoNotReuseStaleThreadDispatchEvidence(t *testing.T) {
	d, srv := newTestDriver(t)
	workspace := t.TempDir()
	srv.reply("thread/start", `{"thread":{"id":"thread-1"},"model":"gpt-5.6-luna","reasoningEffort":"high"}`)

	conv, err := d.Start(context.Background(), ports.ChatStartConfig{
		SessionID: "child-1", WorkspacePath: workspace, Model: "gpt-5.6-luna", Effort: "high",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = conv.Close() }()
	if _, err := conv.SendTurn(context.Background(), ports.ChatUserMessage{
		Text: "hello", Settings: ports.ChatTurnSettings{Model: "gpt-5.6-followup", Effort: "medium"},
	}); err != nil {
		t.Fatalf("SendTurn: %v", err)
	}
	srv.push(`{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"inProgress","items":[]}}}`)
	_ = nextEvent(t, conv.Events(), ports.ChatEventTurnStarted)

	got := conv.(ports.ChatDispatchConformanceReporter).DispatchConformance()
	if got.Status != ports.ChatDispatchConfigMismatch || got.ChildResultValid {
		t.Fatalf("post-settings result = %+v, want config mismatch and invalid", got)
	}
	if got.Dispatched.Values.Model != "gpt-5.6-followup" || got.Dispatched.Values.Effort != "medium" {
		t.Fatalf("dispatched first-turn values = %+v, want per-turn values", got.Dispatched.Values)
	}
	for name, evidence := range map[string]ports.ChatDispatchEvidence{
		"requested":  got.Requested,
		"configured": got.Configured,
		"dispatched": got.Dispatched,
		"provider":   got.ProviderSelected,
	} {
		if evidence.ProviderTurnID != "turn-1" || !evidence.Correlated {
			t.Fatalf("%s evidence = %+v, want first-turn correlation", name, evidence)
		}
	}
}
