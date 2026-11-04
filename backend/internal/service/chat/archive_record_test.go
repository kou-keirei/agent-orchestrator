package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// legacyArchiveRow is the archive record projectEvent wrote before empty fields were
// dropped: all 26 keys on every row, "" or null for the ones an event leaves empty.
// Archives already on disk hold rows of this shape, so it is kept here as a fixture.
func legacyArchiveRow(t *testing.T, event ports.ChatEvent) string {
	t.Helper()
	detail := json.RawMessage("null")
	if len(event.Detail) > 0 {
		detail = event.Detail
	}
	record := map[string]any{
		"kind":                   event.Kind,
		"providerEventId":        event.ProviderEventID,
		"providerTurnId":         event.ProviderTurnID,
		"providerConversationId": event.ProviderConversationID,
		"providerItemId":         event.ProviderItemID,
		"providerItemAliases":    event.ProviderItemAliases,
		"clientMessageId":        event.ClientMessageID,
		"turnState":              event.TurnState,
		"delta":                  event.Delta,
		"text":                   event.Text,
		"activityKind":           event.ActivityKind,
		"activityStatus":         event.ActivityStatus,
		"summary":                event.Summary,
		"detail":                 detail,
		"requestId":              event.RequestID,
		"decisions":              event.Decisions,
		"controllerState":        event.ControllerState,
		"usage":                  event.Usage,
		"rateLimits":             event.RateLimits,
		"title":                  event.Title,
		"plan":                   event.Plan,
		"reroute":                event.Reroute,
		"account":                event.Account,
		"threadState":            event.ThreadState,
		"mcpServers":             event.MCPServers,
		"diff":                   event.Diff,
	}
	if event.Input != nil {
		record["input"] = event.Input
	}
	if event.Err != nil {
		record["error"] = event.Err.Error()
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("encode legacy archive row: %v", err)
	}
	return string(encoded)
}

// decodedArchiveRow decodes an archive row into typed values, so a row compares by what
// it says rather than by which empty spelling it used.
type decodedArchiveRow struct {
	Kind                   ports.ChatEventKind
	ProviderEventID        string
	ProviderTurnID         string
	ProviderConversationID string
	ProviderItemID         string
	ProviderItemAliases    []string
	ClientMessageID        string
	TurnState              domain.TurnState
	Delta                  string
	Text                   string
	ActivityKind           domain.ActivityKind
	ActivityStatus         domain.ActivityStatus
	Summary                string
	Detail                 any
	RequestID              string
	Decisions              []ports.ChatDecisionOption
	ControllerState        ports.ChatControllerState
	Usage                  *ports.ChatUsage
	RateLimits             *ports.ChatRateLimits
	Title                  string
	Plan                   *domain.ConversationPlan
	Reroute                *ports.ChatModelReroute
	Account                *ports.ChatAccount
	ThreadState            *ports.ChatThreadState
	MCPServers             []ports.ChatMCPServer
	Diff                   *ports.ChatTurnDiff
	Input                  *ports.ChatInputRequest
	// Error is a pointer because its presence is meaningful: it is archived only for an
	// event that has an error, and an empty message is still an error.
	Error *string
}

func decodeArchived(t *testing.T, row string) decodedArchiveRow {
	t.Helper()
	var decoded decodedArchiveRow
	if err := json.Unmarshal([]byte(row), &decoded); err != nil {
		t.Fatalf("decode archive row %s: %v", row, err)
	}
	return decoded
}

// archiveCorpus gives every key of the record a non-empty value in at least one event.
func archiveCorpus() []ports.ChatEvent {
	failure := errors.New("boom")
	return []ports.ChatEvent{
		{Kind: ports.ChatEventControllerState, ProviderEventID: "ev-controller", ControllerState: ports.ChatControllerReady},
		{Kind: ports.ChatEventTurnStarted, ProviderEventID: "ev-started", ProviderTurnID: "pt-1", ProviderConversationID: "thread-1"},
		{Kind: ports.ChatEventUserMessageCompleted, ProviderEventID: "ev-user", ProviderTurnID: "pt-1",
			ProviderItemID: "user-1", ClientMessageID: "client-1", Text: "run the tests"},
		{Kind: ports.ChatEventMessageDelta, ProviderEventID: "ev-delta", ProviderTurnID: "pt-1",
			ProviderItemID: "msg-1", Delta: "Running"},
		{Kind: ports.ChatEventMessageCompleted, ProviderEventID: "ev-message", ProviderTurnID: "pt-1",
			ProviderItemID: "msg-1", Text: "Running the tests"},
		{Kind: ports.ChatEventActivityStarted, ProviderEventID: "ev-activity", ProviderTurnID: "pt-1",
			ProviderItemID: "cmd-1", ProviderItemAliases: []string{"cmd-1-legacy"},
			ActivityKind: domain.ActivityKindCommand, ActivityStatus: domain.ActivityStatusRunning,
			Summary: "go test ./...", Detail: []byte(`{"command":"go test ./..."}`)},
		{Kind: ports.ChatEventApprovalRequested, ProviderEventID: "ev-approval", ProviderTurnID: "pt-1",
			ProviderItemID: "approval-1", RequestID: "req-1", Summary: "Run a command",
			Decisions: []ports.ChatDecisionOption{{ID: "accept", Label: "Accept", Kind: ports.ChatDecisionAllowOnce}}},
		{Kind: ports.ChatEventInputRequested, ProviderEventID: "ev-input", ProviderTurnID: "pt-1",
			ProviderItemID: "input-1", Input: &ports.ChatInputRequest{Mode: ports.ChatInputModeForm, Message: "Which one?"}},
		{Kind: ports.ChatEventUsage, ProviderEventID: "ev-usage",
			Usage: &ports.ChatUsage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15, TotalsKnown: true}},
		{Kind: ports.ChatEventRateLimits, ProviderEventID: "ev-limits",
			RateLimits: &ports.ChatRateLimits{PrimaryUsedPercent: 12, SecondaryUsedPercent: -1, PlanLabel: "pro"}},
		{Kind: ports.ChatEventThreadRenamed, ProviderEventID: "ev-title", Title: "Fix the flaky test"},
		{Kind: ports.ChatEventPlanUpdated, ProviderEventID: "ev-plan", ProviderTurnID: "pt-1", Summary: "Plan",
			Plan: &domain.ConversationPlan{Steps: []domain.ConversationPlanStep{{Text: "write the test", Status: domain.PlanStepPending}}}},
		{Kind: ports.ChatEventModelRerouted, ProviderEventID: "ev-reroute", ProviderTurnID: "pt-1",
			Reroute: &ports.ChatModelReroute{FromModel: "big", ToModel: "small", Reason: "capacity"}},
		{Kind: ports.ChatEventAccountChanged, ProviderEventID: "ev-account",
			Account: &ports.ChatAccount{AuthMode: "chatgpt", PlanLabel: "pro"}},
		{Kind: ports.ChatEventThreadState, ProviderEventID: "ev-thread",
			ThreadState: &ports.ChatThreadState{Status: domain.ThreadStatusActive}},
		{Kind: ports.ChatEventMCPServers, ProviderEventID: "ev-mcp",
			MCPServers: []ports.ChatMCPServer{{Name: "fs", Status: "ready"}}},
		{Kind: ports.ChatEventTurnDiff, ProviderEventID: "ev-diff", ProviderTurnID: "pt-1",
			Diff: &ports.ChatTurnDiff{Files: []ports.ChatDiffFile{{Path: "a.go", Additions: 1, Status: "modified"}}}},
		{Kind: ports.ChatEventTurnCompleted, ProviderEventID: "ev-completed", ProviderTurnID: "pt-1",
			TurnState: domain.TurnStateCompleted},
		{Kind: ports.ChatEventTurnStarted, ProviderEventID: "ev-started-failed", ProviderTurnID: "pt-2"},
		{Kind: ports.ChatEventTurnCompleted, ProviderEventID: "ev-failed", ProviderTurnID: "pt-2", Err: failure},
		{Kind: ports.ChatEventTurnStarted, ProviderEventID: "ev-started-silent", ProviderTurnID: "pt-3"},
		// An error with no message is still an error.
		{Kind: ports.ChatEventTurnCompleted, ProviderEventID: "ev-silent-failure", ProviderTurnID: "pt-3", Err: errors.New("")},
	}
}

// Capture only the encoder/store boundary here. The controller/store regression
// in controller_test.go separately exercises the real projection transaction.
type archiveCaptureStore struct {
	Store
	payload                                     string
	conversationID, generation, eventID, method string
	session                                     domain.SessionID
}

func (s *archiveCaptureStore) ProjectProviderEvent(_ context.Context, conversationID string, session domain.SessionID, generation, eventID, method, payload string, _ time.Time, _ func(context.Context) error) (bool, error) {
	s.conversationID, s.session, s.generation = conversationID, session, generation
	s.eventID, s.method, s.payload = eventID, method, payload
	return false, nil
}

func captureArchive(t *testing.T, event ports.ChatEvent, harness domain.AgentHarness) string {
	t.Helper()
	s := &archiveCaptureStore{}
	c := &Controller{store: s, harness: harness, sessionID: "session-1", generation: "generation-1", now: time.Now}
	c.conversation.ID = "conversation-1"
	if _, _, err := c.projectEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if s.conversationID != "conversation-1" || s.session != "session-1" || s.generation != "generation-1" || s.eventID != event.ProviderEventID || s.method != string(event.Kind) {
		t.Fatalf("archive identity changed: %+v", s)
	}
	return s.payload
}

func TestCompactArchiveDecoderCompatibility(t *testing.T) {
	corpus := append(archiveCorpus(),
		ports.ChatEvent{Kind: ports.ChatEventMessageDelta, Delta: "anonymous"},
		ports.ChatEvent{Kind: ports.ChatEventMCPServers, MCPServers: []ports.ChatMCPServer{}},
		ports.ChatEvent{Kind: ports.ChatEventApprovalRequested, Decisions: []ports.ChatDecisionOption{}, ProviderItemAliases: []string{}},
		ports.ChatEvent{Kind: ports.ChatEventUsage, Usage: &ports.ChatUsage{}},
		ports.ChatEvent{Kind: ports.ChatEventInputRequested, Input: &ports.ChatInputRequest{}},
	)
	for _, detail := range []string{"null", " null ", "{}", "[]", "false", "0", `""`, `{"empty":[],"ok":false,"count":0,"nested":null}`} {
		corpus = append(corpus, ports.ChatEvent{Kind: ports.ChatEventActivityStarted, Detail: []byte(detail)})
	}
	for i, event := range corpus {
		t.Run(fmt.Sprintf("%d-%s", i, event.Kind), func(t *testing.T) {
			full := legacyArchiveRow(t, event)
			slim := captureArchive(t, event, domain.HarnessClaudeCode)
			if !reflect.DeepEqual(decodeArchived(t, full), decodeArchived(t, slim)) {
				t.Fatalf("typed decode differs:\nfull %s\nslim %s", full, slim)
			}
			var before, after map[string]json.RawMessage
			if err := json.Unmarshal([]byte(full), &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(slim), &after); err != nil {
				t.Fatal(err)
			}
			for key, value := range before {
				got, kept := after[key]
				if kept && string(got) != string(value) {
					t.Errorf("%s changed: %s -> %s", key, value, got)
				}
				// Only scalar empty strings and null pointers can be absent.
				if !kept && string(value) != `""` && string(value) != "null" {
					t.Errorf("populated field %s=%s dropped", key, value)
				}
				if !kept && (key == "input" || key == "error" || key == "detail" || key == "mcpServers" || key == "decisions" || key == "providerItemAliases") {
					t.Errorf("presence-sensitive field %s dropped", key)
				}
			}
			if len(slim) >= len(full) {
				t.Fatalf("not compact: %d >= %d", len(slim), len(full))
			}
		})
	}
}

func TestCompactArchiveKeepsAmbiguousValues(t *testing.T) {
	zero := 0
	keep := map[string]any{
		"false": false, "zero": 0, "float": 0.0, "uint": uint8(0),
		"struct": struct{ N int }{}, "pointer": &zero,
		"nilSlice": []string(nil), "emptySlice": []string{},
		"nilMap": map[string]int(nil), "emptyMap": map[string]int{},
		"rawNull": json.RawMessage("null"), "rawEmpty": json.RawMessage(`""`),
		"rawArray": json.RawMessage("[]"), "rawMap": json.RawMessage("{}"),
	}
	record := map[string]any{"nil": nil, "nilPointer": (*int)(nil), "string": "", "enum": ports.ChatEventKind("")}
	for key, value := range keep {
		record[key] = value
	}
	dropEmptyArchiveFields(record)
	if !reflect.DeepEqual(record, keep) {
		t.Fatalf("changed meaningful values: %#v", record)
	}
}

func TestCompactArchiveCodexRedaction(t *testing.T) {
	for _, event := range []ports.ChatEvent{
		{Kind: ports.ChatEventAccountChanged, Account: &ports.ChatAccount{AuthMode: "secret", PlanLabel: "secret"}},
		{Kind: ports.ChatEventRateLimits, RateLimits: &ports.ChatRateLimits{PrimaryUsedPercent: 91, PlanLabel: "secret"}},
	} {
		row := captureArchive(t, event, domain.HarnessCodex)
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(row), &fields); err != nil {
			t.Fatal(err)
		}
		if _, ok := fields["account"]; ok {
			t.Fatalf("account retained: %s", row)
		}
		if _, ok := fields["rateLimits"]; ok {
			t.Fatalf("limits retained: %s", row)
		}
		if strings.Contains(row, "secret") || strings.Contains(row, "91") {
			t.Fatalf("Codex data leaked: %s", row)
		}
	}
}

func TestCompactArchiveRejectsMalformedDetail(t *testing.T) {
	s := &archiveCaptureStore{}
	c := &Controller{store: s, now: time.Now}
	_, _, err := c.projectEvent(context.Background(), ports.ChatEvent{Detail: []byte("{")})
	if err == nil || !strings.Contains(err.Error(), "encode provider event archive") || s.payload != "" {
		t.Fatalf("invalid detail reached store: %q, %v", s.payload, err)
	}
}
