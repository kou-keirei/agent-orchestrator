package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// Every scripted reply below is a SINGLE line. readFrame is newline-delimited, so a
// pretty-printed reply is read as a truncated frame and the client waits forever.

// threadWithTurns is a thread/read result carrying an ordered turn list.
const threadWithTurns = `{"thread":{"id":"thread-1","turns":[` +
	`{"id":"turn-a","status":"completed"},` +
	`{"id":"turn-b","status":"completed"},` +
	`{"id":"turn-c","status":"completed"}]}}`

const threadWithRenderedHistory = `{"thread":{"id":"thread-1","turns":[` +
	`{"id":"turn-a","status":"completed","items":[` +
	`{"type":"userMessage","id":"user-1","clientId":"client-1","content":[{"type":"text","text":"Inspect the repository"}]},` +
	`{"type":"commandExecution","id":"cmd-1","command":"/bin/zsh -lc 'pwd'","cwd":"/tmp/ws","aggregatedOutput":"/tmp/ws\n","exitCode":0,"durationMs":12,"status":"completed"},` +
	`{"type":"agentMessage","id":"answer-1","text":"The repository is ready."}` +
	`]}]}}`

func openConversation(t *testing.T) (*conversation, *scriptedServer) {
	t.Helper()
	d, srv := newTestDriver(t)
	conv, err := d.Start(context.Background(), ports.ChatStartConfig{WorkspacePath: "/tmp/ws"})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = conv.Close() })
	return conv.(*conversation), srv
}

func TestReadHistoryIgnoresEmptyCompletedTurnError(t *testing.T) {
	conv, srv := openProviderFailureConversation(t)
	srv.reply("thread/read", `{"thread":{"id":"thread-1","turns":[{"id":"turn-a","status":"completed","error":{}}]}}`)
	events, err := conv.ReadHistory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Kind != ports.ChatEventTurnCompleted || events[1].Err != nil || events[1].TurnState != "completed" {
		t.Fatalf("empty error changed history: %#v", events)
	}
}

func TestReadHistoryReconstructsNativeTurnsForTheChatTimeline(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", threadWithRenderedHistory)

	events, err := conv.ReadHistory(context.Background())
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("events = %d, want start + user + command + assistant + completion: %#v", len(events), events)
	}
	wantKinds := []ports.ChatEventKind{
		ports.ChatEventTurnStarted,
		ports.ChatEventUserMessageCompleted,
		ports.ChatEventActivityCompleted,
		ports.ChatEventMessageCompleted,
		ports.ChatEventTurnCompleted,
	}
	seenIDs := map[string]bool{}
	for i, event := range events {
		if event.Kind != wantKinds[i] {
			t.Errorf("event %d kind = %q, want %q", i, event.Kind, wantKinds[i])
		}
		if event.ProviderEventID == "" || seenIDs[event.ProviderEventID] {
			t.Errorf("event %d has missing or duplicate identity %q", i, event.ProviderEventID)
		}
		seenIDs[event.ProviderEventID] = true
		if event.ProviderTurnID != "turn-a" {
			t.Errorf("event %d turn = %q, want turn-a", i, event.ProviderTurnID)
		}
	}
	if events[1].Text != "Inspect the repository" || events[1].ClientMessageID != "client-1" {
		t.Errorf("recovered user message = %#v", events[1])
	}
	if events[2].ActivityKind != "command" || !strings.Contains(string(events[2].Detail), `/tmp/ws\n`) {
		t.Errorf("recovered command = %#v", events[2])
	}
	if events[3].Text != "The repository is ready." {
		t.Errorf("recovered answer = %q", events[3].Text)
	}
	if events[4].TurnState != "completed" {
		t.Errorf("recovered turn state = %q", events[4].TurnState)
	}
}

func TestReadHistoryPreservesStructuredProviderFailure(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", `{"thread":{"id":"thread-1","turns":[`+
		`{"id":"turn-a","status":"failed","items":[],"error":{"message":"Usage limit reached","additionalDetails":"Resets tomorrow."}}`+
		`]}}`)

	events, err := conv.ReadHistory(context.Background())
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(events) != 2 || events[1].Kind != ports.ChatEventTurnCompleted {
		t.Fatalf("events = %#v", events)
	}
	if events[1].Err == nil || events[1].Err.Error() != "Usage limit reached\n\nResets tomorrow." {
		t.Fatalf("completion error = %#v", events[1].Err)
	}
}

func TestReadHistoryMakesMissingItemIDsUniqueAcrossTurns(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", `{"thread":{"id":"thread-1","turns":[`+
		`{"id":"turn-a","status":"completed","items":[{"type":"agentMessage","text":"first"}]},`+
		`{"id":"turn-b","status":"completed","items":[{"type":"agentMessage","text":"second"}]}`+
		`]}}`)

	events, err := conv.ReadHistory(context.Background())
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	var itemIDs []string
	for _, event := range events {
		if event.Kind == ports.ChatEventMessageCompleted {
			itemIDs = append(itemIDs, event.ProviderItemID)
		}
	}
	if len(itemIDs) != 2 || itemIDs[0] == "" || itemIDs[0] == itemIDs[1] {
		t.Fatalf("provider item ids = %#v, want two conversation-wide identities", itemIDs)
	}
}

func TestReadHistoryRejectsAnUnsettledNativeTurnInsteadOfOmittingIt(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", `{"thread":{"id":"thread-1","turns":[`+
		`{"id":"turn-a","status":"completed","items":[{"type":"agentMessage","text":"settled"}]},`+
		`{"id":"turn-b","status":"inProgress","items":[{"type":"agentMessage","text":"partial"}]}`+
		`]}}`)

	events, err := conv.ReadHistory(context.Background())
	if !errors.Is(err, ports.ErrChatHistoryUnsettled) {
		t.Fatalf("ReadHistory error = %v, want ErrChatHistoryUnsettled", err)
	}
	if len(events) != 0 {
		t.Fatalf("ReadHistory returned %d partial events with an unsettled turn", len(events))
	}
}

func TestRefreshHistoryPerformsANewThreadRead(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", `{"thread":{"id":"thread-1","turns":[`+
		`{"id":"turn-a","status":"inProgress"}`+
		`]}}`)

	if _, err := conv.ReadHistory(context.Background()); !errors.Is(err, ports.ErrChatHistoryUnsettled) {
		t.Fatalf("ReadHistory error = %v, want ErrChatHistoryUnsettled", err)
	}

	srv.reply("thread/read", `{"thread":{"id":"thread-1","turns":[`+
		`{"id":"turn-a","status":"completed"}`+
		`]}}`)
	events, err := conv.RefreshHistory(context.Background())
	if err != nil {
		t.Fatalf("RefreshHistory: %v", err)
	}
	if len(events) != 2 || events[0].Kind != ports.ChatEventTurnStarted ||
		events[1].Kind != ports.ChatEventTurnCompleted || events[1].TurnState != "completed" {
		t.Fatalf("refreshed events = %#v, want settled turn replay", events)
	}

	srv.mu.Lock()
	reads := 0
	for _, seen := range srv.seen {
		if seen.Method == "thread/read" {
			reads++
		}
	}
	srv.mu.Unlock()
	if reads != 2 {
		t.Fatalf("thread/read requests = %d, want one initial read and one refresh", reads)
	}
}

// The provider takes a COUNT from the end of the thread, not a turn id, so the whole
// correctness of rollback rests on turning the named turn into the right number.
// Naming the middle of three turns must discard that turn and the one after it.
func TestRollbackCountsTurnsFromTheNamedOneToTheEnd(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", threadWithTurns)
	srv.reply("thread/rollback", `{"thread":{"id":"thread-1","turns":[{"id":"turn-a","status":"completed"}]}}`)

	if err := conv.Rollback(context.Background(), "turn-b"); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	read := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/read" })
	var readParams struct {
		ThreadID     string `json:"threadId"`
		IncludeTurns bool   `json:"includeTurns"`
	}
	if err := json.Unmarshal(read.Params, &readParams); err != nil {
		t.Fatalf("thread/read params: %v", err)
	}
	if readParams.ThreadID != "thread-1" || !readParams.IncludeTurns {
		t.Errorf("thread/read params = %+v, want thread-1 with turns", readParams)
	}

	rollback := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/rollback" })
	var params struct {
		ThreadID string `json:"threadId"`
		NumTurns int    `json:"numTurns"`
	}
	if err := json.Unmarshal(rollback.Params, &params); err != nil {
		t.Fatalf("thread/rollback params: %v", err)
	}
	if params.ThreadID != "thread-1" {
		t.Errorf("threadId = %q, want thread-1", params.ThreadID)
	}
	if params.NumTurns != 2 {
		t.Errorf("numTurns = %d, want 2 (the named turn and the one after it)", params.NumTurns)
	}
}

// Naming the last turn is the common case and must ask for exactly one turn: the
// provider rejects numTurns < 1, and asking for zero would be a silent no-op.
func TestRollbackOfTheLastTurnDiscardsOne(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", threadWithTurns)
	srv.reply("thread/rollback", `{"thread":{"id":"thread-1","turns":[]}}`)

	if err := conv.Rollback(context.Background(), "turn-c"); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	rollback := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/rollback" })
	var params struct {
		NumTurns int `json:"numTurns"`
	}
	if err := json.Unmarshal(rollback.Params, &params); err != nil {
		t.Fatalf("params: %v", err)
	}
	if params.NumTurns != 1 {
		t.Errorf("numTurns = %d, want 1", params.NumTurns)
	}
}

// A turn the provider does not have must not produce a rollback at all. Guessing a
// count would discard turns the user never named.
func TestRollbackRefusesATurnTheProviderDoesNotHave(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", threadWithTurns)

	err := conv.Rollback(context.Background(), "turn-zzz")
	if err == nil {
		t.Fatal("Rollback of an unknown turn succeeded")
	}
	if !isRefusal(err) {
		t.Errorf("err = %v, want a provider refusal", err)
	}
	if srv.sentMethod("thread/rollback") {
		t.Fatal("thread/rollback was sent for a turn the provider does not have")
	}
}

// The provider refuses a rollback while a turn runs. That refusal must reach the
// caller as a conflict, not as an internal failure: it is an ordinary thing to press
// undo a moment too early.
func TestRollbackTranslatesTheMidTurnRefusal(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/read", threadWithTurns)
	srv.replyError("thread/rollback", -32600, "Cannot rollback while a turn is in progress.")

	err := conv.Rollback(context.Background(), "turn-c")
	if err == nil {
		t.Fatal("Rollback succeeded despite the provider refusing")
	}
	if !isRefusal(err) {
		t.Errorf("err = %v, want a provider refusal", err)
	}
	if !strings.Contains(err.Error(), "turn is in progress") {
		t.Errorf("err = %v, want the provider's own explanation carried through", err)
	}
}

// A transport failure is NOT a refusal. Reporting it as one would tell the user to
// stop the agent when the real problem is that the provider is gone.
func TestRollbackKeepsATransportFailureAsAFailure(t *testing.T) {
	conv, srv := openConversation(t)
	srv.replyError("thread/read", -32603, "internal error")

	err := conv.Rollback(context.Background(), "turn-c")
	if err == nil {
		t.Fatal("Rollback succeeded despite thread/read failing")
	}
	if isRefusal(err) {
		t.Errorf("err = %v, want a plain failure rather than a refusal", err)
	}
}

func TestForkReturnsTheNewThreadIDAndInheritsTheWorkingDirectory(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/fork", `{"thread":{"id":"thread-2","forkedFromId":"thread-1"},"cwd":"/tmp/ws"}`)

	forked, err := conv.Fork(context.Background(), nil)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if forked != "thread-2" {
		t.Errorf("forked thread = %q, want thread-2", forked)
	}

	fork := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/fork" })
	var params map[string]any
	if err := json.Unmarshal(fork.Params, &params); err != nil {
		t.Fatalf("thread/fork params: %v", err)
	}
	if params["threadId"] != "thread-1" {
		t.Errorf("threadId = %v, want thread-1", params["threadId"])
	}
	// cwd must be absent so the fork inherits the source thread's directory. A fork
	// pointed at another tree would remember editing files that are not there.
	if _, ok := params["cwd"]; ok {
		t.Errorf("thread/fork sent a cwd (%v); it must inherit the source thread's", params["cwd"])
	}
	// lastTurnId must be absent too: ChatForker names no turn, and a fork that
	// quietly truncated would be a rollback wearing the wrong name.
	if _, ok := params["lastTurnId"]; ok {
		t.Error("thread/fork sent a lastTurnId; a fork copies the whole history")
	}
}

func TestForkThroughTurnSendsLastTurnID(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/fork", `{"thread":{"id":"thread-2"},"cwd":"/tmp/ws"}`)
	anchor := "turn-before-edit"

	forked, err := conv.Fork(context.Background(), &anchor)
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if forked != "thread-2" {
		t.Fatalf("forked thread = %q, want thread-2", forked)
	}
	frame := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/fork" })
	var params map[string]any
	if err := json.Unmarshal(frame.Params, &params); err != nil {
		t.Fatalf("decode params: %v", err)
	}
	if params["lastTurnId"] != anchor {
		t.Fatalf("lastTurnId = %#v, want %q", params["lastTurnId"], anchor)
	}
	if _, present := params["cwd"]; present {
		t.Fatal("fork must inherit the source cwd")
	}
}

func TestForkRejectsAResponseWithNoThreadID(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/fork", `{"thread":{}}`)

	if _, err := conv.Fork(context.Background(), nil); err == nil {
		t.Fatal("Fork accepted a response carrying no thread id")
	}
}

func TestSetTitleSendsTheTrimmedName(t *testing.T) {
	conv, srv := openConversation(t)
	srv.reply("thread/name/set", `{}`)

	if err := conv.SetTitle(context.Background(), "  Fix OAuth Return URL Loss  "); err != nil {
		t.Fatalf("SetTitle: %v", err)
	}

	set := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/name/set" })
	var params struct {
		ThreadID string `json:"threadId"`
		Name     string `json:"name"`
	}
	if err := json.Unmarshal(set.Params, &params); err != nil {
		t.Fatalf("thread/name/set params: %v", err)
	}
	if params.ThreadID != "thread-1" {
		t.Errorf("threadId = %q, want thread-1", params.ThreadID)
	}
	if params.Name != "Fix OAuth Return URL Loss" {
		t.Errorf("name = %q, want the trimmed title", params.Name)
	}
}

// The provider rejects a blank name. Refusing locally keeps the error about the
// caller's input and spends no round trip on a call that cannot succeed.
func TestSetTitleRefusesABlankName(t *testing.T) {
	conv, srv := openConversation(t)

	err := conv.SetTitle(context.Background(), "   \t ")
	if err == nil {
		t.Fatal("SetTitle accepted a blank name")
	}
	if !isRefusal(err) {
		t.Errorf("err = %v, want a refusal", err)
	}
	if srv.sentMethod("thread/name/set") {
		t.Fatal("thread/name/set was sent for a blank name")
	}
}

// A title AO did not set still has to reach it: another client naming the thread is
// how a provider-derived title arrives at all.
func TestThreadRenamedNotificationBecomesATitleEvent(t *testing.T) {
	conv, srv := openConversation(t)
	srv.push(`{"method":"thread/name/updated","params":{"threadId":"thread-1","threadName":"  Restore Canvas Renderer Fallback  "}}`)

	ev := nextEvent(t, conv.Events(), ports.ChatEventThreadRenamed)
	if ev.Title != "Restore Canvas Renderer Fallback" {
		t.Errorf("title = %q, want the trimmed name", ev.Title)
	}
}

// A cleared name is reported as an empty title rather than dropped: the projection
// has to see that the thread no longer has one.
func TestThreadRenamedNotificationReportsAClearedName(t *testing.T) {
	conv, srv := openConversation(t)
	srv.push(`{"method":"thread/name/updated","params":{"threadId":"thread-1","threadName":null}}`)

	ev := nextEvent(t, conv.Events(), ports.ChatEventThreadRenamed)
	if ev.Title != "" {
		t.Errorf("title = %q, want empty for a cleared name", ev.Title)
	}
}

// The service feature-detects each of these, so a dropped method would read as "the
// provider cannot do this" with nothing to notice.
func TestConversationAdvertisesTheHistoryCapabilities(t *testing.T) {
	caps := capabilities()
	for _, want := range []ports.ChatCapability{
		ports.ChatCapabilityRollback,
		ports.ChatCapabilityFork,
		ports.ChatCapabilityRename,
	} {
		if !caps.Has(want) {
			t.Errorf("capability %q not advertised", want)
		}
	}
}

// isRefusal asks the error the same question the Chat service does.
func isRefusal(err error) bool {
	var refusal interface{ ChatRefusal() bool }
	return errors.As(err, &refusal) && refusal.ChatRefusal()
}

// This is the deserialization refusal measured in R8, before a handler can mutate
// history. Ordinary -32600 business refusals must never select a second method.
const unknownRollbackMethod = "Invalid request: unknown variant `thread/rollback`, expected one of `initialize`, `thread/read`, `thread/revert`"

func historyRequests(srv *scriptedServer) []frame {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	var requests []frame
	for _, f := range srv.seen {
		switch f.Method {
		case "thread/read", "thread/rollback", "thread/revert", "turn/start":
			requests = append(requests, f)
		}
	}
	return requests
}

func TestRollbackCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name        string
		anchor      string
		modern      bool
		scoped      bool
		wantDiscard int
	}{
		{name: "legacy first", anchor: "turn-a", wantDiscard: 3},
		{name: "legacy middle", anchor: "turn-b", wantDiscard: 2},
		{name: "legacy last", anchor: "turn-c", wantDiscard: 1},
		{name: "modern first", anchor: "turn-a", modern: true, wantDiscard: 3},
		{name: "modern middle", anchor: "turn-b", modern: true, wantDiscard: 2},
		{name: "modern last", anchor: "turn-c", modern: true, wantDiscard: 1},
		{name: "modern scoped anchor", anchor: "turn-b", modern: true, scoped: true, wantDiscard: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conv, srv := openConversation(t)
			srv.reply("thread/read", threadWithTurns)
			srv.reply("thread/rollback", `{"thread":{"id":"thread-1","turns":[]}}`)
			if tc.modern {
				srv.replyError("thread/rollback", -32600, unknownRollbackMethod)
				// Revert's response deliberately carries no hydrated turns.
				srv.reply("thread/revert", `{"thread":{"id":"thread-1","turns":[]},"turnsBackwardsCursor":null}`)
			}
			if tc.scoped {
				conv.providerScopeID = "test-scope"
			}
			if err := conv.Rollback(context.Background(), conv.scopedID(tc.anchor)); err != nil {
				t.Fatal(err)
			}
			requests := historyRequests(srv)
			wantMethods := []string{"thread/read", "thread/rollback"}
			if tc.modern {
				wantMethods = append(wantMethods, "thread/revert")
			}
			var methods []string
			for _, f := range requests {
				methods = append(methods, f.Method)
			}
			if !reflect.DeepEqual(methods, wantMethods) {
				t.Fatalf("requests = %v, want %v", methods, wantMethods)
			}
			var legacy map[string]any
			if err := json.Unmarshal(requests[1].Params, &legacy); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(legacy, map[string]any{"threadId": "thread-1", "numTurns": float64(tc.wantDiscard)}) {
				t.Fatalf("legacy params = %v", legacy)
			}
			if tc.modern {
				var modern map[string]any
				if err := json.Unmarshal(requests[2].Params, &modern); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(modern, map[string]any{"threadId": "thread-1", "beforeTurnId": tc.anchor}) {
					t.Fatalf("revert params = %v", modern)
				}
			}
			if conv.threadID != "thread-1" || srv.sentMethod("thread/resume") {
				t.Fatal("rollback changed or resumed provider identity")
			}
		})
	}
}

func TestRollbackFallbackRequiresExactUnsupportedMethod(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int
		message string
	}{
		{"inflight", -32600, "Cannot rollback while a turn is in progress."},
		{"missing thread", -32600, "thread not found: thread-1"},
		{"unknown turn", -32600, "turn not found: turn-b"},
		{"unsupported prose", -32600, "thread/rollback is unsupported"},
		{"embedded unknown variant", -32600, "Cannot rollback: " + unknownRollbackMethod},
		{"different method", -32600, strings.ReplaceAll(unknownRollbackMethod, "thread/rollback", "thread/read")},
		{"missing revert advertisement", -32600, strings.ReplaceAll(unknownRollbackMethod, "`thread/revert`", "`turn/start`")},
		{"wrong code", -32603, unknownRollbackMethod},
		{"generic method not found", -32601, "Method not found"},
		{"unknown variant wrong code", -32601, unknownRollbackMethod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conv, srv := openConversation(t)
			srv.reply("thread/read", threadWithTurns)
			srv.replyError("thread/rollback", tc.code, tc.message)
			if err := conv.Rollback(context.Background(), "turn-b"); err == nil {
				t.Fatal("provider refusal was lost")
			}
			requests := historyRequests(srv)
			if len(requests) != 2 || requests[1].Method != "thread/rollback" {
				t.Fatalf("unexpected fallback/retry: %+v", requests)
			}
		})
	}
}

func TestRollbackRejectsUnsafeHistoryBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name, history, anchor, active string
		pending                       bool
	}{
		{name: "unknown anchor", history: threadWithTurns, anchor: "missing"},
		{name: "blank anchor", history: threadWithTurns, anchor: " "},
		{name: "active local turn", history: threadWithTurns, anchor: "turn-a", active: "turn-live"},
		{name: "pending approval", history: threadWithTurns, anchor: "turn-a", pending: true},
		{name: "foreign thread", history: strings.ReplaceAll(threadWithTurns, "thread-1", "foreign"), anchor: "turn-a"},
		{name: "missing thread identity", history: `{"thread":{"turns":[{"id":"turn-a","status":"completed"}]}}`, anchor: "turn-a"},
		{name: "duplicate turn", history: strings.ReplaceAll(threadWithTurns, "turn-c", "turn-a"), anchor: "turn-a"},
		{name: "blank turn", history: strings.ReplaceAll(threadWithTurns, "turn-c", ""), anchor: "turn-a"},
		{name: "running later turn", history: strings.ReplaceAll(threadWithTurns, "completed", "inProgress"), anchor: "turn-a"},
		{name: "pending native turn", history: strings.ReplaceAll(threadWithTurns, "completed", "pending"), anchor: "turn-a"},
		{name: "queued native turn", history: strings.ReplaceAll(threadWithTurns, "completed", "queued"), anchor: "turn-a"},
		{name: "unknown native status", history: strings.ReplaceAll(threadWithTurns, "completed", "newStatus"), anchor: "turn-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conv, srv := openConversation(t)
			srv.reply("thread/read", tc.history)
			// Let the old implementation complete an unsafe send so the witness
			// fails on its assertions rather than waiting for an unscripted reply.
			srv.reply("thread/rollback", `{}`)
			srv.reply("thread/revert", `{}`)
			conv.mu.Lock()
			conv.activeTurn = tc.active
			if tc.pending {
				conv.pending["approval"] = &parkedRequest{ch: make(chan ports.ChatDecision, 1)}
			}
			conv.mu.Unlock()
			if err := conv.Rollback(context.Background(), tc.anchor); err == nil {
				t.Fatal("unsafe history accepted")
			}
			if srv.sentMethod("thread/rollback") || srv.sentMethod("thread/revert") {
				t.Fatal("unsafe history caused mutation")
			}
		})
	}
}

func TestRollbackRevertRefusalDoesNotRetry(t *testing.T) {
	for _, message := range []string{"Cannot revert while a turn is in progress.", "turn not found: turn-b", "thread not found: thread-1"} {
		t.Run(message, func(t *testing.T) {
			conv, srv := openConversation(t)
			srv.reply("thread/read", threadWithTurns)
			srv.replyError("thread/rollback", -32600, unknownRollbackMethod)
			srv.replyError("thread/revert", -32600, message)
			err := conv.Rollback(context.Background(), "turn-b")
			if err == nil || !isRefusal(err) || !strings.Contains(err.Error(), message) {
				t.Fatalf("refusal lost: %v", err)
			}
			if got := historyRequests(srv); len(got) != 3 || got[2].Method != "thread/revert" {
				t.Fatalf("unexpected retry: %+v", got)
			}
		})
	}
}

func TestRollbackUncertainMutationDoesNotRetry(t *testing.T) {
	for _, modern := range []bool{false, true} {
		for _, failure := range []string{"cancel", "deadline", "transport"} {
			t.Run(fmt.Sprintf("modern=%v/%s", modern, failure), func(t *testing.T) {
				conv, srv := openConversation(t)
				srv.reply("thread/read", threadWithTurns)
				method := "thread/rollback"
				if modern {
					srv.replyError(method, -32600, unknownRollbackMethod)
					method = "thread/revert"
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- conv.Rollback(ctx, "turn-b") }()
				request := srv.awaitFrame(func(f frame) bool { return f.Method == method })
				want := error(context.DeadlineExceeded)
				if failure == "cancel" {
					cancel()
					want = context.Canceled
				} else if failure == "transport" {
					_ = srv.toClient.Close()
					want = ErrConnClosed
				}
				if err := <-done; !errors.Is(err, want) || isRefusal(err) {
					t.Fatalf("error = %v, want uncertainty %v", err, want)
				}
				if failure != "transport" {
					// A success arriving after the caller stopped waiting cannot retry.
					srv.push(`{"id":` + string(*request.ID) + `,"result":{"thread":{"id":"thread-1","turns":[]}}}`)
				}
				wantCount := 2
				if modern {
					wantCount++
				}
				if got := historyRequests(srv); len(got) != wantCount {
					t.Fatalf("uncertain mutation retried: %+v", got)
				}
			})
		}
	}
}

func TestRollbackSerializesReadAndBothMethodsWithSendTurn(t *testing.T) {
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprint(modern), func(t *testing.T) {
			conv, srv := openConversation(t)
			srv.reply("thread/read", threadWithTurns)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			rollbackDone := make(chan error, 1)
			go func() { rollbackDone <- conv.Rollback(ctx, "turn-b") }()
			request := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/rollback" })
			if conv.sendMu.TryLock() {
				conv.sendMu.Unlock()
				t.Fatal("rollback released send serialization during native request")
			}
			sendDone := make(chan error, 1)
			go func() { _, err := conv.SendTurn(ctx, ports.ChatUserMessage{Text: "later input"}); sendDone <- err }()
			if modern {
				errJSON, _ := json.Marshal(rpcError{Code: -32600, Message: unknownRollbackMethod})
				srv.push(`{"id":` + string(*request.ID) + `,"error":` + string(errJSON) + `}`)
				request = srv.awaitFrame(func(f frame) bool { return f.Method == "thread/revert" })
				if conv.sendMu.TryLock() {
					conv.sendMu.Unlock()
					t.Fatal("fallback released send serialization")
				}
			}
			if srv.sentMethod("turn/start") {
				t.Fatal("input overtook rollback")
			}
			srv.push(`{"id":` + string(*request.ID) + `,"result":{"thread":{"id":"thread-1","turns":[]}}}`)
			if err := <-rollbackDone; err != nil {
				t.Fatal(err)
			}
			if err := <-sendDone; err != nil {
				t.Fatal(err)
			}
			requests := historyRequests(srv)
			if requests[len(requests)-1].Method != "turn/start" {
				t.Fatalf("input order lost: %+v", requests)
			}
		})
	}
}

func TestRollbackRetryRechecksAnchorAfterSuccess(t *testing.T) {
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprint(modern), func(t *testing.T) {
			conv, srv := openConversation(t)
			srv.respondSequence("thread/read", threadWithTurns, `{"thread":{"id":"thread-1","turns":[{"id":"turn-a","status":"completed"}]}}`)
			srv.reply("thread/rollback", `{}`)
			if modern {
				srv.replyError("thread/rollback", -32600, unknownRollbackMethod)
				srv.reply("thread/revert", `{}`)
			}
			if err := conv.Rollback(context.Background(), "turn-b"); err != nil {
				t.Fatal(err)
			}
			if err := conv.Rollback(context.Background(), "turn-b"); err == nil || !isRefusal(err) {
				t.Fatalf("discarded anchor accepted again: %v", err)
			}
			wantCount := 3
			if modern {
				wantCount++
			}
			if got := historyRequests(srv); len(got) != wantCount {
				t.Fatalf("successful mutation repeated: %+v", got)
			}
		})
	}
}

func TestRollbackRechecksOperationsAfterProviderIO(t *testing.T) {
	for _, method := range []string{"thread/read", "thread/rollback"} {
		for _, pending := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pending=%v", method, pending), func(t *testing.T) {
				conv, srv := openConversation(t)
				if method != "thread/read" {
					srv.reply("thread/read", threadWithTurns)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- conv.Rollback(ctx, "turn-b") }()
				request := srv.awaitFrame(func(f frame) bool { return f.Method == method })
				if conv.sendMu.TryLock() {
					conv.sendMu.Unlock()
					t.Fatalf("serialization released during %s", method)
				}
				conv.mu.Lock()
				if pending {
					conv.pending["new-request"] = &parkedRequest{ch: make(chan ports.ChatDecision, 1)}
				} else {
					conv.activeTurn = "new-turn"
				}
				conv.mu.Unlock()
				if method == "thread/read" {
					srv.push(`{"id":` + string(*request.ID) + `,"result":` + threadWithTurns + `}`)
				} else {
					errJSON, _ := json.Marshal(rpcError{Code: -32600, Message: unknownRollbackMethod})
					srv.push(`{"id":` + string(*request.ID) + `,"error":` + string(errJSON) + `}`)
				}
				if err := <-done; err == nil || !isRefusal(err) {
					t.Fatalf("fresh operation did not fence mutation: %v", err)
				}
				if srv.sentMethod("thread/revert") || (method == "thread/read" && srv.sentMethod("thread/rollback")) {
					t.Fatal("mutation sent after fresh operation appeared")
				}
			})
		}
	}
}
