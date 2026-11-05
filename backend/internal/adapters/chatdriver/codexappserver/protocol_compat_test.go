package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/codexappserver/codexproto"
	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/persistenthost"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var unrelatedProtocolMethods = []string{
	"thread/attachmentOwner/list", "thread/prediction/updated", "thread/readState/changed",
}

func protocolFixture(t *testing.T, omit []string, wrongDirection string) string {
	t.Helper()
	dir := t.TempDir()
	for _, direction := range []string{"ClientRequest", "ClientNotification", "ServerRequest", "ServerNotification"} {
		arms := []any{}
		for _, method := range codexproto.Methods {
			skipped := false
			for _, name := range omit {
				skipped = skipped || method.Name == name
			}
			if skipped {
				continue
			}
			actualDirection := method.Direction
			if method.Name == wrongDirection {
				actualDirection = "ServerNotification"
			}
			if actualDirection == direction {
				arms = append(arms, map[string]any{"properties": map[string]any{"method": map[string]any{"enum": []string{method.Name}}}})
			}
		}
		// Keep a valid declaration document when omitting its only known method.
		if len(arms) == 0 {
			arms = append(arms, map[string]any{"properties": map[string]any{"method": map[string]any{"enum": []string{"fixture/" + direction}}}})
		}
		raw, err := json.Marshal(map[string]any{"oneOf": arms})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, direction+".json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func fixtureProtocolProbe(dir string) func(context.Context, LaunchContract) (*codexProtocol, error) {
	return func(context.Context, LaunchContract) (*codexProtocol, error) {
		return readCodexProtocol(dir, "codex-cli fixture")
	}
}

func TestResumeProtocolUsesOnlyRequiredMethods(t *testing.T) {
	for _, name := range []string{"matching", "unrelated absent", "resume absent", "history absent", "send absent", "wrong direction"} {
		t.Run(name, func(t *testing.T) {
			var omit []string
			var wrong string
			switch name {
			case "unrelated absent":
				omit = unrelatedProtocolMethods
			case "resume absent":
				omit = []string{"thread/resume"}
			case "history absent":
				omit = []string{"thread/read"}
			case "send absent":
				omit = []string{"turn/start"}
			case "wrong direction":
				wrong = "thread/resume"
			}
			p, err := readCodexProtocol(protocolFixture(t, omit, wrong), "fixture")
			if err != nil {
				t.Fatal(err)
			}
			err = p.require(resumeProtocolMethods...)
			wantOK := name == "matching" || name == "unrelated absent"
			if (err == nil) != wantOK || p.capabilities().Has(ports.ChatCapabilityResume) != wantOK {
				t.Fatalf("Resume admission=%v caps=%v", err, p.capabilities())
			}
			if !wantOK && !errors.Is(err, ports.ErrChatDriverIncompatible) {
				t.Fatalf("untyped refusal: %v", err)
			}
		})
	}
}

func TestProtocolDeclarationsFailClosed(t *testing.T) {
	for _, name := range []string{"missing", "malformed", "empty", "unknown shape", "duplicate", "oversized"} {
		t.Run(name, func(t *testing.T) {
			dir := protocolFixture(t, nil, "")
			file := filepath.Join(dir, "ServerNotification.json")
			if name == "missing" {
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			} else {
				body := map[string]string{
					"malformed": "{", "empty": `{}`,
					"unknown shape": `{"oneOf":[{"properties":{"method":{"const":"unknown"}}}]}`,
					"duplicate":     `{"oneOf":[{"properties":{"method":{"enum":["initialize"]}}}]}`,
				}[name]
				if name == "oversized" {
					body = strings.Repeat(" ", (1<<20)+1)
				}
				if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readCodexProtocol(dir, "fixture"); !errors.Is(err, ports.ErrChatDriverIncompatible) {
				t.Fatalf("unrecognized schema admitted: %v", err)
			}
		})
	}
}

func TestResumeProtocolRefusalPreventsProviderLaunch(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, missing := range append([]string{"initialize", "initialized"}, resumeProtocolMethods...) {
			t.Run(missing+map[bool]string{false: "/pipe", true: "/host"}[persistent], func(t *testing.T) {
				d, srv := newTestDriver(t)
				d.persistent = persistent
				d.protocolProbe = fixtureProtocolProbe(protocolFixture(t, []string{missing}, ""))
				d.spawn = func(context.Context, string, string, []string) (*process, error) {
					t.Fatal("protocol refusal launched provider")
					return nil, nil
				}
				d.connectHost = func(ctx context.Context, cfg persistenthost.Config) (*persistenthost.Transport, error) {
					if cfg.Prepare == nil {
						t.Fatal("fresh host bypassed preflight")
					}
					_, err := cfg.Prepare(ctx)
					if err == nil {
						t.Fatal("fresh host would launch unsupported provider")
					}
					return nil, err
				}
				cfg := ports.ChatResumeConfig{SessionID: "same-session", ProviderConversationID: "thread-1", WorkspacePath: t.TempDir()}
				before := cfg
				conv, err := d.Resume(context.Background(), cfg)
				if conv != nil || !errors.Is(err, ports.ErrChatResumeFailed) || !errors.Is(err, ports.ErrChatDriverIncompatible) || errors.Is(err, ports.ErrChatDriverUnavailable) || !strings.Contains(err.Error(), missing) {
					t.Fatalf("conv=%v err=%v", conv, err)
				}
				if !reflect.DeepEqual(cfg, before) || srv.sentMethod("initialize") || srv.sentMethod("thread/resume") || srv.sentMethod("thread/start") || srv.sentMethod("turn/start") {
					t.Fatal("refusal changed identity or sent provider work")
				}
			})
		}
	}
}

func TestResumeProtocolPreservesHistoryAndSingleSend(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "pipe", true: "fresh host"}[persistent], func(t *testing.T) {
			d, srv := newTestDriver(t)
			d.persistent = persistent
			cfg := ports.ChatResumeConfig{
				SessionID: "same-session", ProviderConversationID: "thread-1", WorkspacePath: t.TempDir(), DataDir: t.TempDir(),
				RuntimeGeneration: "runtime-2", ControllerGeneration: "controller-3", Model: "same-model", Effort: "high",
				PrepareEnv: func(context.Context) (map[string]string, error) {
					return map[string]string{"CODEX_HOME": "prepared-home"}, nil
				},
			}
			probe := fixtureProtocolProbe(protocolFixture(t, unrelatedProtocolMethods, ""))
			calls := 0
			d.protocolProbe = func(ctx context.Context, contract LaunchContract) (*codexProtocol, error) {
				calls++
				if contract.WorkspacePath != cfg.WorkspacePath || contract.Executable == "" || launchEnvironmentValue(contract.Environment, "CODEX_HOME") != "prepared-home" {
					t.Fatalf("wrong prepared launch contract: %+v", contract)
				}
				return probe(ctx, contract)
			}
			d.connectHost = func(ctx context.Context, host persistenthost.Config) (*persistenthost.Transport, error) {
				if host.SessionID != string(cfg.SessionID) || host.RuntimeGeneration != cfg.RuntimeGeneration || host.ControllerGeneration != cfg.ControllerGeneration || host.Workdir != cfg.WorkspacePath || host.DataDir != cfg.DataDir {
					t.Fatalf("host correlation changed: %+v", host)
				}
				prepared, err := host.Prepare(ctx)
				if err != nil {
					return nil, err
				}
				proc, err := d.spawn(ctx, prepared.Argv[0], host.Workdir, prepared.Env)
				if err != nil {
					return nil, err
				}
				return &persistenthost.Transport{Stdin: proc.stdin, Stdout: proc.stdout}, nil
			}
			srv.respondTo("thread/resume", `{"thread":{"id":"thread-1"},"model":"same-model","reasoningEffort":"high"}`)
			srv.respondTo("thread/read", `{"thread":{"id":"thread-1","turns":[{"id":"predecessor-turn","status":"completed","items":[{"type":"userMessage","id":"user-1","content":[{"type":"text","text":"original prompt"}]},{"type":"agentMessage","id":"reply-1","text":"original reply"}]}]}}`)
			conv, err := d.Resume(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conv.Close() }()
			history, err := conv.(ports.ChatHistoryReader).ReadHistory(context.Background())
			if err != nil || len(history) != 4 || history[1].Text != "original prompt" || history[2].Text != "original reply" || history[2].ProviderTurnID != "predecessor-turn" {
				t.Fatalf("history=%+v err=%v", history, err)
			}
			if srv.sentMethod("turn/start") {
				t.Fatal("Resume or history replay submitted a turn")
			}
			turn, err := conv.SendTurn(context.Background(), ports.ChatUserMessage{Text: "continue", ClientMessageID: "same-client-send"})
			if err != nil || turn.ProviderTurnID != "turn-1" || conv.ProviderConversationID() != cfg.ProviderConversationID || calls != 1 {
				t.Fatalf("turn=%+v calls=%d err=%v", turn, calls, err)
			}
			if !conv.Capabilities().Has(ports.ChatCapabilityResume) {
				t.Fatal("unrelated omissions hid Resume")
			}
			var resume struct {
				ThreadID string            `json:"threadId"`
				CWD      string            `json:"cwd"`
				Model    string            `json:"model"`
				Config   map[string]string `json:"config"`
			}
			if err := json.Unmarshal(srv.awaitFrame(func(f frame) bool { return f.Method == "thread/resume" }).Params, &resume); err != nil {
				t.Fatal(err)
			}
			if resume.ThreadID != cfg.ProviderConversationID || resume.CWD != cfg.WorkspacePath || resume.Model != cfg.Model || resume.Config["model_reasoning_effort"] != cfg.Effort {
				t.Fatalf("resume identity/settings changed: %+v", resume)
			}
			var send struct {
				ThreadID string `json:"threadId"`
				ClientID string `json:"clientUserMessageId"`
			}
			if err := json.Unmarshal(srv.awaitFrame(func(f frame) bool { return f.Method == "turn/start" }).Params, &send); err != nil {
				t.Fatal(err)
			}
			if send.ThreadID != cfg.ProviderConversationID || send.ClientID != "same-client-send" {
				t.Fatalf("send correlation changed: %+v", send)
			}
			srv.mu.Lock()
			defer srv.mu.Unlock()
			counts := map[string]int{}
			for _, f := range srv.seen {
				counts[f.Method]++
			}
			if counts["thread/start"] != 0 || counts["thread/resume"] != 1 || counts["turn/start"] != 1 || counts["thread/read"] != 1 {
				t.Fatalf("unexpected provider requests: %v", counts)
			}
		})
	}
}

func TestResumeProtocolPreparedRefusal(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "pipe", true: "host"}[persistent], func(t *testing.T) {
			d, _ := newTestDriver(t)
			d.persistent = persistent
			d.protocolProbe = func(_ context.Context, contract LaunchContract) (*codexProtocol, error) {
				if launchEnvironmentValue(contract.Environment, "CODEX_HOME") != "prepared-home" {
					t.Fatal("checked ambient account instead of prepared account")
				}
				return nil, ports.ErrChatDriverIncompatible
			}
			d.spawn = func(context.Context, string, string, []string) (*process, error) {
				t.Fatal("prepared refusal launched provider")
				return nil, nil
			}
			d.connectHost = func(ctx context.Context, cfg persistenthost.Config) (*persistenthost.Transport, error) {
				_, err := cfg.Prepare(ctx)
				return nil, err
			}
			conv, err := d.Resume(context.Background(), ports.ChatResumeConfig{
				ProviderConversationID: "thread-1", WorkspacePath: t.TempDir(),
				PrepareEnv: func(context.Context) (map[string]string, error) {
					return map[string]string{"CODEX_HOME": "prepared-home"}, nil
				},
			})
			if conv != nil || !errors.Is(err, ports.ErrChatResumeFailed) || !errors.Is(err, ports.ErrChatDriverIncompatible) {
				t.Fatalf("conv=%v err=%v", conv, err)
			}
		})
	}
}

func TestResumeProtocolReconnectUsesSurvivingHost(t *testing.T) {
	d, srv := newTestDriver(t)
	proc, err := d.spawn(context.Background(), "codex", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	d.persistent = true
	d.protocolProbe = func(context.Context, LaunchContract) (*codexProtocol, error) {
		t.Fatal("surviving provider was checked against a new binary")
		return nil, nil
	}
	d.connectHost = func(_ context.Context, cfg persistenthost.Config) (*persistenthost.Transport, error) {
		if cfg.SessionID != "session-1" || cfg.RuntimeGeneration != "runtime-1" || cfg.ControllerGeneration != "controller-2" {
			t.Fatal("reconnect lost host generation correlation")
		}
		return &persistenthost.Transport{Stdin: proc.stdin, Stdout: proc.stdout, Reconnected: true}, nil
	}
	conv, err := d.Resume(context.Background(), ports.ChatResumeConfig{
		SessionID: "session-1", ProviderConversationID: "thread-1", WorkspacePath: t.TempDir(),
		RuntimeGeneration: "runtime-1", ControllerGeneration: "controller-2",
		PrepareEnv: func(context.Context) (map[string]string, error) {
			t.Fatal("reconnect prepared a new account")
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conv.Close() }()
	if conv.ProviderConversationID() != "thread-1" || srv.sentMethod("initialize") || srv.sentMethod("thread/resume") || srv.sentMethod("thread/start") || srv.sentMethod("turn/start") {
		t.Fatal("reconnect sent fresh session work")
	}
}

func TestResumeProtocolDoesNotMaskPayloadOrACKFailure(t *testing.T) {
	for _, reply := range []string{`{"thread":{"id":7}}`, `{"thread":{"id":"foreign"}}`, `{}`, "lost ACK"} {
		t.Run(reply, func(t *testing.T) {
			d, srv := newTestDriver(t)
			d.protocolProbe = fixtureProtocolProbe(protocolFixture(t, unrelatedProtocolMethods, ""))
			srv.respondTo("thread/resume", reply)
			if reply == "lost ACK" {
				srv.mu.Lock()
				delete(srv.responses, "thread/resume")
				srv.mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			conv, err := d.Resume(ctx, ports.ChatResumeConfig{ProviderConversationID: "thread-1", WorkspacePath: t.TempDir()})
			if conv != nil || !errors.Is(err, ports.ErrChatResumeFailed) {
				t.Fatalf("bad payload or missing ACK admitted: %v", err)
			}
			if srv.sentMethod("thread/start") || srv.sentMethod("turn/start") {
				t.Fatal("Resume failure fell back to new work")
			}
		})
	}
}

func TestProtocolProbeAdvertisesResumeIndependently(t *testing.T) {
	for _, missing := range []string{"thread/prediction/updated", "thread/resume", "thread/read", "turn/start"} {
		t.Run(missing, func(t *testing.T) {
			d, _ := newTestDriver(t)
			d.protocolProbe = fixtureProtocolProbe(protocolFixture(t, []string{missing}, ""))
			caps, err := d.ProbeForWorkspace(context.Background(), t.TempDir())
			if err != nil || !caps.Has(ports.ChatCapabilityModels) || caps.Has(ports.ChatCapabilityResume) != (missing == "thread/prediction/updated") || caps.Has(ports.ChatCapabilityHistory) != (missing != "thread/read") {
				t.Fatalf("caps=%v err=%v", caps, err)
			}
		})
	}
}

func TestInstalledResumeProtocol(t *testing.T) {
	bin, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("codex not installed")
	}
	d := New(fixedCodexPlugin(bin), nil)
	if d.protocolProbe == nil {
		t.Fatal("production driver omitted introspection")
	}
	contract, err := newLaunchContract(context.Background(), bin, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := d.checkProtocol(context.Background(), contract, resumeProtocolMethods...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s declares Resume methods; authenticated payload/history behavior remains unproven", p.version)
}
