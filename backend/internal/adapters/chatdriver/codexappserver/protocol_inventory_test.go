package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/codexappserver/codexproto"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func inventoryFixture(t *testing.T, version, omit string) string {
	t.Helper()
	methods, err := codexproto.MethodInventory(version)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, direction := range []string{"ClientRequest", "ClientNotification", "ServerRequest", "ServerNotification"} {
		var arms []any
		for name, declared := range methods {
			if declared == direction && name != omit {
				arms = append(arms, map[string]any{"properties": map[string]any{"method": map[string]any{"enum": []string{name}}}})
			}
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

func TestSelectedProtocolAdmission(t *testing.T) {
	for _, version := range []string{"codex-cli 0.159.3", "codex-cli 0.163.0"} {
		t.Run(version, func(t *testing.T) {
			dir := inventoryFixture(t, version, "")
			p, err := readSelectedCodexProtocol(dir, version)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.require(append([]string{"initialize", "initialized", "thread/start"}, resumeProtocolMethods...)...); err != nil {
				t.Fatal(err)
			}
			for _, required := range driverMethods {
				if err := p.require(required.method); err != nil {
					t.Error(err)
				}
			}
			other := "codex-cli 0.159.3"
			if version == other {
				other = "codex-cli 0.163.0"
			}
			for _, selected := range []string{other, "codex-cli 0.164.0"} {
				if p, err := readSelectedCodexProtocol(dir, selected); p != nil || !errors.Is(err, ports.ErrChatDriverIncompatible) {
					t.Fatalf("wrong version %s admitted: p=%v err=%v", selected, p, err)
				}
			}
			// A supported version string never excuses a missing required method.
			dir = inventoryFixture(t, version, "thread/resume")
			if p, err := readSelectedCodexProtocol(dir, version); p != nil || !errors.Is(err, ports.ErrChatDriverIncompatible) {
				t.Fatalf("missing Resume admitted: p=%v err=%v", p, err)
			}
		})
	}
}

func TestSelectedProtocolDoesNotAcceptMalformedResumeReply(t *testing.T) {
	for _, version := range []string{"codex-cli 0.159.3", "codex-cli 0.163.0"} {
		t.Run(version, func(t *testing.T) {
			dir := inventoryFixture(t, version, "")
			d, srv := newTestDriver(t)
			d.protocolProbe = func(context.Context, LaunchContract) (*codexProtocol, error) {
				return readSelectedCodexProtocol(dir, version)
			}
			srv.respondTo("thread/resume", `{"thread":{"id":7}}`)
			conv, err := d.Resume(context.Background(), ports.ChatResumeConfig{ProviderConversationID: "thread-1", WorkspacePath: t.TempDir()})
			if conv != nil || !errors.Is(err, ports.ErrChatResumeFailed) {
				t.Fatalf("malformed reply admitted: %v", err)
			}
			if srv.sentMethod("thread/start") || srv.sentMethod("turn/start") {
				t.Fatal("malformed reply triggered fresh work")
			}
		})
	}
}

func TestSelectedProtocolResumesWithoutReplaySend(t *testing.T) {
	for _, version := range []string{"codex-cli 0.159.3", "codex-cli 0.163.0"} {
		t.Run(version, func(t *testing.T) {
			dir := inventoryFixture(t, version, "")
			d, srv := newTestDriver(t)
			d.protocolProbe = func(context.Context, LaunchContract) (*codexProtocol, error) {
				return readSelectedCodexProtocol(dir, version)
			}
			srv.respondTo("thread/resume", `{"thread":{"id":"thread-1"}}`)
			srv.respondTo("thread/read", `{"thread":{"id":"thread-1","turns":[]}}`)
			conv, err := d.Resume(context.Background(), ports.ChatResumeConfig{ProviderConversationID: "thread-1", WorkspacePath: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conv.Close() }()
			if _, err := conv.(ports.ChatHistoryReader).ReadHistory(context.Background()); err != nil {
				t.Fatal(err)
			}
			if srv.sentMethod("thread/start") || srv.sentMethod("turn/start") {
				t.Fatal("Resume/history replay submitted new work")
			}
			turn, err := conv.SendTurn(context.Background(), ports.ChatUserMessage{Text: "continue", ClientMessageID: "one-send"})
			if err != nil || turn.ProviderTurnID != "turn-1" || conv.ProviderConversationID() != "thread-1" {
				t.Fatalf("continuation=%+v thread=%s err=%v", turn, conv.ProviderConversationID(), err)
			}
			srv.mu.Lock()
			defer srv.mu.Unlock()
			counts := make(map[string]int)
			for _, frame := range srv.seen {
				counts[frame.Method]++
			}
			if counts["thread/start"] != 0 || counts["thread/resume"] != 1 || counts["thread/read"] != 1 || counts["turn/start"] != 1 {
				t.Fatalf("unexpected provider work: %v", counts)
			}
		})
	}
}
