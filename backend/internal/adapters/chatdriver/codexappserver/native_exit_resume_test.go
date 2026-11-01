package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/persistenthost"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// TestNativeExitResumeDisposable is deliberately separate from AO_CODEX_LIVE:
// no user profile, credentials, persistent host, or preexisting thread is used.
// The only model turn is on the predecessor. Resume must recover that turn,
// without sending it again. An auth requirement is unavailability, not a pass
// for native resume. No fixture-created history or scripted child substitutes
// for the native predecessor's on-disk history.
func TestNativeExitResumeDisposable(t *testing.T) {
	if os.Getenv("AO_CODEX_NATIVE_EXIT_RESUME") != "1" {
		t.Skip("NATIVE_E2E_UNAVAILABLE: opt in with AO_CODEX_NATIVE_EXIT_RESUME=1")
	}
	bin, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("NATIVE_E2E_UNAVAILABLE: locally installed codex not found")
	}
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	home := filepath.Join(root, "profile")
	for _, dir := range []string{workspace, home} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Pin file-only auth so the OS credential store cannot supply a user login.
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte("cli_auth_credentials_store = \"file\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Supply a complete allowlisted environment to exec, never the driver's
	// ambient overlay. In particular no API keys, auth tokens, or user homes.
	env := []string{"CODEX_HOME=" + home, "HOME=" + home, "USERPROFILE=" + home,
		"APPDATA=" + home, "LOCALAPPDATA=" + home, "TEMP=" + root, "TMP=" + root,
		"HTTP_PROXY=http://127.0.0.1:1", "HTTPS_PROXY=http://127.0.0.1:1", "ALL_PROXY=http://127.0.0.1:1"}
	for _, key := range []string{"SystemRoot", "WINDIR", "PATH"} {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var children []*exec.Cmd
	var exited []bool
	d := New(fixedCodexPlugin(bin), nil)
	// Exercise Driver.Resume's fresh-process branch, not persistent-host adoption.
	d.persistent = false
	d.spawn = func(_ context.Context, executable, workdir string, _ []string) (*process, error) {
		cmd := exec.Command(executable, "app-server")
		cmd.Dir, cmd.Env = workdir, env
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			return nil, err
		}
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			return nil, err
		}
		index := len(children)
		children = append(children, cmd)
		exited = append(exited, false)
		t.Logf("own disposable child %d started", cmd.Process.Pid)
		var once sync.Once
		stop := func() error {
			once.Do(func() {
				_ = stdin.Close()
				done := make(chan struct{})
				go func() { _ = cmd.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					// Only this exact disposable child, never a process tree.
					_ = cmd.Process.Kill()
					<-done
				}
				exited[index] = true
			})
			return nil
		}
		t.Cleanup(func() { _ = stop() })
		return &process{stdin: stdin, stdout: stdout, stop: stop}, nil
	}
	// Local account/read does not refresh credentials or create a thread.
	contract, err := d.resolveLaunchContract(ctx, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := d.connect(ctx, contract, "")
	if err != nil {
		t.Skipf("NATIVE_E2E_UNAVAILABLE: isolated initialize: %v", err)
	}
	var account struct {
		Account            json.RawMessage `json:"account"`
		RequiresOpenaiAuth bool            `json:"requiresOpenaiAuth"`
	}
	err = probe.conn.request(ctx, "account/read", map[string]any{"refreshToken": false}, &account)
	_ = probe.Close()
	if err != nil {
		t.Skipf("NATIVE_E2E_UNAVAILABLE: isolated account/read: %v", err)
	}
	if account.RequiresOpenaiAuth || (len(account.Account) != 0 && string(account.Account) != "null") {
		t.Skip("NATIVE_E2E_UNAVAILABLE: provider requires authentication or discovered an unexpected account; zero model turns")
	}
	predecessor, err := d.Start(ctx, ports.ChatStartConfig{
		SessionID: "w402-disposable", WorkspacePath: workspace, Permissions: ports.PermissionModeReadOnly,
	})
	if err != nil {
		t.Skipf("NATIVE_E2E_UNAVAILABLE: isolated predecessor: %v", err)
	}
	threadID := predecessor.ProviderConversationID()
	turn, err := predecessor.SendTurn(ctx, ports.ChatUserMessage{
		Text: "Reply only: acknowledged", ClientMessageID: "w402-only-turn", Origin: domain.MessageOriginHuman,
	})
	if err != nil {
		t.Skipf("NATIVE_E2E_UNAVAILABLE: sole turn refused (no retry): %v", err)
	}
	for completed := false; !completed; {
		select {
		case event, ok := <-predecessor.Events():
			if !ok {
				t.Fatal("native event stream closed before completion")
			}
			if event.Kind == ports.ChatEventTurnCompleted {
				if event.TurnState != domain.TurnStateCompleted {
					t.Skip("NATIVE_E2E_UNAVAILABLE: sole turn did not complete; no retry")
				}
				completed = true
			}
		case <-ctx.Done():
			t.Fatal("native turn deadline; no retry")
		}
	}
	before, err := predecessor.(ports.ChatHistoryReader).ReadHistory(ctx)
	if err != nil {
		t.Fatalf("predecessor history: %v", err)
	}
	if err := predecessor.Close(); err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 || !exited[1] {
		t.Fatal("predecessor exit was not observed")
	}
	resumed, err := d.Resume(ctx, ports.ChatResumeConfig{
		SessionID: "w402-disposable", ProviderConversationID: threadID, WorkspacePath: workspace,
		Permissions: ports.PermissionModeReadOnly,
	})
	if err != nil {
		if errors.Is(err, ports.ErrChatResumeFailed) || errors.Is(err, ports.ErrChatPermissionModeUnsupported) {
			t.Skipf("NATIVE_E2E_UNAVAILABLE: typed native resume refusal: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = resumed.Close() }()
	if len(children) != 3 || children[1] == children[2] || resumed.ProviderConversationID() != threadID {
		t.Fatal("resume did not use a fresh process and original thread")
	}
	after, err := resumed.(ports.ChatHistoryReader).ReadHistory(ctx)
	if err != nil {
		t.Fatalf("native persisted history unavailable: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("native history changed across provider exit")
	}
	var count int
	for _, event := range after {
		if event.Kind == ports.ChatEventTurnStarted {
			count++
			if event.ProviderTurnID != turn.ProviderTurnID {
				t.Fatal("resumed a foreign turn")
			}
		}
	}
	if count != 1 {
		t.Fatalf("persisted turn count = %d, want exactly one", count)
	}
	t.Log("NATIVE_E2E_PASS: own predecessor exited; fresh transport retained session/thread and exactly one persisted turn")
}

// These are source-level boundary fixtures, not evidence of native provider exit.
func TestNativeExitResumeRefusalBoundaries(t *testing.T) {
	for _, name := range []string{"wrong ID", "lost ACK", "foreign workspace", "stale generation"} {
		t.Run(name, func(t *testing.T) {
			d, srv := newTestDriver(t)
			workspace := t.TempDir()
			want := ports.ErrChatResumeFailed
			if name == "wrong ID" {
				srv.reply("thread/resume", `{"thread":{"id":"foreign"}}`)
			}
			if name == "lost ACK" {
				srv.mu.Lock()
				delete(srv.responses, "thread/resume")
				srv.mu.Unlock()
			}
			if name == "foreign workspace" || name == "stale generation" {
				want = ports.ErrChatRecoveryInconclusive
				d.persistent = true
				d.connectHost = func(_ context.Context, cfg persistenthost.Config) (*persistenthost.Transport, error) {
					if cfg.Workdir != workspace || cfg.RuntimeGeneration != "runtime-current" || cfg.ControllerGeneration != "controller-current" {
						t.Error("driver failed to forward ownership boundary")
					}
					// Persistent host owns workspace/generation validation. This
					// fixture only proves the driver preserves its typed refusal.
					return nil, persistenthost.ErrOwnershipInconclusive
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			conv, err := d.Resume(ctx, ports.ChatResumeConfig{
				SessionID: "w402-disposable", ProviderConversationID: "thread-1", WorkspacePath: workspace,
				DataDir: t.TempDir(), RuntimeGeneration: "runtime-current", ControllerGeneration: "controller-current",
			})
			if conv != nil {
				_ = conv.Close()
				t.Fatal("refused resume returned a conversation")
			}
			if !errors.Is(err, want) {
				t.Fatalf("resume error = %v, want %v", err, want)
			}
			if name == "lost ACK" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost ACK cause = %v", err)
			}
			if srv.sentMethod("thread/start") || srv.sentMethod("turn/start") {
				t.Fatal("refusal retried as a new thread or turn")
			}
			if name == "lost ACK" || name == "wrong ID" {
				if !srv.sentMethod("thread/resume") {
					t.Fatal("fixture did not exercise native resume request")
				}
			}
		})
	}
}
