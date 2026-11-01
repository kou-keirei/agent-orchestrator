package codexappserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
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
	requests := &nativeResumeRequests{}
	t.Cleanup(func() {
		if err := requests.failure(); err != nil {
			t.Errorf("outgoing native request rejected: %v", err)
		}
	})
	d := New(fixedCodexPlugin(bin), nil)
	// Exercise Driver.Resume's fresh-process branch, not persistent-host adoption.
	d.persistent = false
	d.spawn = func(_ context.Context, executable, workdir string, _ []string) (*process, error) {
		cmd := exec.Command(executable, "app-server")
		cmd.Dir, cmd.Env = workdir, env
		cmd.WaitDelay = time.Second
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
		var stopErr error
		stop := func() error {
			once.Do(func() {
				_ = stdin.Close()
				done := make(chan struct{})
				go func() { _ = cmd.Wait(); close(done) }()
				// Both waits are bounded, including when Kill fails. The
				// callback addresses only this exact disposable child.
				stopErr = nativeResumeWaitExit(done, cmd.Process.Kill, 3*time.Second, time.Second)
				exited[index] = stopErr == nil
			})
			return stopErr
		}
		t.Cleanup(func() {
			if err := stop(); err != nil {
				// Add cleanup evidence without replacing the original failure
				// or refusal with a fatal cleanup result.
				t.Errorf("owned child %d exit not observed: %v", cmd.Process.Pid, err)
			}
		})
		guard := &nativeResumeRequestWriter{WriteCloser: stdin, requests: requests, child: index}
		return &process{stdin: guard, stdout: stdout, stop: stop}, nil
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
	const prompt = "Reply only: acknowledged"
	turn, err := predecessor.SendTurn(ctx, ports.ChatUserMessage{
		Text: prompt, ClientMessageID: "w402-only-turn", Origin: domain.MessageOriginHuman,
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
	if err := nativeResumeRequireExchange(before, turn.ProviderTurnID, prompt); err != nil {
		t.Fatalf("predecessor persisted exchange: %v", err)
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
	if err := nativeResumeRequireExchange(after, turn.ProviderTurnID, prompt); err != nil {
		t.Fatalf("resumed persisted exchange: %v", err)
	}
	if err := requests.complete(); err != nil {
		t.Fatal(err)
	}
	t.Log("NATIVE_E2E_PASS: own predecessor exited; fresh transport retained session/thread and exactly one persisted turn")
}

// The transport checks every complete JSON-RPC write before the child sees it.
// Child 0 is account-only, 1 is the predecessor, and 2 may only resume. Retain
// rejected attempts so a swallowed driver error cannot turn into a passing skip.
type nativeResumeRequests struct {
	mu                     sync.Mutex
	starts, turns, resumes int
	err                    error
}

func (r *nativeResumeRequests) observe(child int, method string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var allowed bool
	switch method {
	case "thread/start":
		allowed = child == 1 && r.starts == 0 && r.turns == 0 && r.resumes == 0
		if allowed {
			r.starts++
		}
	case "turn/start":
		allowed = child == 1 && r.starts == 1 && r.turns == 0 && r.resumes == 0
		if allowed {
			r.turns++
		}
	case "thread/resume":
		allowed = child == 2 && r.starts == 1 && r.turns == 1 && r.resumes == 0
		if allowed {
			r.resumes++
		}
	default:
		return nil
	}
	if !allowed {
		r.err = errors.Join(r.err, fmt.Errorf("unexpected %s on disposable child %d", method, child))
		return r.err
	}
	return nil
}

func (r *nativeResumeRequests) failure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *nativeResumeRequests) complete() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if r.starts != 1 || r.turns != 1 || r.resumes != 1 {
		return fmt.Errorf("outgoing requests: starts=%d turns=%d resumes=%d, want 1 each", r.starts, r.turns, r.resumes)
	}
	return nil
}

type nativeResumeRequestWriter struct {
	io.WriteCloser
	requests *nativeResumeRequests
	child    int
}

func (w *nativeResumeRequestWriter) Write(p []byte) (int, error) {
	var request struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(p, &request); err != nil {
		return 0, err
	}
	if err := w.requests.observe(w.child, request.Method); err != nil {
		return 0, err
	}
	return w.WriteCloser.Write(p)
}

func nativeResumeRequireExchange(events []ports.ChatEvent, turnID, prompt string) error {
	if turnID == "" {
		return errors.New("missing acknowledged native turn ID")
	}
	var starts, completed, users, replies int
	for _, event := range events {
		switch event.Kind {
		case ports.ChatEventTurnStarted, ports.ChatEventTurnCompleted,
			ports.ChatEventUserMessageCompleted, ports.ChatEventMessageCompleted:
			if event.ProviderTurnID != turnID {
				return errors.New("history contains a foreign turn")
			}
		default:
			continue
		}
		switch event.Kind {
		case ports.ChatEventTurnStarted:
			starts++
		case ports.ChatEventTurnCompleted:
			if event.TurnState != domain.TurnStateCompleted || event.Err != nil {
				return errors.New("persisted turn is not successfully completed")
			}
			completed++
		case ports.ChatEventUserMessageCompleted:
			if event.Text != prompt {
				return errors.New("persisted user prompt changed")
			}
			users++
		case ports.ChatEventMessageCompleted:
			if strings.TrimSpace(event.Text) == "" {
				return errors.New("persisted assistant reply is empty")
			}
			replies++
		}
	}
	if starts != 1 || completed != 1 || users != 1 || replies != 1 {
		return fmt.Errorf("persisted exchange: starts=%d completed=%d users=%d replies=%d, want 1 each", starts, completed, users, replies)
	}
	return nil
}

func nativeResumeWaitExit(done <-chan struct{}, kill func() error, grace, afterKill time.Duration) error {
	select {
	case <-done:
		return nil
	case <-time.After(grace):
	}
	killErr := kill()
	select {
	case <-done:
		return nil
	case <-time.After(afterKill):
		return errors.Join(fmt.Errorf("owned child exit was not observed: %w", context.DeadlineExceeded), killErr)
	}
}

type nativeResumeWriteBuffer struct{ bytes.Buffer }

func (*nativeResumeWriteBuffer) Close() error { return nil }

func TestNativeExitResumeRequestGuard(t *testing.T) {
	for _, tc := range []struct {
		name   string
		child  int
		method string
	}{
		{"extra thread before resume", 2, "thread/start"},
		{"extra turn before resume", 2, "turn/start"},
		{"duplicate predecessor thread", 1, "thread/start"},
		{"duplicate predecessor turn", 1, "turn/start"},
		{"probe thread", 0, "thread/start"},
		{"probe turn", 0, "turn/start"},
		{"premature resume", 1, "thread/resume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &nativeResumeRequests{}
			sink := &nativeResumeWriteBuffer{}
			write := func(child int, method string) error {
				w := &nativeResumeRequestWriter{WriteCloser: sink, requests: r, child: child}
				_, err := w.Write([]byte(fmt.Sprintf("{\"id\":1,\"method\":%q}\n", method)))
				return err
			}
			for _, method := range []string{"thread/start", "turn/start"} {
				if err := write(1, method); err != nil {
					t.Fatal(err)
				}
			}
			before := sink.Len()
			if err := write(tc.child, tc.method); err == nil {
				t.Fatal("unexpected request admitted")
			}
			if sink.Len() != before {
				t.Fatal("rejected request forwarded to provider")
			}
			if err := write(2, "thread/resume"); err != nil {
				t.Fatal(err)
			}
			if r.complete() == nil {
				t.Fatal("correct resume hid earlier rejected request")
			}
		})
	}
	t.Run("exact exchange and duplicate resume", func(t *testing.T) {
		r := &nativeResumeRequests{}
		for _, req := range []struct {
			child  int
			method string
		}{{1, "thread/start"}, {1, "turn/start"}, {2, "thread/resume"}} {
			if err := r.observe(req.child, req.method); err != nil {
				t.Fatal(err)
			}
		}
		if err := r.complete(); err != nil {
			t.Fatal(err)
		}
		if r.observe(2, "thread/resume") == nil {
			t.Fatal("duplicate resume admitted")
		}
	})
}

func TestNativeExitResumePersistedExchange(t *testing.T) {
	valid := []ports.ChatEvent{
		{Kind: ports.ChatEventTurnStarted, ProviderTurnID: "turn"},
		{Kind: ports.ChatEventUserMessageCompleted, ProviderTurnID: "turn", Text: "prompt"},
		{Kind: ports.ChatEventMessageCompleted, ProviderTurnID: "turn", Text: "acknowledged"},
		{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: "turn", TurnState: domain.TurnStateCompleted},
	}
	for _, name := range []string{"valid", "empty", "no messages", "no user", "no reply", "empty reply", "foreign reply", "changed prompt", "duplicate user", "duplicate reply", "failed completion", "no turn ID"} {
		t.Run(name, func(t *testing.T) {
			events := append([]ports.ChatEvent(nil), valid...)
			turnID := "turn"
			switch name {
			case "empty":
				events = nil
			case "no messages":
				events = []ports.ChatEvent{valid[0], valid[3]}
			case "no user":
				events = append(events[:1], events[2:]...)
			case "no reply":
				events = append(events[:2], events[3:]...)
			case "empty reply":
				events[2].Text = " "
			case "foreign reply":
				events[2].ProviderTurnID = "foreign"
			case "changed prompt":
				events[1].Text = "changed"
			case "duplicate user":
				events = append(events, valid[1])
			case "duplicate reply":
				events = append(events, valid[2])
			case "failed completion":
				events[3].TurnState = domain.TurnStateFailed
			case "no turn ID":
				turnID = ""
			}
			err := nativeResumeRequireExchange(events, turnID, "prompt")
			if (err == nil) != (name == "valid") {
				t.Fatalf("exchange validation = %v", err)
			}
		})
	}
}

func TestNativeExitResumeBoundedCleanup(t *testing.T) {
	for _, name := range []string{"already exited", "exits after kill", "kill fails", "kill succeeds without exit"} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			if name == "already exited" {
				close(done)
			}
			killErr := errors.New("owned kill failed")
			calls := 0
			kill := func() error {
				calls++
				if name == "exits after kill" {
					close(done)
				}
				if name == "kill fails" {
					return killErr
				}
				return nil
			}
			result := make(chan error, 1)
			go func() { result <- nativeResumeWaitExit(done, kill, time.Millisecond, time.Millisecond) }()
			select {
			case err := <-result:
				wantFailure := name == "kill fails" || name == "kill succeeds without exit"
				if (err != nil) != wantFailure {
					t.Fatalf("cleanup error = %v", err)
				}
				if wantFailure && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("missing exit timeout: %v", err)
				}
				if name == "kill fails" && !errors.Is(err, killErr) {
					t.Fatalf("lost kill failure: %v", err)
				}
				if (calls == 0) != (name == "already exited") {
					t.Fatalf("kill calls = %d", calls)
				}
			case <-time.After(time.Second):
				// Release a regressed unbounded waiter after reporting it.
				close(done)
				t.Fatal("cleanup exceeded its bounded wait")
			}
		})
	}
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
