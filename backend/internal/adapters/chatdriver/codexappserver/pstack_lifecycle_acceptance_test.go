package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// TestPStackNativeLifecycleCrashResume is tier 2 only. Unlike fixture tests, it
// requires a real completed provider exchange and an OS-observed predecessor
// exit before reconnecting. No user profile, AO daemon, or credentials are read.
// Authentication refusal is a skip/HOLD, never native acceptance.
func TestPStackNativeLifecycleCrashResume(t *testing.T) {
	if os.Getenv("AO_CODEX_NATIVE_PSTACK") != "1" {
		t.Skip("NATIVE_E2E_UNAVAILABLE: opt in with AO_CODEX_NATIVE_PSTACK=1")
	}
	bin, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("NATIVE_E2E_UNAVAILABLE: installed codex executable unavailable")
	}
	root := t.TempDir()
	home, workspace := filepath.Join(root, "codex"), filepath.Join(root, "workspace")
	for _, dir := range []string{home, workspace, filepath.Join(root, "ao")} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, "config.toml"), []byte("cli_auth_credentials_store = \"file\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := []string{"CODEX_HOME=" + home, "HOME=" + home, "USERPROFILE=" + home,
		"APPDATA=" + home, "LOCALAPPDATA=" + home, "TEMP=" + root, "TMP=" + root,
		"AO_DATA_DIR=" + filepath.Join(root, "ao"), "AO_RUN_FILE=" + filepath.Join(root, "ao", "running.json"),
		"HTTP_PROXY=http://127.0.0.1:1", "HTTPS_PROXY=http://127.0.0.1:1", "ALL_PROXY=http://127.0.0.1:1"}
	for _, key := range []string{"SystemRoot", "WINDIR", "PATH"} {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}
	// The protocol probe creates schema scratch via os.UserHomeDir in this
	// process, independently of the child launch environment. Confine both.
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	requests := &nativeResumeRequests{}
	t.Cleanup(func() {
		if err := requests.failure(); err != nil {
			t.Errorf("native request budget violated: %v", err)
		}
	})
	var children []*exec.Cmd
	var exits []<-chan struct{}
	d := New(fixedCodexPlugin(bin), nil)
	d.persistent = false
	// Preflight runs before spawn: its version/schema children must receive
	// the same complete allowlist, never the ambient contract environment.
	d.protocolProbe = func(ctx context.Context, contract LaunchContract) (*codexProtocol, error) {
		contract.Environment = env
		return probeCodexProtocol(ctx, contract)
	}
	d.spawn = func(_ context.Context, executable, workdir string, _ []string) (*process, error) {
		cmd := exec.Command(executable, "app-server")
		cmd.Dir, cmd.Env, cmd.Stderr, cmd.WaitDelay = workdir, env, io.Discard, time.Second
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			return nil, err
		}
		index := len(children)
		children = append(children, cmd)
		done := make(chan struct{})
		exits = append(exits, done)
		// Wait exactly once. Reading ProcessState only after done synchronizes
		// with Wait; a successful stop callback alone is insufficient evidence.
		go func() { _ = cmd.Wait(); close(done) }()
		var once sync.Once
		var stopErr error
		stop := func() error {
			once.Do(func() {
				_ = stdin.Close()
				stopErr = nativeResumeWaitExit(done, cmd.Process.Kill, time.Second, time.Second)
			})
			return stopErr
		}
		t.Cleanup(func() {
			if err := stop(); err != nil {
				t.Errorf("owned child %d cleanup: %v", cmd.Process.Pid, err)
			}
		})
		return &process{stdin: &nativeResumeRequestWriter{WriteCloser: stdin, requests: requests, child: index}, stdout: stdout, stop: stop}, nil
	}
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
	if closeErr := probe.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || account.RequiresOpenaiAuth || (len(account.Account) != 0 && string(account.Account) != "null") {
		t.Skip("NATIVE_E2E_UNAVAILABLE: isolated account unavailable or authentication required; zero model turns")
	}
	predecessor, err := d.Start(ctx, ports.ChatStartConfig{SessionID: "pstack-disposable", WorkspacePath: workspace, Permissions: ports.PermissionModeReadOnly})
	if err != nil {
		t.Skipf("NATIVE_E2E_UNAVAILABLE: isolated predecessor: %v", err)
	}
	thread := predecessor.ProviderConversationID()
	const prompt = "Reply only: acknowledged"
	turn, err := predecessor.SendTurn(ctx, ports.ChatUserMessage{ClientMessageID: "pstack-only-turn", Text: prompt, Origin: domain.MessageOriginHuman})
	if err != nil {
		t.Skipf("NATIVE_E2E_UNAVAILABLE: sole model turn refused; no retry: %v", err)
	}
	for completed := false; !completed; {
		select {
		case event, ok := <-predecessor.Events():
			if !ok {
				t.Fatal("provider stream closed before sole turn completed")
			}
			if event.Kind == ports.ChatEventTurnCompleted {
				if event.ProviderTurnID != turn.ProviderTurnID {
					t.Fatal("foreign provider completion")
				}
				if event.TurnState != domain.TurnStateCompleted || event.Err != nil {
					t.Skip("NATIVE_E2E_UNAVAILABLE: sole model turn failed; no retry")
				}
				completed = true
			}
		case <-ctx.Done():
			t.Fatal("sole native turn deadline; no retry")
		}
	}
	before, err := predecessor.(ports.ChatHistoryReader).ReadHistory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := nativeResumeRequireExchange(before, turn.ProviderTurnID, prompt); err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 {
		t.Fatalf("unexpected child count: %d", len(children))
	}
	// Crash only our exact disposable provider after its exchange is persisted.
	if err := children[1].Process.Kill(); err != nil {
		t.Fatalf("crash disposable predecessor: %v", err)
	}
	select {
	case <-exits[1]:
	case <-time.After(3 * time.Second):
		t.Fatal("HOLD: no positive OS predecessor exit observation")
	}
	state := children[1].ProcessState
	if state == nil || state.Success() {
		t.Fatal("crash did not produce a non-success OS process result")
	}
	t.Logf("native predecessor pid=%d exit=%s thread=%s turn=%s", children[1].Process.Pid, state, thread, turn.ProviderTurnID)
	if err := predecessor.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := d.Resume(ctx, ports.ChatResumeConfig{SessionID: "pstack-disposable", ProviderConversationID: thread, WorkspacePath: workspace, Permissions: ports.PermissionModeReadOnly})
	if err != nil {
		t.Fatalf("native resume after observed exit: %v", err)
	}
	defer func() {
		if err := resumed.Close(); err != nil {
			t.Error(err)
		}
	}()
	if len(children) != 3 || children[1].Process.Pid == children[2].Process.Pid || resumed.ProviderConversationID() != thread {
		t.Fatal("resume did not replace process while retaining original provider thread")
	}
	after, err := resumed.(ports.ChatHistoryReader).ReadHistory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A successful native exchange must not contain errors on either side.
	// Check them explicitly before comparing every history field, so error
	// implementation details never stand in for successful replay semantics.
	for i, event := range before {
		require.NoError(t, event.Err, "native history before crash, event %d", i)
	}
	for i, event := range after {
		require.NoError(t, event.Err, "native history after resume, event %d", i)
	}
	require.Equal(t, before, after, "correlated native history changed across crash/resume")
	if err := nativeResumeRequireExchange(after, turn.ProviderTurnID, prompt); err != nil {
		t.Fatal(err)
	}
	if err := requests.complete(); err != nil {
		t.Fatal(err)
	}
	t.Log("NATIVE_E2E_PASS: tier 2 only; one original turn, zero replay turns; report/semantic tiers remain HOLD")
}

// This is a transport fixture, not a native provider run. An unknown turn/start
// result must return to the owner without replaying the prompt or making a thread.
func TestPStackLifecycleUnknownTurnDoesNotReplay(t *testing.T) {
	d, srv := newTestDriver(t)
	srv.mu.Lock()
	delete(srv.responses, "turn/start")
	srv.mu.Unlock()
	conv, err := d.Start(t.Context(), ports.ChatStartConfig{WorkspacePath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conv.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = conv.SendTurn(ctx, ports.ChatUserMessage{ClientMessageID: "unknown-original", Text: "must not replay"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unknown turn result: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	counts := map[string]int{}
	for _, frame := range srv.seen {
		counts[frame.Method]++
	}
	if counts["thread/start"] != 1 || counts["turn/start"] != 1 || counts["thread/resume"] != 0 {
		t.Fatalf("unknown result created duplicate provider work: %v", counts)
	}
}
