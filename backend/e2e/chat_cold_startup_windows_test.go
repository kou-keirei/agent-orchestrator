//go:build windows

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// Runs a source-built native Windows daemon and native protocol stub, with no
// installed AO, real provider, credentials, live data, or network model calls.
func TestWindowsColdDaemonRestart(t *testing.T) {
	goBin := os.Getenv("AO_COLD_GO")
	if goBin == "" {
		t.Skip("set AO_COLD_GO to the pinned offline Go executable")
	}
	count := 20
	if raw := os.Getenv("AO_COLD_WORKERS"); raw != "" {
		var err error
		count, err = strconv.Atoi(raw)
		if err != nil || count < 20 || count > 200 {
			t.Fatal("AO_COLD_WORKERS must be 20..200")
		}
	}
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	workspaceRoot := filepath.Join(root, "workers")
	binDir := filepath.Join(root, "bin")
	homeDir := filepath.Join(root, "home")
	for _, dir := range []string{dataDir, workspaceRoot, binDir, filepath.Join(homeDir, ".cargo", "bin")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	build := func(out, pkg string) {
		cmd := exec.Command(goBin, "build", "-o", out, pkg)
		cmd.Dir = ".."
		cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOPROXY=off")
		if data, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, data)
		}
	}
	aoBin := filepath.Join(binDir, "ao.exe")
	stubBin := filepath.Join(homeDir, ".cargo", "bin", "codex.exe")
	build(aoBin, "./cmd/ao")
	build(stubBin, "./e2e/testdata/windows_cold_codex")
	calls := filepath.Join(root, "provider-calls")
	// Isolate all discovery roots, including well-known Windows install paths.
	env := make([]string, 0)
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		key = strings.ToUpper(key)
		if strings.HasPrefix(key, "AO_") || key == "HOME" || key == "USERPROFILE" || key == "APPDATA" || key == "LOCALAPPDATA" || key == "PATH" || key == "CODEX_HOME" {
			continue
		}
		env = append(env, item)
	}
	env = append(env, "AO_DATA_DIR="+dataDir, "AO_RUN_FILE="+filepath.Join(dataDir, "running.json"),
		"AO_COLD_CALLS="+calls, "AO_COLD_WORKSPACE_ROOT="+workspaceRoot,
		"HOME="+homeDir, "USERPROFILE="+homeDir, "APPDATA="+homeDir, "LOCALAPPDATA="+homeDir,
		"CODEX_HOME="+filepath.Join(homeDir, ".codex"), "PATH="+filepath.Dir(stubBin)+string(os.PathListSeparator)+os.Getenv("PATH"))
	// A positive control proves the workspace-specific process counter works.
	probe := exec.Command(stubBin, "app-server")
	probe.Env, probe.Dir = env, workspaceRoot
	probe.Dir = filepath.Join(workspaceRoot, "positive-control")
	if err := os.Mkdir(probe.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := probe.Run(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(calls); err != nil || string(data) != "process app-server\n" {
		t.Fatalf("counter positive control=%q err=%v", data, err)
	}
	if err := os.Remove(calls); err != nil {
		t.Fatal(err)
	}

	st, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var ids []domain.SessionID
	for i := range count {
		workspace := filepath.Join(workspaceRoot, strconv.Itoa(i))
		if err := os.Mkdir(workspace, 0700); err != nil {
			t.Fatal(err)
		}
		rec, err := st.CreateSession(ctx, domain.SessionRecord{
			Kind: domain.KindWorker, Mode: domain.SessionModeChat, Harness: domain.HarnessCodex,
			Activity: domain.Activity{State: domain.ActivityIdle}, CreatedAt: time.Now(), UpdatedAt: time.Now(), FirstSignalAt: time.Now(),
			Metadata: domain.SessionMetadata{WorkspacePath: workspace, ProviderConversationID: fmt.Sprintf("native-%d", i), ControllerGeneration: "old", Permissions: domain.PermissionModeBypassPermissions},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.CreateConversation(ctx, fmt.Sprintf("conversation-%d", i), domain.ConversationScopeSession, "", rec.ID, time.Now()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, rec.ID)
	}
	type protectedWork struct {
		id                 domain.SessionID
		conversation, kind string
	}
	var protected []protectedWork
	for _, kind := range []string{"active", "parent", "approval", "user_input", "reserved", "checkpoint"} {
		workspace := filepath.Join(workspaceRoot, kind)
		if err := os.Mkdir(workspace, 0700); err != nil {
			t.Fatal(err)
		}
		state := domain.ActivityIdle
		role := domain.KindWorker
		if kind == "active" || kind == "parent" {
			state = domain.ActivityActive
		}
		if kind == "parent" {
			role = domain.KindOrchestrator
		}
		if kind == "approval" {
			state = domain.ActivityBlocked
		}
		if kind == "user_input" {
			state = domain.ActivityWaitingInput
		}
		rec, err := st.CreateSession(ctx, domain.SessionRecord{
			Kind: role, Mode: domain.SessionModeChat, Harness: domain.HarnessCodex,
			Activity: domain.Activity{State: state}, CreatedAt: time.Now(), UpdatedAt: time.Now(), FirstSignalAt: time.Now(),
			Metadata: domain.SessionMetadata{WorkspacePath: workspace, ProviderConversationID: "native-" + kind, ControllerGeneration: "old", ConversationCheckpointUnsettled: kind == "checkpoint"},
		})
		if err != nil {
			t.Fatal(err)
		}
		conversation, err := st.CreateConversation(ctx, "conversation-"+kind, domain.ConversationScopeSession, "", rec.ID, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if kind == "active" || kind == "parent" || kind == "reserved" {
			if _, err := st.AppendUserMessage(ctx, conversation.ID, rec.ID, "old", domain.ConversationMessage{ID: "message-" + kind, Text: "preserve me", Origin: domain.MessageOriginHuman}, "turn-"+kind, time.Now()); err != nil {
				t.Fatal(err)
			}
			if kind == "reserved" {
				if _, err := st.ReserveQueuedTurnForPromotion(ctx, conversation.ID, "turn-"+kind, time.Now()); err != nil {
					t.Fatal(err)
				}
			} else if err := st.BindTurnToProvider(ctx, "turn-"+kind, "native-turn-"+kind, time.Now()); err != nil {
				t.Fatal(err)
			}
		} else if kind != "checkpoint" {
			if err := st.UpsertActivity(ctx, conversation.ID, "", domain.ConversationActivity{ID: "request-" + kind, Kind: domain.ActivityKind(kind), Status: domain.ActivityStatusPending, RequestID: "request-" + kind}, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		protected = append(protected, protectedWork{rec.ID, conversation.ID, kind})
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	verifyProtected := func() {
		store, err := sqlite.Open(dataDir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		for _, item := range protected {
			rec, found, err := store.GetSession(ctx, item.id)
			if err != nil || !found || rec.IsTerminated || rec.Metadata.ProviderConversationID != "native-"+item.kind || rec.Metadata.ControllerGeneration != "old" {
				t.Fatalf("protected identity changed: %+v err=%v", rec, err)
			}
			if item.kind == "reserved" {
				if _, err := store.NextQueuedTurn(ctx, item.conversation); !errors.Is(err, domain.ErrNoQueuedTurn) {
					t.Fatalf("promotion reservation lost: %v", err)
				}
				turn, err := store.TurnByID(ctx, "turn-reserved")
				if err != nil || turn.State != domain.TurnStateQueued {
					t.Fatalf("reserved turn settled: %+v err=%v", turn, err)
				}
			} else if item.kind == "active" || item.kind == "parent" {
				turn, err := store.TurnByID(ctx, "turn-"+item.kind)
				if err != nil || turn.State != domain.TurnStateRunning || rec.Activity.State != domain.ActivityActive {
					t.Fatalf("running work changed: %+v err=%v", turn, err)
				}
			} else if item.kind == "checkpoint" {
				if !rec.Metadata.ConversationCheckpointUnsettled {
					t.Fatal("checkpoint cleared")
				}
			} else if pending, err := store.HasPendingConversationInteractions(ctx, item.conversation); err != nil || !pending {
				t.Fatalf("pending interaction lost: kind=%s err=%v", item.kind, err)
			}
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	call := func(base, method, route string, body any) (int, []byte, error) {
		var reader io.Reader
		if body != nil {
			data, err := json.Marshal(body)
			if err != nil {
				return 0, nil, err
			}
			reader = bytes.NewReader(data)
		}
		req, err := http.NewRequest(method, base+route, reader)
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		return res.StatusCode, data, err
	}
	start := func() (string, func()) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		base := fmt.Sprintf("http://127.0.0.1:%d", port)
		logPath := filepath.Join(root, fmt.Sprintf("daemon-%d.log", port))
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(aoBin, "daemon")
		cmd.Env = append(append([]string{}, env...), "AO_PORT="+strconv.Itoa(port))
		cmd.Dir, cmd.Stdout, cmd.Stderr = dataDir, logFile, logFile
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		stop := func() {
			once.Do(func() {
				_, _, _ = call(base, "POST", "/shutdown", map[string]any{})
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				select {
				case <-done:
				case <-time.After(15 * time.Second):
					_ = cmd.Process.Kill()
					<-done
				}
				_ = logFile.Close()
			})
		}
		t.Cleanup(stop)
		deadline := time.Now().Add(45 * time.Second)
		for {
			code, _, err := call(base, "GET", "/api/v1/settings", nil)
			if err == nil && code == 200 {
				break
			}
			if time.Now().After(deadline) {
				data, _ := os.ReadFile(logPath)
				t.Fatalf("daemon not ready: %v\n%s", err, data)
			}
			time.Sleep(25 * time.Millisecond)
		}
		return base, stop
	}
	await := func(base string, id domain.SessionID, want string) {
		deadline := time.Now().Add(40 * time.Second)
		for {
			code, data, err := call(base, "GET", "/api/v1/sessions/"+string(id), nil)
			var result struct {
				Session struct {
					StatusReadiness string `json:"statusReadiness"`
					Activity        struct {
						State string `json:"state"`
					} `json:"activity"`
				} `json:"session"`
			}
			_ = json.Unmarshal(data, &result)
			if err == nil && code == 200 && result.Session.StatusReadiness == "ready" {
				code, data, err = call(base, "GET", "/api/v1/sessions/"+string(id)+"/conversation", nil)
				var snapshot struct {
					Controller string `json:"controller"`
				}
				_ = json.Unmarshal(data, &snapshot)
				if err == nil && code == 200 && snapshot.Controller == want {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("session %s never reached %s: status=%d body=%s err=%v", id, want, code, data, err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	awaitProtected := func(base string) {
		for _, item := range protected {
			deadline := time.Now().Add(40 * time.Second)
			for {
				code, data, err := call(base, "GET", "/api/v1/sessions/"+string(item.id), nil)
				var result struct {
					Session struct {
						StatusReadiness string `json:"statusReadiness"`
					} `json:"session"`
				}
				_ = json.Unmarshal(data, &result)
				if err == nil && code == 200 && result.Session.StatusReadiness == "unavailable" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("protected %s not fenced: %s err=%v", item.kind, data, err)
				}
				time.Sleep(25 * time.Millisecond)
			}
		}
	}
	base, stop := start()
	for _, id := range ids {
		await(base, id, "cold")
	}
	awaitProtected(base)
	if data, err := os.ReadFile(calls); !os.IsNotExist(err) {
		t.Fatalf("restart 1 launched workers: %s err=%v", data, err)
	}
	stop()
	verifyProtected()
	base, stop = start()
	for _, id := range ids {
		await(base, id, "cold")
	}
	awaitProtected(base)
	if data, err := os.ReadFile(calls); !os.IsNotExist(err) {
		t.Fatalf("restart 2 launched workers: %s err=%v", data, err)
	}
	t.Logf("Windows persisted idle workers=%d restart1 providers=0 restart2 providers=0", count)
	// Concurrent retries cross real HTTP routing, daemon wiring, native host,
	// thread/resume and durable controller idempotency.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, data, err := call(base, "POST", "/api/v1/sessions/"+string(ids[0])+"/conversation/messages", map[string]any{"text": "continue", "clientMessageId": "first-input"})
			if err != nil || code >= 300 {
				errs <- fmt.Errorf("send status=%d body=%s err=%v", code, data, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(calls)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(data), "process app-server\n") != 1 || strings.Count(string(data), "resume native-0\n") != 1 || strings.Count(string(data), "send native-0 first-input\n") != 1 {
			t.Fatalf("wake duplicated/replaced provider: %s", data)
		}
		code, body, err := call(base, "GET", "/api/v1/sessions/"+string(ids[0])+"/conversation", nil)
		if err == nil && code == 200 && bytes.Contains(body, []byte(`"state":"completed"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stub turn not settled: %s err=%v", body, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if code, data, err := call(base, "POST", "/api/v1/sessions/"+string(ids[0])+"/exit-agent", map[string]any{}); err != nil || code != 200 {
		t.Fatalf("native parking status=%d body=%s err=%v", code, data, err)
	}
	await(base, ids[0], "stopped")
	stop()
	verifyProtected()
	base, stop = start()
	defer stop()
	await(base, ids[0], "stopped")
	for _, id := range ids[1:] {
		await(base, id, "cold")
	}
	awaitProtected(base)
	verifyProtected()
	data, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(data), "process app-server\n") != 1 {
		t.Fatalf("parked worker resurrected: %s err=%v", data, err)
	}
	t.Log("first-input providers=1 native resumes=1 duplicate sends=0; post-exit restart additional providers=0")
}
