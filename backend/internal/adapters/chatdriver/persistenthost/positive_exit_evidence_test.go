package persistenthost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// This is a disposable native child, not a genuine provider acceptance run.
func TestPositiveExitProviderHelper(t *testing.T) {
	if os.Getenv("AO_POSITIVE_EXIT_HELPER") != "1" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			continue
		}
		if request.Method == "exit" {
			os.Exit(7)
		}
		if request.Method == "exit-zero" {
			os.Exit(0)
		}
		if request.Method == "thread/start" || request.Method == "thread/resume" {
			_, _ = fmt.Fprintf(os.Stdout, "{\"id\":%d,\"result\":{\"thread\":{\"id\":\"exit-thread\"}}}\n", request.ID)
		} else {
			_, _ = fmt.Fprintf(os.Stdout, "{\"id\":%d,\"result\":{\"pid\":%d}}\n", request.ID, os.Getpid())
		}
	}
	os.Exit(0)
}

func startPositiveExitHost(t *testing.T, cfg Config) (Descriptor, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Run(ctx, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("native test host did not stop")
		}
	})
	return awaitDescriptor(t, cfg.DataDir, cfg.SessionID), done
}

func positiveExitHostConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		SessionID: "positive-exit", DataDir: t.TempDir(), Workdir: t.TempDir(),
		Env:  append(os.Environ(), "AO_POSITIVE_EXIT_HELPER=1"),
		Argv: []string{os.Args[0], "-test.run=^TestPositiveExitProviderHelper$"},
	}
}

func openExitThread(t *testing.T, transport *Transport) {
	t.Helper()
	sendFrame(t, transport, `{"id":1,"method":"thread/start","params":{}}`)
	_ = readFrame(t, bufio.NewReader(transport.Stdout))
}

func receivePositiveExit(t *testing.T, transport *Transport) ProviderExitEvidence {
	t.Helper()
	select {
	case proof, ok := <-transport.ProviderExit:
		if !ok {
			t.Fatal("exit source closed without positive evidence")
		}
		return proof
	case <-time.After(8 * time.Second):
		t.Fatal("no native wait evidence")
		return ProviderExitEvidence{}
	}
}

func requireUnknownExit(t *testing.T, source <-chan ProviderExitEvidence) {
	t.Helper()
	select {
	case proof, ok := <-source:
		if ok {
			t.Fatalf("unknown observation promoted to exit: %+v", proof)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("unknown source did not close")
	}
}

// Block the first replay write independently of socket buffer sizes, so the
// attach caller must register its observer before it can consume replay.
type blockedExitReplayConn struct {
	net.Conn
	frame   string
	blocked chan<- struct{}
	release <-chan struct{}
}

func (c *blockedExitReplayConn) Write(p []byte) (int, error) {
	if string(p) == c.frame {
		close(c.blocked)
		<-c.release
	}
	return c.Conn.Write(p)
}

func TestPositiveExitObserverSurvivesBackpressuredReplay(t *testing.T) {
	for _, exitDuringReplay := range []bool{false, true} {
		t.Run(fmt.Sprintf("exit-during-replay=%v", exitDuringReplay), func(t *testing.T) {
			testPositiveExitBackpressuredReplay(t, exitDuringReplay)
		})
	}
}

func testPositiveExitBackpressuredReplay(t *testing.T, exitDuringReplay bool) {
	t.Helper()
	child := exec.Command(os.Args[0], "-test.run=^TestPositiveExitProviderHelper$")
	child.Env = append(os.Environ(), "AO_POSITIVE_EXIT_HELPER=1")
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatal(err)
	}
	firstFrame := `{"method":"replayed/first"}` + "\n"
	secondFrame := `{"method":"replayed/second"}` + "\n"
	h := &host{
		ctx: context.Background(), listener: listener, child: child, stdin: stdin,
		token: "secret", ownerID: "owner", sessionID: "replay-exit",
		pendingRequests: make(map[string]*pendingRequest), shutdown: make(chan struct{}),
	}
	h.cond = sync.NewCond(&h.mu)
	// Establish the detached host's native handle through a real correlated
	// child response, as it would have done before a controller reconnect.
	threadRequest := []byte(`{"id":1,"method":"thread/start","params":{}}` + "\n")
	h.observeCodexRequest(threadRequest)
	if _, err := stdin.Write(threadRequest); err != nil {
		t.Fatal(err)
	}
	providerReader := bufio.NewReader(stdout)
	h.observeCodexReadOnly(readFrame(t, providerReader))
	h.bufferFrameLocked([]byte(firstFrame))
	h.bufferFrameLocked([]byte(secondFrame))
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		go h.handle(&blockedExitReplayConn{Conn: conn, frame: firstFrame, blocked: blocked, release: release})
		_ = h.accept()
	}()
	done := make(chan struct{})
	waitObserved := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.forwardProvider(providerReader)
		waitErr := child.Wait()
		close(waitObserved)
		h.finishProviderExit(waitErr)
	}()
	t.Cleanup(func() {
		unblock()
		_ = listener.Close()
		_ = child.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("backpressured host did not finish native wait")
		}
		<-acceptDone
	})
	d := Descriptor{Version: ProtocolVersion, Address: listener.Addr().String(), Token: "secret", SessionID: h.sessionID}
	transport, err := attachWithGenerations(context.Background(), d, true, "runtime-replay", "controller-replay")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Stdin.Close() })
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("replay never reached backpressure")
	}
	select {
	case proof, ok := <-transport.ProviderExit:
		t.Fatalf("observer lost during blocked replay: %+v, open=%v", proof, ok)
	default:
	}
	if exitDuringReplay {
		// A completed OS wait must survive backpressure without closing stdout
		// before replay is consumed or losing its registered observer.
		if _, err := fmt.Fprintln(stdin, `{"method":"exit"}`); err != nil {
			t.Fatal(err)
		}
		select {
		case <-waitObserved:
		case <-time.After(3 * time.Second):
			t.Fatal("child did not exit during blocked replay")
		}
		unblock()
		reader := bufio.NewReader(transport.Stdout)
		for _, want := range []string{firstFrame, secondFrame} {
			if got := string(readFrame(t, reader)); got != want {
				t.Fatalf("exit truncated replay: got %q, want %q", got, want)
			}
		}
	} else {
		// Live provider output must remain behind both replay frames even though
		// the observer handshake can now acquire the host lock.
		if _, err := fmt.Fprintln(stdin, `{"id":2,"method":"ping"}`); err != nil {
			t.Fatal(err)
		}
		unblock()
		reader := bufio.NewReader(transport.Stdout)
		for _, want := range []string{firstFrame, secondFrame} {
			if got := string(readFrame(t, reader)); got != want {
				t.Fatalf("replay order: got %q, want %q", got, want)
			}
		}
		if got := string(readFrame(t, reader)); !strings.Contains(got, `"id":2`) {
			t.Fatalf("live output lost after replay: %s", got)
		}
		sendFrame(t, transport, `{"method":"exit"}`)
	}
	proof := receivePositiveExit(t, transport)
	if proof.SessionID != d.SessionID || proof.NativeThreadID != "exit-thread" || proof.OwnerID != transport.OwnerID ||
		proof.OwnerID != h.ownerID || proof.AttachmentGeneration != transport.AttachmentGeneration || proof.AttachmentGeneration != 1 ||
		proof.RuntimeGeneration != "runtime-replay" || proof.ControllerGeneration != "controller-replay" ||
		proof.ExitCode != 7 || proof.ObservedAt.IsZero() {
		t.Fatalf("backpressured replay relabeled native wait: %+v", proof)
	}
}

func TestPositiveExitNativeWaitAndReplacementStayDistinct(t *testing.T) {
	cfg := positiveExitHostConfig(t)
	d, done := startPositiveExitHost(t, cfg)
	first := awaitAttachWithGenerations(t, d, "runtime-1", "controller-1")
	t.Cleanup(func() { _ = first.Stdin.Close() })
	openExitThread(t, first)
	sendFrame(t, first, `{"method":"exit"}`)
	proof := receivePositiveExit(t, first)
	if proof.SessionID != cfg.SessionID || proof.NativeThreadID != "exit-thread" || proof.OwnerID != first.OwnerID ||
		proof.OwnerID == "" || proof.AttachmentGeneration != first.AttachmentGeneration ||
		proof.RuntimeGeneration != "runtime-1" || proof.ControllerGeneration != "controller-1" || proof.ObservedAt.IsZero() || proof.ExitCode != 7 {
		t.Fatalf("incorrect native wait identity: %+v", proof)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("exited owner did not release its lock")
	}
	// A new host may use the same session/thread and attachment counter, but
	// cannot reuse its predecessor's owner or observation source.
	replacement, _ := startPositiveExitHost(t, cfg)
	second := awaitAttachWithGenerations(t, replacement, "runtime-2", "controller-2")
	t.Cleanup(func() { _ = second.Stdin.Close() })
	if second.OwnerID == first.OwnerID {
		t.Fatal("replacement reused native owner identity")
	}
	select {
	case p, ok := <-second.ProviderExit:
		t.Fatalf("replacement inherited predecessor exit: %+v, %v", p, ok)
	default:
	}
	openExitThread(t, second)
	sendFrame(t, second, `{"method":"exit-zero"}`)
	next := receivePositiveExit(t, second)
	if next.OwnerID != second.OwnerID || next.RuntimeGeneration != "runtime-2" || next.ControllerGeneration != "controller-2" || next.ExitCode != 0 {
		t.Fatalf("replacement evidence relabeled: %+v", next)
	}
}

func TestPositiveExitDetachIsUnknownAndReconnectHasNewLease(t *testing.T) {
	cfg := positiveExitHostConfig(t)
	d, _ := startPositiveExitHost(t, cfg)
	first := awaitAttachWithGenerations(t, d, "runtime-1", "controller-1")
	openExitThread(t, first)
	_ = first.Stdin.Close()
	requireUnknownExit(t, first.ProviderExit)
	second := awaitAttachWithGenerations(t, d, "runtime-2", "controller-2")
	t.Cleanup(func() { _ = second.Stdin.Close() })
	if second.OwnerID != first.OwnerID || second.AttachmentGeneration <= first.AttachmentGeneration {
		t.Fatal("live reconnect lost owner or reused lease")
	}
	// The already-open native thread survives; no duplicate thread/start.
	sendFrame(t, second, `{"method":"exit"}`)
	proof := receivePositiveExit(t, second)
	if proof.NativeThreadID != "exit-thread" || proof.ControllerGeneration != "controller-2" ||
		proof.AttachmentGeneration != second.AttachmentGeneration {
		t.Fatalf("reconnected observation used stale identity: %+v", proof)
	}
	requireUnknownExit(t, first.ProviderExit)
}

func TestPositiveExitMissingNativeHandleStaysUnknown(t *testing.T) {
	cfg := positiveExitHostConfig(t)
	d, _ := startPositiveExitHost(t, cfg)
	transport := awaitAttachWithGenerations(t, d, "runtime", "controller")
	t.Cleanup(func() { _ = transport.Stdin.Close() })
	sendFrame(t, transport, `{"method":"exit"}`)
	requireUnknownExit(t, transport.ProviderExit)
}

func TestPositiveExitObserverRejectsStaleOrForeignAttachment(t *testing.T) {
	for _, name := range []string{"token", "owner", "lease", "runtime", "controller", "absent-controller", "late-exit"} {
		t.Run(name, func(t *testing.T) {
			client, clientPeer := net.Pipe()
			defer func() { _ = client.Close(); _ = clientPeer.Close() }()
			h := &host{token: "secret", ownerID: "owner", client: client, clientGeneration: 2,
				runtimeGeneration: "runtime", controllerGeneration: "controller"}
			request := hello{Version: ProtocolVersion, Token: "secret", Action: "observe-exit", OwnerID: "owner",
				AttachmentGeneration: 2, RuntimeGeneration: "runtime", ControllerGeneration: "controller"}
			switch name {
			case "token":
				request.Token = "foreign"
			case "owner":
				request.OwnerID = "old-owner"
			case "lease":
				request.AttachmentGeneration = 1
			case "runtime":
				request.RuntimeGeneration = "old-runtime"
			case "controller":
				request.ControllerGeneration = "old-controller"
			case "absent-controller":
				h.client = nil
			case "late-exit":
				h.exiting = true
			}
			observer, peer := net.Pipe()
			defer func() { _ = observer.Close() }()
			_ = observer.SetDeadline(time.Now().Add(time.Second))
			done := make(chan struct{})
			go func() { h.handle(peer); close(done) }()
			if err := json.NewEncoder(observer).Encode(request); err != nil {
				t.Fatal(err)
			}
			var response helloResponse
			if err := json.NewDecoder(observer).Decode(&response); err != nil {
				t.Fatal(err)
			}
			if response.OK || response.ProviderExit != nil {
				t.Fatalf("stale observer admitted: %+v", response)
			}
			<-done
		})
	}
}

func TestPositiveExitUnknownWaitCannotPublish(t *testing.T) {
	// Even a native ProcessState is insufficient if this wait returned an
	// infrastructure error rather than a completed process wait.
	completed := exec.Command(os.Args[0], "-test.run=^TestPositiveExitProviderHelper$")
	completed.Env = append(os.Environ(), "AO_POSITIVE_EXIT_HELPER=1")
	if err := completed.Run(); err != nil {
		t.Fatal(err)
	}
	for _, child := range []*exec.Cmd{nil, {}, completed} {
		observer, peer := net.Pipe()
		h := &host{child: child, exitObserver: peer, codexNativeThreadID: "thread"}
		go h.finishProviderExit(errors.New("wait failed"))
		var response helloResponse
		if err := json.NewDecoder(observer).Decode(&response); err == nil {
			t.Fatalf("unknown wait published: %+v", response)
		}
		_ = observer.Close()
	}
}

func TestPositiveExitConcurrentRestartersKeepSingleHost(t *testing.T) {
	cfg := positiveExitHostConfig(t)
	d, _ := startPositiveExitHost(t, cfg)
	transport := awaitAttachWithGenerations(t, d, "runtime", "controller")
	t.Cleanup(func() { _ = transport.Stdin.Close() })
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if err := Run(context.Background(), cfg); !errors.Is(err, ErrHostExists) {
				t.Errorf("competing starter did not preserve owner: %v", err)
			}
		})
	}
	wg.Wait()
	openExitThread(t, transport)
	sendFrame(t, transport, `{"method":"exit"}`)
	if proof := receivePositiveExit(t, transport); proof.OwnerID != transport.OwnerID {
		t.Fatalf("competing starter changed evidence owner: %+v", proof)
	}
}

func TestPositiveExitNativeHandleRequiresCurrentSuccessfulResponse(t *testing.T) {
	for _, name := range []string{"start", "resume", "mismatch", "error", "absent", "late-controller", "superseded", "notification"} {
		t.Run(name, func(t *testing.T) {
			h := &host{clientGeneration: 1}
			request := `{"id":1,"method":"thread/start","params":{}}`
			if name == "resume" || name == "mismatch" {
				request = `{"id":1,"method":"thread/resume","params":{"threadId":"thread"}}`
			}
			h.observeCodexRequest([]byte(request))
			response := `{"id":1,"result":{"thread":{"id":"thread"}}}`
			switch name {
			case "mismatch":
				response = `{"id":1,"result":{"thread":{"id":"foreign"}}}`
			case "error":
				response = `{"id":1,"error":null,"result":{"thread":{"id":"thread"}}}`
			case "absent":
				response = `{"id":1,"result":{"thread":{}}}`
			case "late-controller":
				h.clientGeneration++
			case "superseded":
				h.observeCodexRequest([]byte(`{"id":2,"method":"thread/start","params":{}}`))
			case "notification":
				response = `{"method":"thread/started","params":{"thread":{"id":"thread"}}}`
			}
			h.observeCodexReadOnly([]byte(response))
			want := ""
			if name == "start" || name == "resume" {
				want = "thread"
			}
			if h.codexNativeThreadID != want {
				t.Fatalf("native handle = %q; want %q", h.codexNativeThreadID, want)
			}
		})
	}
}
