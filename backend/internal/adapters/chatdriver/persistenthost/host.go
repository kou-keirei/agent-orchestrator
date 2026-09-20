// Package persistenthost keeps a provider stdio process alive while AO's daemon
// is replaced. Raw protocols are forwarded unchanged. ACP uses a small relay
// that preserves connection-scoped initialization, session, request-correlation,
// and in-flight prompt state across daemon attachments.
package persistenthost

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
	"github.com/aoagents/agent-orchestrator/backend/internal/processalive"
)

const (
	// ProtocolVersion fences daemon/host control-plane compatibility.
	ProtocolVersion  = 1
	maxDetachedBytes = 32 << 20
	startupTimeout   = 10 * time.Second
	// Host hello is local control-plane I/O and must not stall daemon recovery.
	handshakeTimeout = time.Second
)

// Protocol selects the provider-wire behavior owned by the host.
type Protocol string

const (
	// ProtocolRaw preserves the original newline-delimited forwarding profile.
	ProtocolRaw Protocol = ""
	// ProtocolACP enables the host-owned ACP correlation and replay profile.
	ProtocolACP Protocol = "acp"

	// ACPPromptResultMethod is an AO-private notification emitted when an ACP
	// prompt finishes after the daemon attachment that issued it has gone away.
	ACPPromptResultMethod = "_ao/persistent_prompt_result"
	// ACPPromptAckMethod acknowledges that AO durably committed a replayed prompt
	// result, allowing the host to discard its prompt journal.
	ACPPromptAckMethod = "_ao/persistent_prompt_ack"
	// ACPInteractionCommandMethod records a user interaction decision in the host
	// before the daemon releases the blocked ACP responder.
	ACPInteractionCommandMethod = "_ao/persistent_interaction_command"
	// ACPRequestIDMetaKey carries one host-stable identity on replayable
	// provider-to-client requests such as permissions and elicitations.
	ACPRequestIDMetaKey = "ao.persistentRequestId"
	// ACPInitialPermissionsMetaKey records the process-launch approval policy,
	// which can differ from mutable session settings after reconnect.
	ACPInitialPermissionsMetaKey = "ao.initialPermissions"
)

var (
	// ErrAttached reports that another daemon controller owns the host.
	ErrAttached = errors.New("chat host already has a controller")
	// ErrUnauthorized reports an invalid host capability.
	ErrUnauthorized = errors.New("chat host authentication failed")
	// ErrIncompatible reports a control-plane or provider-ownership mismatch.
	ErrIncompatible = errors.New("chat host incompatible")
	// ErrHostExists reports an atomic per-session launch-lock conflict.
	ErrHostExists = errors.New("chat host already exists")
	// ErrOwnershipInconclusive means a descriptor or live host exists, but this
	// client could not prove that it is safe to replace. Callers must preserve the
	// durable session rather than treating the failed attachment as provider death.
	ErrOwnershipInconclusive = errors.New("chat host ownership is inconclusive")
)

// Descriptor is the private connection record published by a running host.
type Descriptor struct {
	Version              int       `json:"version"`
	SessionID            string    `json:"sessionId"`
	Protocol             Protocol  `json:"protocol,omitempty"`
	OwnershipFingerprint string    `json:"ownershipFingerprint,omitempty"`
	Address              string    `json:"address"`
	Token                string    `json:"token"`
	PID                  int       `json:"pid"`
	StartedAt            time.Time `json:"startedAt"`
}

// Config identifies one provider process and its AO session ownership.
type Config struct {
	SessionID            string
	DataDir              string
	Workdir              string
	Env                  []string
	Argv                 []string
	Protocol             Protocol
	OwnershipFingerprint string
	Prepare              func(context.Context) (PreparedProvider, error)
}

// PreparedProvider is resolved only when no compatible live host can be
// adopted. This keeps launch-only credential rotation out of reconnects.
type PreparedProvider struct {
	Env  []string
	Argv []string
}

// ACPState is the connection-scoped state needed to reconstruct an ACP client
// without initializing or resuming the same live provider a second time.
type ACPState struct {
	InitialPermissions   string          `json:"initialPermissions,omitempty"`
	InitializeResult     json.RawMessage `json:"initializeResult,omitempty"`
	SessionResult        json.RawMessage `json:"sessionResult,omitempty"`
	SessionID            string          `json:"sessionId,omitempty"`
	ActivePrompt         bool            `json:"activePrompt,omitempty"`
	ActiveCompaction     bool            `json:"activeCompaction,omitempty"`
	PendingResultEventID string          `json:"pendingResultEventId,omitempty"`
}

// Transport is one authenticated attachment to a persistent provider host.
type Transport struct {
	Stdin         io.WriteCloser
	Stdout        io.Reader
	Reconnected   bool
	NextRequestID int64
	ACPState      *ACPState
	// CodexReadOnly contains typed, host-owned receipts. A thread id by itself
	// is not permission evidence: callers must validate the correlated session,
	// runtime, controller, and permission-configuration generations as well.
	CodexReadOnly []CodexReadOnlyProof
}

// CodexReadOnlyProof is a host-driver receipt for one provider thread. The host
// creates it only after observing a matching read-only request and successful
// provider response. TurnID is empty until a successful turn/start response has
// been observed; an empty value never authorizes a different thread.
type CodexReadOnlyProof struct {
	SessionID                  string `json:"sessionId"`
	ThreadID                   string `json:"threadId"`
	TurnID                     string `json:"turnId,omitempty"`
	RuntimeGeneration          string `json:"runtimeGeneration"`
	ControllerGeneration       string `json:"controllerGeneration"`
	PermissionConfigGeneration uint64 `json:"permissionConfigGeneration"`
	ApprovalPolicy             string `json:"approvalPolicy"`
	Sandbox                    string `json:"sandbox"`
}

type hello struct {
	Version int    `json:"version"`
	Token   string `json:"token"`
	Action  string `json:"action"`
}

type helloResponse struct {
	OK            bool                 `json:"ok"`
	Error         string               `json:"error,omitempty"`
	NextRequestID int64                `json:"nextRequestId,omitempty"`
	ACPState      *ACPState            `json:"acpState,omitempty"`
	CodexReadOnly []CodexReadOnlyProof `json:"codexReadOnly,omitempty"`
}

func hostDir(dataDir, sessionID string) (string, error) {
	if sessionID == "" || filepath.Base(sessionID) != sessionID || strings.ContainsAny(sessionID, `/\\`) {
		return "", errors.New("invalid chat host session id")
	}
	if !filepath.IsAbs(dataDir) {
		return "", errors.New("chat host data dir must be absolute")
	}
	return filepath.Join(dataDir, "chat-hosts", sessionID), nil
}

func descriptorPath(dataDir, sessionID string) (string, error) {
	dir, err := hostDir(dataDir, sessionID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "host.json"), nil
}

func lockPath(dataDir, sessionID string) (string, error) {
	dir, err := hostDir(dataDir, sessionID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "host.lock"), nil
}

func acquireHostLock(dataDir, sessionID string) (func(), error) {
	dir, err := hostDir(dataDir, sessionID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // Directories need execute permission; access is owner-only.
		return nil, err
	}
	path, _ := lockPath(dataDir, sessionID)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // AO-owned capability directory.
	if errors.Is(err, os.ErrExist) {
		owner, readErr := os.ReadFile(path) //nolint:gosec // AO-owned path derived from validated session id.
		pid, parseErr := strconv.Atoi(strings.TrimSpace(string(owner)))
		if readErr != nil || parseErr != nil || pid <= 0 || processalive.Alive(pid) {
			return nil, ErrHostExists
		}
		// A host can die after atomically creating its launch lock but before it
		// publishes host.json. Reclaim only a lock whose recorded owner is proven
		// dead, then retry the same O_EXCL acquisition once.
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return nil, ErrHostExists
		}
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // AO-owned capability directory.
		if errors.Is(err, os.ErrExist) {
			return nil, ErrHostExists
		}
	}
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return func() { _ = os.Remove(path) }, nil
}

func readDescriptor(dataDir, sessionID string) (Descriptor, error) {
	path, err := descriptorPath(dataDir, sessionID)
	if err != nil {
		return Descriptor{}, err
	}
	b, err := os.ReadFile(path) //nolint:gosec // AO-owned path derived from validated session id.
	if err != nil {
		return Descriptor{}, err
	}
	var d Descriptor
	if err := json.Unmarshal(b, &d); err != nil {
		return Descriptor{}, fmt.Errorf("decode chat host descriptor: %w", err)
	}
	if d.SessionID != sessionID || d.Address == "" || d.Token == "" {
		return Descriptor{}, errors.New("invalid chat host descriptor")
	}
	return d, nil
}

func writeDescriptor(dataDir string, d Descriptor) error {
	dir, err := hostDir(dataDir, d.SessionID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // Directories need execute permission; access is owner-only.
		return err
	}
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".host.json.tmp-"+strconv.Itoa(os.Getpid()))
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	path := filepath.Join(dir, "host.json")
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Chmod(path, 0o600)
}

func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func validateDescriptor(cfg Config, d Descriptor) error {
	if d.Protocol != cfg.Protocol {
		return fmt.Errorf("%w: host protocol=%q requested=%q", ErrIncompatible, d.Protocol, cfg.Protocol)
	}
	// Codex raw hosts predate this ACP compatibility contract and retain their
	// existing version/protocol fencing.
	if d.Protocol == ProtocolRaw {
		return nil
	}
	if cfg.OwnershipFingerprint == "" || d.OwnershipFingerprint != cfg.OwnershipFingerprint {
		return fmt.Errorf("%w: provider launch configuration changed", ErrIncompatible)
	}
	return nil
}

// ConnectOrStart attaches to an existing compatible host or starts one with the
// current AO executable. Failed/incompatible probes never terminate that host.
func ConnectOrStart(ctx context.Context, cfg Config) (*Transport, error) {
	if d, err := readDescriptor(cfg.DataDir, cfg.SessionID); err == nil {
		if err := validateDescriptor(cfg, d); err != nil && processalive.Alive(d.PID) {
			return nil, err
		}
		transport, attachErr := attach(ctx, d, true)
		if attachErr == nil {
			return transport, nil
		}
		if errors.Is(attachErr, ErrAttached) {
			// Desktop updater handoff can briefly start the replacement daemon
			// before the old controller has detached. Wait for exclusive ownership;
			// never turn that overlap into a competing provider process.
			deadline := time.Now().Add(startupTimeout)
			for time.Now().Before(deadline) && processalive.Alive(d.PID) {
				timer := time.NewTimer(20 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}
				transport, attachErr = attach(ctx, d, true)
				if attachErr == nil {
					return transport, nil
				}
				if !errors.Is(attachErr, ErrAttached) {
					break
				}
			}
		}
		// A failed socket probe is not proof that a provider is dead. Only an OS
		// process observation permits clearing the descriptor and falling back to
		// native resume in a new host.
		if processalive.Alive(d.PID) {
			return nil, fmt.Errorf("%w: %w", ErrOwnershipInconclusive, attachErr)
		}
		// Do not remove stale ownership files here. Another starter may have
		// already reclaimed the dead lock and published a replacement descriptor
		// since this client read d. The detached host's O_EXCL lock acquisition is
		// the single authority for reclaiming dead ownership.
	} else if !errors.Is(err, os.ErrNotExist) {
		// A malformed or unreadable ownership record is not proof that no host
		// exists. Fail closed instead of launching a competing process.
		return nil, fmt.Errorf("%w: %w", ErrOwnershipInconclusive, err)
	}
	if !filepath.IsAbs(cfg.Workdir) {
		return nil, errors.New("chat host start requires an absolute workdir")
	}
	if cfg.Prepare != nil {
		prepared, err := cfg.Prepare(ctx)
		if err != nil {
			return nil, err
		}
		cfg.Env = prepared.Env
		cfg.Argv = prepared.Argv
	}
	if len(cfg.Argv) == 0 {
		return nil, errors.New("chat host start requires provider argv")
	}
	if err := spawnDetached(ctx, cfg); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(startupTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d, err := readDescriptor(cfg.DataDir, cfg.SessionID)
		if err == nil {
			if compatibleErr := validateDescriptor(cfg, d); compatibleErr != nil {
				return nil, compatibleErr
			}
			transport, attachErr := attach(ctx, d, false)
			if attachErr == nil {
				return transport, nil
			}
			lastErr = attachErr
		} else {
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	if d, readErr := readDescriptor(cfg.DataDir, cfg.SessionID); readErr == nil && processalive.Alive(d.PID) {
		return nil, fmt.Errorf("%w: %w", ErrOwnershipInconclusive, lastErr)
	}
	return nil, fmt.Errorf("start chat host: %w", lastErr)
}

// Reconcile terminates compatible authenticated hosts whose durable session no
// longer exists. Live-session hosts, incompatible hosts, and inconclusive probes
// are preserved.
func Reconcile(ctx context.Context, dataDir string, keep map[string]struct{}) error {
	root := filepath.Join(dataDir, "chat-hosts")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		sessionID := entry.Name()
		if _, ok := keep[sessionID]; ok {
			continue
		}
		d, readErr := readDescriptor(dataDir, sessionID)
		if readErr != nil {
			continue
		}
		if d.Version != ProtocolVersion {
			continue
		}
		if !processalive.Alive(d.PID) {
			// Claim the launch lock before cleaning a dead descriptor. If a new host
			// already owns it, preserve its descriptor instead of deleting ownership
			// state observed by a stale reconciler.
			release, lockErr := acquireHostLock(dataDir, sessionID)
			if lockErr != nil {
				continue
			}
			path, _ := descriptorPath(dataDir, sessionID)
			_ = os.Remove(path)
			release()
			continue
		}
		if shutdownErr := Shutdown(ctx, dataDir, sessionID); shutdownErr != nil {
			errs = append(errs, fmt.Errorf("reap chat host %s: %w", sessionID, shutdownErr))
		}
	}
	return errors.Join(errs...)
}

func attach(ctx context.Context, d Descriptor, reconnected bool) (*Transport, error) {
	if d.Version != ProtocolVersion {
		return nil, fmt.Errorf("%w: host=%d daemon=%d", ErrIncompatible, d.Version, ProtocolVersion)
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(handshakeCtx, "tcp", d.Address)
	if err != nil {
		return nil, err
	}
	finishHandshake := bindConnToContext(handshakeCtx, conn)
	reader := bufio.NewReader(conn)
	if err := json.NewEncoder(conn).Encode(hello{Version: ProtocolVersion, Token: d.Token, Action: "attach"}); err != nil {
		_ = conn.Close()
		return nil, finishHandshake(err)
	}
	var response helloResponse
	line, err := reader.ReadBytes('\n')
	if err != nil {
		_ = conn.Close()
		return nil, finishHandshake(err)
	}
	if err := finishHandshake(nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := json.Unmarshal(line, &response); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !response.OK {
		_ = conn.Close()
		switch response.Error {
		case ErrAttached.Error():
			return nil, ErrAttached
		case ErrUnauthorized.Error():
			return nil, ErrUnauthorized
		default:
			return nil, errors.New(response.Error)
		}
	}
	return &Transport{
		Stdin: conn, Stdout: reader, Reconnected: reconnected,
		NextRequestID: response.NextRequestID, ACPState: response.ACPState,
		CodexReadOnly: cloneCodexReadOnlyProofs(response.CodexReadOnly),
	}, nil
}

func bindConnToContext(ctx context.Context, conn net.Conn) func(error) error {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return func(err error) error {
		stop()
		if ctxErr := ctx.Err(); ctxErr != nil {
			_ = conn.Close()
			return ctxErr
		}
		return err
	}
}

// Shutdown terminates current session ownership and waits for it to end.
// Missing/dead hosts are harmless; unknown live owners fail closed.
// The protocol acknowledgement only confirms that shutdown was requested.
func Shutdown(ctx context.Context, dataDir, sessionID string) error {
	d, err := readDescriptor(dataDir, sessionID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if d.Version != ProtocolVersion {
		return ErrIncompatible
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(handshakeCtx, "tcp", d.Address)
	if err != nil {
		if !processalive.Alive(d.PID) {
			return nil
		}
		return err
	}
	defer func() { _ = conn.Close() }()
	finishHandshake := bindConnToContext(handshakeCtx, conn)
	if err := json.NewEncoder(conn).Encode(hello{Version: ProtocolVersion, Token: d.Token, Action: "shutdown"}); err != nil {
		return finishHandshake(err)
	}
	var response helloResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return finishHandshake(err)
	}
	if err := finishHandshake(nil); err != nil {
		return err
	}
	if !response.OK {
		return errors.New(response.Error)
	}
	waitCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, readErr := readDescriptor(dataDir, sessionID)
		if readErr == nil && current.Token != d.Token {
			return nil // A new owner could only publish after the old lock released.
		}
		if errors.Is(readErr, os.ErrNotExist) {
			path, _ := lockPath(dataDir, sessionID)
			if _, lockErr := os.Stat(path); errors.Is(lockErr, os.ErrNotExist) {
				return nil
			} else if lockErr != nil {
				return lockErr
			}
		} else if readErr != nil {
			return readErr
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for chat host shutdown: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// Run owns the provider until it exits or an authenticated shutdown arrives.
func Run(ctx context.Context, cfg Config) error {
	if len(cfg.Argv) == 0 || !filepath.IsAbs(cfg.Workdir) {
		return errors.New("chat host requires provider argv and absolute workdir")
	}
	releaseLock, err := acquireHostLock(cfg.DataDir, cfg.SessionID)
	if err != nil {
		return err
	}
	defer releaseLock()
	token, err := randomToken()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()

	child := aoprocess.Command(cfg.Argv[0], cfg.Argv[1:]...) //nolint:gosec // provider argv is constructed by AO's driver.
	child.Dir = cfg.Workdir
	child.Env = cfg.Env
	configureProviderProcess(child)
	stdin, err := child.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		return err
	}
	child.Stderr = io.Discard
	if err := child.Start(); err != nil {
		return err
	}

	d := Descriptor{
		Version: ProtocolVersion, SessionID: cfg.SessionID, Protocol: cfg.Protocol,
		OwnershipFingerprint: cfg.OwnershipFingerprint,
		Address:              listener.Addr().String(), Token: token, PID: os.Getpid(), StartedAt: time.Now().UTC(),
	}
	if err := writeDescriptor(cfg.DataDir, d); err != nil {
		_ = killProviderProcess(context.WithoutCancel(ctx), child)
		return err
	}
	path, _ := descriptorPath(cfg.DataDir, cfg.SessionID)
	defer func() { _ = os.Remove(path) }()

	h := &host{
		ctx: ctx, listener: listener, child: child, stdin: stdin, token: token,
		sessionID: cfg.SessionID, runtimeGeneration: token,
		detached: make([][]byte, 0, 64), pendingRequests: make(map[string]*pendingRequest),
		codexReadOnly:         make(map[string]codexReadOnlyReceipt),
		codexPermissions:      make(map[string]codexPermissionRequest),
		codexThreadGeneration: make(map[string]uint64),
		shutdown:              make(chan struct{}),
	}
	if cfg.Protocol == ProtocolACP {
		h.acp, err = newACPRelay(ctx, filepath.Join(filepath.Dir(path), "acp-prompt.journal"))
		if err != nil {
			_ = killProviderProcess(context.WithoutCancel(ctx), child)
			return err
		}
		defer func() { _ = h.acp.close(context.WithoutCancel(ctx)) }()
	}
	h.cond = sync.NewCond(&h.mu)
	providerDone := make(chan error, 1)
	go func() { providerDone <- h.forwardProvider(stdout) }()
	acceptDone := make(chan error, 1)
	go func() { acceptDone <- h.accept() }()

	stopProvider := func() {
		_ = stdin.Close()
		select {
		case <-providerDone:
			// Wrapper adapters may exit before their provider child. The hosted
			// process group is the ownership boundary, so explicit shutdown reaps
			// any descendant that did not follow stdin closure.
			_ = killProviderProcess(context.WithoutCancel(ctx), child)
		case <-time.After(3 * time.Second):
			_ = killProviderProcess(context.WithoutCancel(ctx), child)
		}
	}
	var runErr error
	select {
	case runErr = <-providerDone:
		_ = killProviderProcess(context.WithoutCancel(ctx), child)
	case <-h.shutdown:
		stopProvider()
	case <-ctx.Done():
		runErr = ctx.Err()
		stopProvider()
	case err := <-acceptDone:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			_ = killProviderProcess(context.WithoutCancel(ctx), child)
			return err
		}
	}
	_ = listener.Close()
	_ = child.Wait()
	return runErr
}

type host struct {
	ctx               context.Context
	listener          net.Listener
	child             *exec.Cmd
	stdin             io.WriteCloser
	token             string
	sessionID         string
	runtimeGeneration string
	acp               *acpRelay

	mu                        sync.Mutex
	cond                      *sync.Cond
	client                    net.Conn
	clientGeneration          uint64
	detached                  [][]byte
	detachedBytes             int
	pendingRequests           map[string]*pendingRequest
	pendingOrder              []string
	maxRequestID              int64
	codexReadOnly             map[string]codexReadOnlyReceipt
	codexPermissions          map[string]codexPermissionRequest
	codexThreadGeneration     map[string]uint64
	codexPermissionGeneration uint64
	shutdown                  chan struct{}
	shutdownOnce              sync.Once
}

type pendingRequest struct {
	frame    []byte
	buffered bool
}

type codexPermissionRequest struct {
	method             string
	threadID           string
	threadIDRequired   bool
	readOnly           bool
	permissionProvided bool
	generation         uint64
}

type codexReadOnlyReceipt struct {
	threadID          string
	turnID            string
	generation        uint64
	approvalPolicy    string
	sandbox           string
	runtimeGeneration string
}

func (h *host) accept() error {
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			return err
		}
		go h.handle(conn)
	}
}

func (h *host) handle(conn net.Conn) {
	reader := bufio.NewReader(conn)
	var request hello
	if err := json.NewDecoder(reader).Decode(&request); err != nil {
		_ = conn.Close()
		return
	}
	if request.Version != ProtocolVersion {
		_ = json.NewEncoder(conn).Encode(helloResponse{Error: ErrIncompatible.Error()})
		_ = conn.Close()
		return
	}
	if subtle.ConstantTimeCompare([]byte(request.Token), []byte(h.token)) != 1 {
		_ = json.NewEncoder(conn).Encode(helloResponse{Error: ErrUnauthorized.Error()})
		_ = conn.Close()
		return
	}
	if request.Action == "shutdown" {
		_ = json.NewEncoder(conn).Encode(helloResponse{OK: true})
		_ = conn.Close()
		h.shutdownOnce.Do(func() { close(h.shutdown) })
		return
	}
	if request.Action != "attach" {
		_ = json.NewEncoder(conn).Encode(helloResponse{Error: "unknown chat host action"})
		_ = conn.Close()
		return
	}

	h.mu.Lock()
	if h.client != nil {
		h.mu.Unlock()
		_ = json.NewEncoder(conn).Encode(helloResponse{Error: ErrAttached.Error()})
		_ = conn.Close()
		return
	}
	h.client = conn
	h.clientGeneration++
	generation := h.clientGeneration
	response := helloResponse{
		OK: true, NextRequestID: h.maxRequestID,
		CodexReadOnly: h.codexReadOnlySnapshotLocked(),
	}
	if h.acp != nil {
		response.ACPState = h.acp.snapshot()
	}
	writeErr := json.NewEncoder(conn).Encode(response)
	if writeErr == nil && h.acp != nil {
		writeErr = h.acp.replayTo(h.ctx, conn)
	}
	if writeErr == nil {
		for _, frame := range h.detached {
			if _, writeErr = conn.Write(frame); writeErr != nil {
				break
			}
		}
	}
	if writeErr == nil {
		h.detached = h.detached[:0]
		h.detachedBytes = 0
		for _, pending := range h.pendingRequests {
			pending.buffered = false
		}
		h.cond.Broadcast()
	}
	h.mu.Unlock()
	if writeErr != nil {
		h.detach(conn)
		return
	}

	for {
		frame, readErr := reader.ReadBytes('\n')
		if len(frame) > 0 {
			providerFrame := frame
			var clientFrame []byte
			h.mu.Lock()
			if h.acp == nil {
				if h.client != conn || generation != h.clientGeneration {
					h.mu.Unlock()
					_ = conn.Close()
					return
				}
				h.observeCodexRequest(frame)
			}
			if h.acp != nil {
				var relayErr error
				var relayed acpClientFrames
				relayed, relayErr = h.acp.clientFrame(h.ctx, frame, generation)
				providerFrame = relayed.provider
				clientFrame = relayed.client
				if relayErr != nil {
					h.shutdownOnce.Do(func() { close(h.shutdown) })
					h.mu.Unlock()
					h.detach(conn)
					return
				}
			}
			h.mu.Unlock()
			if len(providerFrame) > 0 {
				if _, err := h.stdin.Write(providerFrame); err != nil {
					readErr = err
				} else {
					h.observeClientFrame(providerFrame)
				}
			}
			if readErr == nil && len(clientFrame) > 0 {
				if _, err := conn.Write(clientFrame); err != nil {
					readErr = err
				}
			}
		}
		if readErr != nil {
			h.detach(conn)
			return
		}
	}
}

func (h *host) observeClientFrame(frame []byte) {
	var value struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if json.Unmarshal(frame, &value) != nil || len(value.ID) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if value.Method != "" {
		var id int64
		if json.Unmarshal(value.ID, &id) == nil && id > h.maxRequestID {
			h.maxRequestID = id
		}
		return
	}
	requestID := string(value.ID)
	if _, ok := h.pendingRequests[requestID]; ok {
		delete(h.pendingRequests, requestID)
		for i, id := range h.pendingOrder {
			if id == requestID {
				h.pendingOrder = append(h.pendingOrder[:i], h.pendingOrder[i+1:]...)
				break
			}
		}
	}
}

func (h *host) detach(conn net.Conn) {
	h.mu.Lock()
	if h.client == conn {
		h.client = nil
		h.bufferPendingRequestsLocked()
	}
	h.mu.Unlock()
	_ = conn.Close()
}

func (h *host) forwardProvider(stdout io.Reader) error {
	reader := bufio.NewReader(stdout)
	for {
		frame, err := reader.ReadBytes('\n')
		if len(frame) > 0 {
			h.mu.Lock()
			if h.acp == nil {
				h.observeCodexReadOnly(frame)
			}
			retainedByACP := false
			if h.acp != nil {
				var relayErr error
				frame, retainedByACP, relayErr = h.acp.providerFrame(
					h.ctx, frame, h.clientGeneration, h.client != nil,
				)
				if relayErr != nil {
					h.mu.Unlock()
					return relayErr
				}
			}
			if len(frame) == 0 {
				h.mu.Unlock()
				if err != nil {
					return err
				}
				continue
			}
			if requestID, ok := serverRequestID(frame); ok && !retainedByACP {
				if _, exists := h.pendingRequests[requestID]; !exists {
					h.pendingRequests[requestID] = &pendingRequest{frame: append([]byte(nil), frame...)}
					h.pendingOrder = append(h.pendingOrder, requestID)
				}
			}
			for h.client == nil && h.detachedBytes+len(frame) > maxDetachedBytes {
				h.cond.Wait()
			}
			if h.client != nil {
				if _, writeErr := h.client.Write(frame); writeErr != nil {
					_ = h.client.Close()
					h.client = nil
					h.bufferPendingRequestsLocked()
					if _, pending := serverRequestID(frame); !pending && !retainedByACP {
						h.bufferFrameLocked(frame)
					}
				}
			} else if !retainedByACP {
				if requestID, pending := serverRequestID(frame); !pending || !h.pendingRequests[requestID].buffered {
					h.bufferFrameLocked(frame)
					if pending {
						h.pendingRequests[requestID].buffered = true
					}
				}
			}
			h.mu.Unlock()
		}
		if err != nil {
			return err
		}
	}
}

// observeCodexRequest records a permission-bearing Codex request while h.mu is
// held. It never promotes a request to proof: only the matching provider
// response can do that.
func (h *host) observeCodexRequest(frame []byte) {
	var value struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if json.Unmarshal(frame, &value) != nil || !validRPCID(value.ID) {
		return
	}
	if value.Method != "thread/start" && value.Method != "thread/resume" && value.Method != "turn/start" {
		return
	}
	h.ensureCodexMapsLocked()
	h.codexPermissionGeneration++
	generation := h.codexPermissionGeneration

	params, paramsValid := objectParams(value.Params)
	posture := codexPermissionPosture{}
	if paramsValid {
		posture = requestedCodexPosture(params)
	} else {
		posture.valid = false
	}
	threadID, threadPresent, threadValid := jsonStringField(params, "threadId")
	if !paramsValid {
		threadID, threadPresent, threadValid = "", false, false
	}
	if threadPresent && (!threadValid || threadID == "") {
		threadID = ""
		threadValid = false
	}

	threadIDRequired := value.Method == "thread/resume" || value.Method == "turn/start"
	if !paramsValid || !threadValid || (threadIDRequired && !threadPresent) ||
		(value.Method == "thread/resume" && threadID == "") {
		h.invalidateAllCodexReadOnlyLocked(generation)
		threadID = ""
	}
	if value.Method == "thread/start" && !posture.valid {
		h.invalidateAllCodexReadOnlyLocked(generation)
	}

	if value.Method == "turn/start" {
		if threadID == "" {
			h.invalidateAllCodexReadOnlyLocked(generation)
		} else if posture.provided && (!posture.valid || !posture.readOnly) {
			// A broader, malformed, or conflicting turn posture invalidates the
			// thread receipt before the provider can answer.
			h.invalidateCodexReadOnlyLocked(threadID, generation)
		}
	} else if threadID != "" {
		// A new start/resume operation fences an older response for this thread.
		h.invalidateCodexReadOnlyLocked(threadID, generation)
	}

	h.codexPermissions[string(value.ID)] = codexPermissionRequest{
		method:             value.Method,
		threadID:           threadID,
		threadIDRequired:   threadIDRequired,
		readOnly:           posture.readOnly,
		permissionProvided: posture.provided,
		generation:         generation,
	}
}

// observeCodexReadOnly promotes only a successful, correlated provider
// response. JSON-RPC responses carrying an error never become permission
// evidence, even if they also contain a result-shaped payload.
func (h *host) observeCodexReadOnly(frame []byte) {
	var value struct {
		ID     json.RawMessage `json:"id"`
		Method json.RawMessage `json:"method"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.Unmarshal(frame, &value) != nil || !validRPCID(value.ID) ||
		(len(value.Method) > 0 && string(value.Method) != "null") {
		return
	}
	h.ensureCodexMapsLocked()
	requestID := string(value.ID)
	request, ok := h.codexPermissions[requestID]
	if !ok {
		return
	}
	delete(h.codexPermissions, requestID)
	if len(value.Error) > 0 {
		// Presence itself is enough to fail closed; a malformed/null error is
		// not a successful provider posture receipt either.
		h.invalidateCodexRequestLocked(request)
		return
	}

	if request.method == "turn/start" {
		h.observeCodexTurnResponseLocked(request, value.Result)
		return
	}

	threadID, approvalPolicy, sandbox, valid := codexThreadResponse(value.Result)
	if !valid || (request.threadID != "" && request.threadID != threadID) ||
		(request.threadIDRequired && request.threadID != threadID) ||
		!request.readOnly {
		target := threadID
		if request.threadID != "" {
			target = request.threadID
		}
		if target != "" {
			h.invalidateCodexReadOnlyLocked(target, request.generation)
		} else {
			h.invalidateAllCodexReadOnlyLocked(request.generation)
		}
		return
	}
	if current := h.codexThreadGeneration[threadID]; current > request.generation {
		// A newer operation already fenced this thread. This is a stale
		// response and must not restore its old proof.
		return
	}
	h.codexThreadGeneration[threadID] = request.generation
	h.codexReadOnly[threadID] = codexReadOnlyReceipt{
		threadID: threadID, generation: request.generation,
		approvalPolicy: approvalPolicy, sandbox: sandbox,
		runtimeGeneration: h.runtimeGeneration,
	}
}

func (h *host) observeCodexTurnResponseLocked(request codexPermissionRequest, raw json.RawMessage) {
	if request.threadID == "" {
		return
	}
	result, valid := jsonObject(raw)
	if !valid {
		h.invalidateCodexReadOnlyLocked(request.threadID, request.generation)
		return
	}
	turnRaw, present := result["turn"]
	if !present {
		h.invalidateCodexReadOnlyLocked(request.threadID, request.generation)
		return
	}
	var turn struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(turnRaw, &turn) != nil || turn.ID == "" {
		h.invalidateCodexReadOnlyLocked(request.threadID, request.generation)
		return
	}
	if current := h.codexThreadGeneration[request.threadID]; current > request.generation {
		return
	}
	receipt, exists := h.codexReadOnly[request.threadID]
	if !exists || (request.permissionProvided && !request.readOnly) {
		return
	}
	h.codexThreadGeneration[request.threadID] = request.generation
	receipt.threadID = request.threadID
	receipt.turnID = turn.ID
	receipt.generation = request.generation
	receipt.runtimeGeneration = h.runtimeGeneration
	h.codexReadOnly[request.threadID] = receipt
}

func (h *host) invalidateCodexRequestLocked(request codexPermissionRequest) {
	if request.threadID != "" {
		h.invalidateCodexReadOnlyLocked(request.threadID, request.generation)
		return
	}
	h.invalidateAllCodexReadOnlyLocked(request.generation)
}

func (h *host) invalidateCodexReadOnlyLocked(threadID string, generation uint64) {
	if threadID == "" {
		return
	}
	if current := h.codexThreadGeneration[threadID]; generation >= current {
		h.codexThreadGeneration[threadID] = generation
		delete(h.codexReadOnly, threadID)
	}
}

func (h *host) invalidateAllCodexReadOnlyLocked(generation uint64) {
	for threadID := range h.codexReadOnly {
		h.invalidateCodexReadOnlyLocked(threadID, generation)
	}
	for threadID, current := range h.codexThreadGeneration {
		if generation >= current {
			h.codexThreadGeneration[threadID] = generation
		}
	}
}

func (h *host) ensureCodexMapsLocked() {
	if h.codexReadOnly == nil {
		h.codexReadOnly = make(map[string]codexReadOnlyReceipt)
	}
	if h.codexPermissions == nil {
		h.codexPermissions = make(map[string]codexPermissionRequest)
	}
	if h.codexThreadGeneration == nil {
		h.codexThreadGeneration = make(map[string]uint64)
	}
}

func (h *host) codexReadOnlySnapshotLocked() []CodexReadOnlyProof {
	h.ensureCodexMapsLocked()
	threadIDs := make([]string, 0, len(h.codexReadOnly))
	for threadID, receipt := range h.codexReadOnly {
		if threadID == "" || receipt.generation == 0 ||
			h.codexThreadGeneration[threadID] != receipt.generation ||
			receipt.runtimeGeneration == "" || receipt.approvalPolicy != "never" ||
			receipt.sandbox != "readOnly" {
			continue
		}
		threadIDs = append(threadIDs, threadID)
	}
	sort.Strings(threadIDs)
	proofs := make([]CodexReadOnlyProof, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		receipt := h.codexReadOnly[threadID]
		proofs = append(proofs, CodexReadOnlyProof{
			SessionID:                  h.sessionID,
			ThreadID:                   receipt.threadID,
			TurnID:                     receipt.turnID,
			RuntimeGeneration:          receipt.runtimeGeneration,
			ControllerGeneration:       strconv.FormatUint(h.clientGeneration, 10),
			PermissionConfigGeneration: receipt.generation,
			ApprovalPolicy:             receipt.approvalPolicy,
			Sandbox:                    receipt.sandbox,
		})
	}
	return proofs
}

func cloneCodexReadOnlyProofs(proofs []CodexReadOnlyProof) []CodexReadOnlyProof {
	if len(proofs) == 0 {
		return nil
	}
	return append([]CodexReadOnlyProof(nil), proofs...)
}

type codexPermissionPosture struct {
	readOnly bool
	provided bool
	valid    bool
}

func requestedCodexPosture(params map[string]json.RawMessage) codexPermissionPosture {
	policy, policyPresent, policyValid := jsonStringField(params, "approvalPolicy")
	reviewer, reviewerPresent, reviewerValid := jsonStringField(params, "approvalsReviewer")
	sandbox, sandboxPresent, sandboxValid := requestedSandbox(params)
	provided := policyPresent || reviewerPresent || sandboxPresent
	valid := policyValid && reviewerValid && sandboxValid
	readOnly := valid && policyPresent && policy == "never" && sandboxPresent && sandbox == "read-only" &&
		(!reviewerPresent || reviewer == "user")
	return codexPermissionPosture{readOnly: readOnly, provided: provided, valid: valid}
}

func codexThreadResponse(raw json.RawMessage) (threadID, approvalPolicy, sandbox string, valid bool) {
	result, ok := jsonObject(raw)
	if !ok {
		return "", "", "", false
	}
	if _, hasError := result["error"]; hasError {
		return "", "", "", false
	}
	threadRaw, threadPresent := result["thread"]
	approvalRaw, approvalPresent := result["approvalPolicy"]
	sandboxRaw, sandboxPresent := result["sandbox"]
	if !threadPresent || !approvalPresent || !sandboxPresent {
		return "", "", "", false
	}
	var thread struct {
		ID string `json:"id"`
	}
	var sandboxObject struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(threadRaw, &thread) != nil || thread.ID == "" ||
		json.Unmarshal(approvalRaw, &approvalPolicy) != nil || approvalPolicy == "" ||
		json.Unmarshal(sandboxRaw, &sandboxObject) != nil || sandboxObject.Type == "" {
		return "", "", "", false
	}
	return thread.ID, approvalPolicy, sandboxObject.Type, true
}

func objectParams(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]json.RawMessage{}, true
	}
	return jsonObject(raw)
}

func jsonObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, false
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, false
	}
	return value, true
}

func jsonStringField(params map[string]json.RawMessage, name string) (string, bool, bool) {
	raw, present := params[name]
	if !present {
		return "", false, true
	}
	if len(raw) == 0 {
		return "", true, false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", true, false
	}
	return value, true, true
}

func requestedSandbox(params map[string]json.RawMessage) (string, bool, bool) {
	direct, directPresent, directValid := jsonStringField(params, "sandbox")
	policyRaw, policyPresent := params["sandboxPolicy"]
	policy, policyValid := "", true
	if policyPresent {
		var object map[string]json.RawMessage
		if object, policyValid = jsonObject(policyRaw); policyValid {
			var typePresent bool
			policy, typePresent, policyValid = jsonStringField(object, "type")
			policyValid = policyValid && typePresent && policy != ""
		}
	}
	if directPresent && !directValid || policyPresent && !policyValid {
		return "", true, false
	}
	if directPresent && policyPresent && canonicalSandbox(direct) != canonicalSandbox(policy) {
		return "", true, false
	}
	if directPresent {
		return canonicalSandbox(direct), true, true
	}
	if policyPresent {
		return canonicalSandbox(policy), true, true
	}
	return "", false, true
}

func canonicalSandbox(value string) string {
	switch value {
	case "readOnly", "read-only":
		return "read-only"
	case "workspaceWrite", "workspace-write":
		return "workspace-write"
	case "dangerFullAccess", "danger-full-access":
		return "danger-full-access"
	default:
		return value
	}
}

func validRPCID(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null" && json.Valid(raw)
}

func serverRequestID(frame []byte) (string, bool) {
	var value struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if json.Unmarshal(frame, &value) != nil || len(value.ID) == 0 || value.Method == "" {
		return "", false
	}
	return string(value.ID), true
}

func (h *host) bufferPendingRequestsLocked() {
	for _, requestID := range h.pendingOrder {
		pending := h.pendingRequests[requestID]
		if pending == nil || pending.buffered {
			continue
		}
		h.bufferFrameLocked(pending.frame)
		pending.buffered = true
	}
}

func (h *host) bufferFrameLocked(frame []byte) {
	h.detached = append(h.detached, append([]byte(nil), frame...))
	h.detachedBytes += len(frame)
}

// hostArgs keeps the Unix and Windows detached entry points on one wire format.
func hostArgs(cfg Config) []string {
	args := []string{"chat-host", cfg.SessionID, cfg.DataDir, cfg.Workdir}
	if cfg.Protocol != ProtocolRaw {
		args = append(args, string(cfg.Protocol), cfg.OwnershipFingerprint)
	}
	return append(append(args, "--"), cfg.Argv...)
}
