package codexappserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/persistenthost"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func positiveExitFixture() (*process, chan persistenthost.ProviderExitEvidence, persistenthost.ProviderExitEvidence) {
	source := make(chan persistenthost.ProviderExitEvidence, 1)
	proc := &process{
		providerExit: source, sessionID: "session", ownerID: "owner", attachmentGeneration: 3,
		runtimeGeneration: "runtime", controllerGeneration: "controller",
	}
	proof := persistenthost.ProviderExitEvidence{
		SessionID: "session", NativeThreadID: "thread-1", OwnerID: "owner", AttachmentGeneration: 3,
		RuntimeGeneration: "runtime", ControllerGeneration: "controller", ObservedAt: time.Now().UTC(), ExitCode: 7,
	}
	return proc, source, proof
}

func TestPositiveExitEvidenceRejectsMissingAndMismatchedIdentity(t *testing.T) {
	for _, name := range []string{"positive", "session", "thread", "owner", "lease", "runtime", "controller", "timestamp", "transport-loss", "no-source", "no-handle", "no-generation"} {
		t.Run(name, func(t *testing.T) {
			proc, source, proof := positiveExitFixture()
			threadID := "thread-1"
			switch name {
			case "session":
				proof.SessionID = "foreign"
			case "thread":
				proof.NativeThreadID = "foreign"
			case "owner":
				proof.OwnerID = "old-owner"
			case "lease":
				proof.AttachmentGeneration--
			case "runtime":
				proof.RuntimeGeneration = "old-runtime"
			case "controller":
				proof.ControllerGeneration = "old-controller"
			case "timestamp":
				proof.ObservedAt = time.Time{}
			case "no-source":
				proc.providerExit = nil
			case "no-handle":
				threadID = ""
			case "no-generation":
				proc.controllerGeneration = ""
			}
			c := &conversation{positiveExit: newPositiveExitEvidence(proc, threadID, "scope")}
			if name != "transport-loss" {
				source <- proof
			}
			close(source)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := c.PositiveExitEvidence(ctx)
			if name != "positive" {
				if !errors.Is(err, ports.ErrChatProviderExitUnproven) || got != (ports.ChatProviderExitEvidence{}) {
					t.Fatalf("invalid evidence admitted: %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.SessionID != proc.sessionID || got.ProviderConversationID != threadID || got.ProviderScopeID != "scope" ||
				got.RuntimeGeneration != proof.RuntimeGeneration || got.ControllerGeneration != proof.ControllerGeneration ||
				got.OwnerID != proof.OwnerID || got.AttachmentGeneration != proof.AttachmentGeneration ||
				got.ExitCode != 7 || !got.ObservedAt.Equal(proof.ObservedAt) {
				t.Fatalf("lost positive evidence identity: %+v, %v", got, err)
			}
		})
	}
}

func TestPositiveExitCancellationAndTimeoutNeverProveExit(t *testing.T) {
	proc, source, proof := positiveExitFixture()
	c := &conversation{positiveExit: newPositiveExitEvidence(proc, "thread-1", "scope")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := c.PositiveExitEvidence(ctx); !errors.Is(err, context.Canceled) || got != (ports.ChatProviderExitEvidence{}) {
		t.Fatalf("cancellation became exit: %+v, %v", got, err)
	}
	timeout, stop := context.WithTimeout(context.Background(), time.Millisecond)
	defer stop()
	if got, err := c.PositiveExitEvidence(timeout); !errors.Is(err, context.DeadlineExceeded) || got != (ports.ChatProviderExitEvidence{}) {
		t.Fatalf("timeout became exit: %+v, %v", got, err)
	}
	// A timed-out read cannot poison a later genuine observation or consume it.
	source <- proof
	close(source)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.PositiveExitEvidence(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPositiveExitConcurrentReadersAndReplacementCannotRelabel(t *testing.T) {
	proc, source, proof := positiveExitFixture()
	old := &conversation{positiveExit: newPositiveExitEvidence(proc, "thread-1", "scope")}
	replacementProc, replacementSource, replacementProof := positiveExitFixture()
	replacementProc.controllerGeneration = "replacement"
	replacementProc.attachmentGeneration++
	replacement := &conversation{positiveExit: newPositiveExitEvidence(replacementProc, "thread-1", "scope")}
	// A delayed old observation on a replacement source must be rejected.
	replacementSource <- replacementProof
	close(replacementSource)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			got, err := old.PositiveExitEvidence(ctx)
			if err != nil || got.ControllerGeneration != "controller" || got.AttachmentGeneration != 3 {
				t.Errorf("concurrent reader lost original fence: %+v, %v", got, err)
			}
		})
	}
	source <- proof
	close(source)
	wg.Wait()
	if got, err := replacement.PositiveExitEvidence(ctx); !errors.Is(err, ports.ErrChatProviderExitUnproven) || got != (ports.ChatProviderExitEvidence{}) {
		t.Fatalf("replacement inherited old evidence: %+v, %v", got, err)
	}
}

func TestPositiveExitDriverWiresHostSourceAndKeepsStoppedIndependent(t *testing.T) {
	d, server := newTestDriver(t)
	pipeProc, err := d.spawn(context.Background(), "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, source, proof := positiveExitFixture()
	d.persistent = true
	connects := 0
	d.connectHost = func(_ context.Context, cfg persistenthost.Config) (*persistenthost.Transport, error) {
		connects++
		if cfg.SessionID != "session" || cfg.RuntimeGeneration != "runtime" || cfg.ControllerGeneration != "controller" {
			t.Fatalf("host fence lost: %+v", cfg)
		}
		return &persistenthost.Transport{
			Stdin: pipeProc.stdin, Stdout: pipeProc.stdout,
			OwnerID: "owner", AttachmentGeneration: 3, ProviderExit: source,
		}, nil
	}
	conv, err := d.Start(context.Background(), ports.ChatStartConfig{
		SessionID: "session", RuntimeGeneration: "runtime", ControllerGeneration: "controller",
		WorkspacePath: t.TempDir(), DataDir: t.TempDir(), Permissions: ports.PermissionModeBypassPermissions,
		ProviderIDsScoped: true, ProviderScopeID: "scope",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := conv.(ports.ChatPositiveExitEvidenceReader)
	// A provider can emit process/exited (for a tool) or imitate AO fields on
	// stdout. Neither is the host's authenticated child wait connection.
	server.push(`{"method":"process/exited","params":{"exitCode":0}}`)
	_ = server.toClient.Close()
	event := nextEvent(t, conv.Events(), ports.ChatEventControllerState)
	if event.ControllerState != ports.ChatControllerStopped {
		t.Fatalf("state = %q", event.ControllerState)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if got, err := reader.PositiveExitEvidence(ctx); !errors.Is(err, context.DeadlineExceeded) || got != (ports.ChatProviderExitEvidence{}) {
		t.Fatalf("stdout EOF became positive exit: %+v, %v", got, err)
	}
	// A late actual host result belongs to this same original controller only.
	source <- proof
	close(source)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	got, err := reader.PositiveExitEvidence(ctx)
	if err != nil || got.ProviderConversationID != "thread-1" || got.ProviderScopeID != "scope" || connects != 1 {
		t.Fatalf("driver did not preserve source/native identity: %+v, %v, connects=%d", got, err, connects)
	}
}
