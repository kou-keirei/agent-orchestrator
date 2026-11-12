package codexappserver

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var _ ports.ChatPositiveExitEvidenceReader = (*conversation)(nil)

// positiveExitEvidence has one immutable result, shared by concurrent readers.
// Only the host control channel can supply process evidence. The native thread
// comes from the successful Start/Resume on that same process, never stdout EOF
// or an arbitrary notification. There is no generation conversion here.
type positiveExitEvidence struct {
	done     chan struct{}
	evidence ports.ChatProviderExitEvidence
	err      error
}

func newPositiveExitEvidence(proc *process, threadID, scopeID string) *positiveExitEvidence {
	if proc.providerExit == nil || proc.sessionID == "" || threadID == "" || proc.ownerID == "" ||
		proc.attachmentGeneration == 0 || proc.runtimeGeneration == "" || proc.controllerGeneration == "" {
		return nil
	}
	source := &positiveExitEvidence{done: make(chan struct{}), err: ports.ErrChatProviderExitUnproven}
	go func() {
		defer close(source.done)
		proof, ok := <-proc.providerExit
		if !ok || proof.SessionID != string(proc.sessionID) || proof.NativeThreadID != threadID || proof.OwnerID != proc.ownerID ||
			proof.AttachmentGeneration != proc.attachmentGeneration || proof.RuntimeGeneration != proc.runtimeGeneration ||
			proof.ControllerGeneration != proc.controllerGeneration || proof.ObservedAt.IsZero() {
			return
		}
		source.evidence = ports.ChatProviderExitEvidence{
			SessionID: proc.sessionID, ProviderConversationID: threadID, ProviderScopeID: scopeID,
			RuntimeGeneration: proof.RuntimeGeneration, ControllerGeneration: proof.ControllerGeneration,
			OwnerID: proof.OwnerID, AttachmentGeneration: proof.AttachmentGeneration,
			ExitCode: proof.ExitCode, ObservedAt: proof.ObservedAt,
		}
		source.err = nil
	}()
	return source
}

// PositiveExitEvidence waits only for this attachment's host observation. A
// detached/crashed host closes the source without proof; a missing native handle
// cannot qualify even when a process wait was observed. Stopped stays independent.
func (c *conversation) PositiveExitEvidence(ctx context.Context) (ports.ChatProviderExitEvidence, error) {
	if err := ctx.Err(); err != nil {
		return ports.ChatProviderExitEvidence{}, err
	}
	if c.positiveExit == nil {
		return ports.ChatProviderExitEvidence{}, ports.ErrChatProviderExitUnproven
	}
	select {
	case <-ctx.Done():
		return ports.ChatProviderExitEvidence{}, ctx.Err()
	case <-c.positiveExit.done:
		if err := ctx.Err(); err != nil {
			return ports.ChatProviderExitEvidence{}, err
		}
		return c.positiveExit.evidence, c.positiveExit.err
	}
}
