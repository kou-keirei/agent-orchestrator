package ports

import (
	"context"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ErrChatProviderExitUnproven means no positive native process wait observation
// is available. Controller Stopped, EOF, cancellation and absence do not qualify.
var ErrChatProviderExitUnproven = errors.New("chat provider exit unproven")

// ChatProviderExitEvidence binds an OS process exit to the native conversation
// opened by this exact controller attachment. It does not authorize replacement,
// translate generations, prove history continuity, or acknowledge a message.
type ChatProviderExitEvidence struct {
	SessionID              domain.SessionID
	ProviderConversationID string
	ProviderScopeID        string
	RuntimeGeneration      string
	ControllerGeneration   string
	OwnerID                string
	AttachmentGeneration   uint64
	ExitCode               int
	ObservedAt             time.Time
}

// ChatPositiveExitEvidenceReader is an optional native adapter capability.
// The returned identity must be compared with the caller's existing ownership
// fence; it must never be relabeled as evidence for a replacement controller.
// A deadline bounds waiting and is never converted into an exit observation.
type ChatPositiveExitEvidenceReader interface {
	PositiveExitEvidence(context.Context) (ChatProviderExitEvidence, error)
}
