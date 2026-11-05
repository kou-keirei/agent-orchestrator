package codexappserver

import (
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/codexappserver/codexproto"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// Keep release admission at the fresh provider boundary. Reconnecting to an
// initialized host continues to use that host's protocol and generation fences.
func readSelectedCodexProtocol(dir, version string) (*codexProtocol, error) {
	if _, err := codexproto.MethodInventory(version); err != nil {
		return nil, fmt.Errorf("%w: %w", ports.ErrChatDriverIncompatible, err)
	}
	p, err := readCodexProtocol(dir, version)
	if err != nil {
		return nil, err
	}
	if err := codexproto.ValidateMethodInventory(version, p.methods); err != nil {
		return nil, fmt.Errorf("%w: %w", ports.ErrChatDriverIncompatible, err)
	}
	return p, nil
}
