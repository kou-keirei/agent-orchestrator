package store

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

var _ ports.ExactReportParentReader = (*Store)(nil)

// ReadReportOriginalParent projects only validated immutable issuance readback.
// GetWorkItemOriginalParent checks the full target, including issuer/provenance,
// against the stored work identity before any of those fields are discarded.
// This supplies neither live native continuity nor semantic ACK authority.
func (s *Store) ReadReportOriginalParent(ctx context.Context, work domain.WorkItemID, attempt domain.WorkAttemptID) (ports.ExactReportParent, error) {
	target, err := s.GetWorkItemOriginalParent(ctx, work, attempt)
	if err != nil {
		return ports.ExactReportParent{}, err
	}
	return ports.ExactReportParent{
		WorkItemID: target.WorkItemID, AttemptID: target.AttemptID,
		SessionID: target.OriginalParentSessionID, NativeSessionID: target.NativeSessionID,
		ControllerGeneration:   target.ControllerGeneration,
		ProviderConversationID: target.ProviderConversationID, ProviderScopeID: target.ProviderScopeID,
	}, nil
}
