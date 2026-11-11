package chat

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// OriginalParentIssuer captures the current AO-owned Chat controller for an
// internal work authorization. It must be acquired at authorization, before
// worker/report ingress. A captured capability never follows a replacement.
// Runtime WorkItem dispatch is not yet wired: its owner is the session-manager
// issuance path, which must acquire and consume this before dispatching work.
func (s *Service) OriginalParentIssuer(id domain.SessionID) (ports.WorkItemParentIssuer, error) {
	c, err := s.Controller(id)
	if err != nil {
		return nil, err
	}
	return originalParentIssuer{service: s, controller: c}, nil
}

type originalParentIssuer struct {
	service    *Service
	controller *Controller
}

var _ ports.WorkItemParentIssuer = originalParentIssuer{}
var _ ports.WorkItemSpawnIssuer = originalParentIssuer{}

func (issuer originalParentIssuer) IssueWorkItemFromOriginalParent(ctx context.Context, i domain.WorkItemIdentity, workerNativeID string) (domain.WorkItemParentTarget, error) {
	receipt, err := issuer.issue(ctx, i, workerNativeID, nil)
	return receipt.Parent, err
}

func (issuer originalParentIssuer) IssueWorkItemSpawn(ctx context.Context, i domain.WorkItemIdentity, workerNativeID string, grant domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
	if err := grant.Validate(); err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	return issuer.issue(ctx, i, workerNativeID, &grant)
}

func (issuer originalParentIssuer) issue(ctx context.Context, i domain.WorkItemIdentity, workerNativeID string, grant *domain.WorkLeaseGrant) (ports.WorkItemSpawnReceipt, error) {
	if err := ctx.Err(); err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	if err := i.Validate(); err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	s, c := issuer.service, issuer.controller
	held := &domain.WorkItemParentHold{Reason: domain.WorkItemParentControllerMismatch}
	if i.OriginalParentSessionID != c.sessionID {
		return ports.WorkItemSpawnReceipt{}, held
	}
	gate := s.controllerGate(c.sessionID)
	if err := gate.lock(ctx); err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	defer gate.unlock()
	current, err := s.Controller(c.sessionID)
	if err != nil || current != c {
		return ports.WorkItemSpawnReceipt{}, held
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	c.mu.Lock()
	available := (c.state == ports.ChatControllerReady || c.state == ports.ChatControllerBusy) &&
		c.handoff == controllerHandoffNone && !c.preserveProviderOnStop
	c.mu.Unlock()
	if !available {
		return ports.WorkItemSpawnReceipt{}, held
	}
	select {
	case <-c.stopped:
		return ports.WorkItemSpawnReceipt{}, held
	default:
	}
	persistence, ok := s.store.(ports.WorkItemParentPersistence)
	if !ok {
		return ports.WorkItemSpawnReceipt{}, &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
	}
	conversation, err := s.store.ConversationForSession(ctx, c.sessionID)
	if err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	if conversation.ID != c.conversation.ID {
		return ports.WorkItemSpawnReceipt{}, held
	}
	branch, err := s.store.ConversationBranch(ctx, conversation.ID, conversation.ActiveBranchID)
	if err != nil {
		return ports.WorkItemSpawnReceipt{}, err
	}
	// ChatConversation's native protocol handle is authoritative here. This does
	// not equate the separate terminal AgentSessionID for an arbitrary provider.
	nativeID := c.ProviderConversationID()
	target := domain.WorkItemParentTarget{
		WorkItemID: i.ID, AttemptID: i.AttemptID, OriginalParentSessionID: c.sessionID,
		NativeSessionID: nativeID, ControllerGeneration: c.generation,
		ProviderConversationID: nativeID, ProviderScopeID: branch.ProviderScopeID,
		IssuedBySessionID: c.sessionID, IssuedByControllerGeneration: c.generation,
		Provenance: domain.WorkItemParentLiveProvenance,
	}
	if grant != nil {
		atomic, ok := s.store.(ports.WorkItemSpawnPersistence)
		if !ok {
			return ports.WorkItemSpawnReceipt{}, &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
		}
		return atomic.PersistWorkItemSpawn(ctx, i, workerNativeID, target, *grant)
	}
	parent, err := persistence.PersistWorkItemOriginalParent(ctx, i, workerNativeID, target)
	return ports.WorkItemSpawnReceipt{Parent: parent}, err
}
