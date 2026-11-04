package chat

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// SetInputAdmission is wired before serving. Restoration must finish before
// acquiring this lease: the manager's restore operation itself drains input.
func (s *Service) SetInputAdmission(admit func(context.Context, domain.SessionID, string) (func(), error)) {
	s.admitInput = admit
}

// The caller holds the controller gate, so the selected generation cannot be
// replaced between admission and delivery. The manager validates durable owner
// facts under its input lease, which excludes switches and other mutations.
func (s *Service) acquireInput(ctx context.Context, id domain.SessionID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.admitInput == nil {
		return func() {}, nil
	}
	controller, err := s.Controller(id)
	if err != nil {
		return nil, err
	}
	return s.acquireControllerInput(ctx, id, controller, func() (func(), error) {
		return s.admitInput(ctx, id, controller.Generation())
	})
}

// Validate the selected durable owner across admission, while the controller
// gate prevents local replacement. The manager owns the exclusion lease.
func (s *Service) acquireControllerInput(ctx context.Context, id domain.SessionID, controller *Controller, admit func() (func(), error)) (func(), error) {
	selected, err := s.requireChatSession(ctx, id)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	cfg := s.startConfigs[id]
	s.mu.RUnlock()
	if selected.ID != id || selected.IsTerminated ||
		selected.ControllerOwner() != cfg.ExpectedControllerOwner ||
		selected.Metadata.WorkspacePath != cfg.WorkspacePath ||
		selected.Metadata.ControllerGeneration != controller.Generation() ||
		selected.Metadata.ProviderConversationID != controller.ProviderConversationID() {
		return nil, ErrControllerHandoff
	}
	release, err := admit()
	if err != nil {
		return nil, err
	}
	current, err := s.requireChatSession(ctx, id)
	if err == nil && (current.ID != id || current.ControllerOwner() != selected.ControllerOwner() ||
		current.Metadata.RuntimeHandleID != selected.Metadata.RuntimeHandleID ||
		current.Metadata.WorkspacePath != selected.Metadata.WorkspacePath ||
		current.Metadata.WorkspaceRepoPath != selected.Metadata.WorkspaceRepoPath ||
		controller.State() == ports.ChatControllerStopped) {
		err = ErrControllerHandoff
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (s *Service) inputController(ctx context.Context, id domain.SessionID) (*Controller, func(), error) {
	gate := s.inputGate(id)
	if err := gate.lock(ctx); err != nil {
		return nil, nil, err
	}
	controller, release, err := s.workingController(ctx, id)
	if err != nil {
		gate.unlock()
		return nil, nil, err
	}
	releaseInput, err := s.acquireInput(ctx, id)
	if err != nil {
		release()
		gate.unlock()
		return nil, nil, err
	}
	return controller, func() {
		releaseInput()
		release()
		gate.unlock()
	}, nil
}
