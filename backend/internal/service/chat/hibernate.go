package chat

import (
	"context"
	"errors"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// SetControllerRestorer is wired before serving. History/catalog reads never
// invoke it. The manager owns the shared restore attempt and operation fence.
func (s *Service) SetControllerRestorer(restore func(context.Context, domain.SessionID) error) {
	s.restoreController = restore
}

// Optional for embedders; a store without the durable work read cannot prove
// idleness. Do not fall back to queue reads that hide promotion reservations.
type unsettledTurnReader interface {
	HasUnsettledConversationTurns(context.Context, string) (bool, error)
}

// ChatNeedsController reports whether durable session facts require a controller.
// Missing or uncertain continuity is treated as requiring one.
func (s *Service) ChatNeedsController(ctx context.Context, id domain.SessionID) (bool, error) {
	rec, err := s.requireChatSession(ctx, id)
	if err != nil {
		return true, err
	}
	switch rec.Activity.State {
	case domain.ActivityIdle, domain.ActivityWaitingInput, domain.ActivityExited:
	default:
		return true, nil
	}
	if rec.Metadata.ConversationCheckpointUnsettled {
		return true, nil
	}
	conversation, err := s.store.ConversationForSession(ctx, id)
	if errors.Is(err, domain.ErrNoConversation) {
		return true, nil // Missing history is not proof of a settled worker.
	}
	if err != nil {
		return true, err
	}
	if conversation.ID == "" || conversation.SessionID != id {
		return true, errors.New("invalid Chat continuity owner")
	}
	work, ok := s.store.(unsettledTurnReader)
	if !ok {
		return true, nil
	}
	unfinished, err := work.HasUnsettledConversationTurns(ctx, conversation.ID)
	if err != nil || unfinished {
		return true, err
	}
	return s.store.HasPendingConversationInteractions(ctx, conversation.ID)
}

// workingController admits input under the existing start/stop gate. Release
// it before restoration, which takes that same gate while starting a provider.
func (s *Service) workingController(ctx context.Context, id domain.SessionID) (*Controller, func(), error) {
	gate := s.controllerGate(id)
	for restored := false; ; restored = true {
		if !gate.tryLock() {
			if controller, err := s.Controller(id); err == nil && controller.handoffActive() {
				return nil, nil, ErrControllerHandoff
			}
			if err := gate.lock(ctx); err != nil {
				return nil, nil, err
			}
		}
		rec, err := s.requireChatSession(ctx, id)
		if err != nil || rec.IsTerminated || rec.Activity.State == domain.ActivityExited {
			gate.unlock()
			if err == nil {
				err = ErrNoController
			}
			return nil, nil, err
		}
		controller, err := s.Controller(id)
		if err == nil && controller.State() != ports.ChatControllerStopped {
			return controller, gate.unlock, nil
		}
		gate.unlock()
		if s.restoreController == nil {
			return nil, nil, ErrNoController
		}
		if restored {
			return nil, nil, fmt.Errorf("%w: %w", ports.ErrChatControllerRestore, ErrNoController)
		}
		if err := s.restoreController(ctx, id); err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ports.ErrChatControllerRestore, err)
		}
	}
}

// Input admission order survives the interval before a controller/sendMu exists.
func (s *Service) inputGate(id domain.SessionID) controllerGate {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	gate := s.inputGates[id]
	if gate == nil {
		gate = make(controllerGate, 1)
		s.inputGates[id] = gate
	}
	return gate
}

func (s *Service) absentControllerState(ctx context.Context, rec domain.SessionRecord) ports.ChatControllerState {
	if s.restoreController != nil && !rec.IsTerminated && rec.Kind == domain.KindWorker &&
		rec.Metadata.WorkspacePath != "" && rec.Metadata.ProviderConversationID != "" &&
		(rec.Activity.State == domain.ActivityIdle || rec.Activity.State == domain.ActivityWaitingInput) {
		if work, err := s.ChatNeedsController(ctx, rec.ID); err == nil && !work {
			return ports.ChatControllerCold
		}
	}
	return ports.ChatControllerStopped
}
