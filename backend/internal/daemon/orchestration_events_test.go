package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
)

type automationSenderFake struct {
	id           domain.SessionID
	message, key string
	err          error
	acknowledged bool
}

func (f *automationSenderFake) SendAutomation(_ context.Context, id domain.SessionID, message, key string) error {
	f.id = id
	f.message = message
	f.key = key
	return f.err
}

func (f *automationSenderFake) SendAutomationWithAcknowledgement(ctx context.Context, id domain.SessionID, message, key string) (bool, error) {
	err := f.SendAutomation(ctx, id, message, key)
	return f.acknowledged, err
}

func TestOrchestrationTransportChatAcknowledgesStableClientMessageID(t *testing.T) {
	s := &automationSenderFake{acknowledged: true}
	result, err := (orchestrationTransport{sender: s}).Submit(context.Background(), domain.SessionRecord{ID: "o", Mode: domain.SessionModeChat}, orchestrationevents.Batch{ID: "batch", Payload: "safe"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Submitted || !result.Acknowledged || s.key != "batch" {
		t.Fatalf("result=%+v key=%q", result, s.key)
	}
}

func TestOrchestrationTransportUsesAcceptedControllerAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name         string
		selected     domain.SessionMode
		acknowledged bool
	}{
		{"Chat snapshot, TUI admission", domain.SessionModeChat, false},
		{"TUI snapshot, Chat admission", domain.SessionModeTUI, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &automationSenderFake{acknowledged: tc.acknowledged}
			result, err := (orchestrationTransport{sender: s}).Submit(context.Background(), domain.SessionRecord{ID: "o", Mode: tc.selected}, orchestrationevents.Batch{ID: "batch", Payload: "safe"})
			if err != nil || !result.Submitted || result.Acknowledged != tc.acknowledged || s.key != "batch" {
				t.Fatalf("result=%+v key=%q err=%v", result, s.key, err)
			}
		})
	}
}

type automationErrorOnlySender struct{ calls int }

func (s *automationErrorOnlySender) SendAutomation(context.Context, domain.SessionID, string, string) error {
	s.calls++
	return nil
}

func TestOrchestrationTransportErrorOnlySenderCannotClaimChatAcknowledgement(t *testing.T) {
	s := &automationErrorOnlySender{}
	result, err := (orchestrationTransport{sender: s}).Submit(context.Background(), domain.SessionRecord{ID: "o", Mode: domain.SessionModeChat}, orchestrationevents.Batch{ID: "batch", Payload: "safe"})
	if err != nil || !result.Submitted || result.Acknowledged || s.calls != 1 {
		t.Fatalf("result=%+v calls=%d err=%v", result, s.calls, err)
	}
}
func TestOrchestrationTransportTUIWaitsForHookAcknowledgement(t *testing.T) {
	s := &automationSenderFake{}
	result, err := (orchestrationTransport{sender: s}).Submit(context.Background(), domain.SessionRecord{ID: "o", Mode: domain.SessionModeTUI}, orchestrationevents.Batch{ID: "batch", Payload: "safe"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Submitted || result.Acknowledged {
		t.Fatalf("result=%+v", result)
	}
}
func TestOrchestrationTransportFailureClaimsNoSubmission(t *testing.T) {
	s := &automationSenderFake{err: errors.New("no")}
	result, err := (orchestrationTransport{sender: s}).Submit(context.Background(), domain.SessionRecord{ID: "o", Mode: domain.SessionModeChat}, orchestrationevents.Batch{ID: "batch", Payload: "safe"})
	if err == nil || result.Submitted || result.Acknowledged {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
