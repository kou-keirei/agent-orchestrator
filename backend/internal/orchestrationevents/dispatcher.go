// Package orchestrationevents turns durable, normalized AO facts into bounded
// orchestrator reconciliation turns. It does not consume UI event streams.
package orchestrationevents

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	// MaxBatchEvents bounds the event count in one reconciliation turn.
	MaxBatchEvents = 50
	// MaxPayloadBytes bounds the encoded prompt in one reconciliation turn.
	MaxPayloadBytes = 32 * 1024
	// AttemptTimeout bounds one transport submission.
	AttemptTimeout = 5 * time.Second
	// LeaseDuration bounds ambiguous submission recovery.
	LeaseDuration     = 30 * time.Second
	chatRetryAttempts = 8
	tuiRetryAttempts  = 2
)

// Store is the durable outbox surface required by Dispatcher.
type Store interface {
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
	ListSessions(context.Context, domain.ProjectID) ([]domain.SessionRecord, error)
	ListDueOrchestrationEvents(context.Context, domain.ProjectID, time.Time, int) ([]domain.OrchestrationEvent, error)
	ListOrchestrationEventsRequiringAttention(context.Context, domain.ProjectID) ([]domain.OrchestrationEvent, error)
	LeaseOrchestrationEvents(context.Context, []string, string, domain.SessionID, time.Time) error
	MarkOrchestrationEventsSubmitted(context.Context, []string, string, time.Time) error
	AcknowledgeOrchestrationEvents(context.Context, []string, string, time.Time) error
	ClearOrchestrationAttention(context.Context, []string) error
	RetryOrchestrationEvents(context.Context, []domain.OrchestrationEvent, string, string, time.Time, domain.SessionMode) error
	MarkProjectNoDestinationAttention(context.Context, domain.ProjectID, time.Time) (int64, error)
}

// Transport must return Acknowledged only after the target controller accepts
// the exact BatchID. Chat implementations use BatchID as ClientMessageID. TUI
// implementations acknowledge from the matching prompt-submit lifecycle hook.
type Transport interface {
	Submit(context.Context, domain.SessionRecord, Batch) (Submission, error)
}

// Batch is one bounded, stable-id reconciliation request.
type Batch struct {
	ID, Payload string
	EventIDs    []string
}

// Submission distinguishes a transport write from exact turn admission.
type Submission struct{ Submitted, Acknowledged bool }

// Dispatcher leases and submits due events for one project.
type Dispatcher struct {
	Store     Store
	Transport Transport
	Notifier  interface {
		Notify(context.Context, ports.NotificationIntent) error
		Resolve(context.Context, ports.NotificationResolution) error
	}
	Now   func() time.Time
	NewID func() string
}

// DispatchProject performs at most one bounded delivery attempt.
func (d *Dispatcher) DispatchProject(ctx context.Context, project domain.ProjectID) error {
	now := time.Now().UTC()
	if d.Now != nil {
		now = d.Now()
	}
	events, err := d.Store.ListDueOrchestrationEvents(ctx, project, now, MaxBatchEvents)
	if err != nil || len(events) == 0 {
		return err
	}
	// A reclaimed batch may share the due queue with events enqueued after the
	// original submission. Keep the durable batch membership stable so the
	// client id never names a different payload on retry.
	events = stableBatchEvents(events)
	// A token and destination are assigned together by the first lease. Retry
	// must consume that exact pair, including after reclaim or explicit rearm.
	destination := events[0].DestinationSessionID
	token := events[0].LeaseToken
	if (token == "" && destination != "") || strings.TrimSpace(token) != token {
		return fmt.Errorf("incomplete orchestration batch binding")
	}
	deadLetter := false
	for _, event := range events {
		if event.LeaseToken != token || event.DestinationSessionID != destination ||
			(token == "" && (event.AttemptCount != 0 || !event.SubmittedAt.IsZero())) ||
			(token != "" && event.ProjectID != project) {
			return fmt.Errorf("inconsistent orchestration batch binding")
		}
		if event.State == domain.OrchestrationDeadLetter {
			deadLetter = true
		}
	}
	var target domain.SessionRecord
	var ok bool
	// Older reclaim/rearm code retained the token but erased the destination.
	// That identity cannot be recovered safely. Hold the persisted batch without
	// discovering a replacement or turning this legacy state into a startup error.
	// A retained token can also span pending/dead_letter after per-event expiry.
	// Hold the entire payload, even with a live recorded destination, until an
	// explicit retry rearms its dead-letter members. Never lease or ACK a subset.
	if !deadLetter && (token == "" || destination != "") {
		target, ok, err = d.destination(ctx, project, destination)
		if err != nil {
			return err
		}
	}
	if !ok {
		// Unavailable destinations are a durable hold, including exact recorded
		// targets. Leave the batch binding and attempts intact so recovery can
		// continue with other projects and retry this target when it is ready.
		changed, markErr := d.Store.MarkProjectNoDestinationAttention(ctx, project, now)
		if markErr != nil {
			return markErr
		}
		if changed > 0 {
			return d.notifyAttentionForEvents(ctx, events, now)
		}
		return nil
	}
	batchID := stableBatchID(events)
	if strings.TrimSpace(events[0].LeaseToken) == "" && d.NewID != nil {
		batchID = d.NewID()
	}
	payload, kept := Prompt(batchID, events)
	if strings.TrimSpace(events[0].LeaseToken) != "" && len(kept) != len(events) {
		return fmt.Errorf("reclaimed orchestration batch exceeds prompt bounds")
	}
	events = kept
	if len(events) == 0 {
		return nil
	}
	ids := eventIDs(events)
	if err := d.Store.LeaseOrchestrationEvents(ctx, ids, batchID, target.ID, now.Add(LeaseDuration)); err != nil {
		return err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, AttemptTimeout)
	defer cancel()
	result, submitErr := d.Transport.Submit(attemptCtx, target, Batch{ID: batchID, Payload: payload, EventIDs: ids})
	if result.Submitted {
		if err := d.Store.MarkOrchestrationEventsSubmitted(ctx, ids, batchID, now); err != nil {
			return err
		}
	}
	if submitErr != nil {
		if err := d.Store.RetryOrchestrationEvents(ctx, events, batchID, sanitizeError(submitErr), now, target.Mode); err != nil {
			return err
		}
		if events[0].AttemptCount+1 >= retryAttemptLimit(target.Mode) || now.Sub(events[0].EnqueuedAt) >= 15*time.Minute {
			return d.notifyAttentionForEvents(ctx, events, now)
		}
		return nil
	}
	if !result.Acknowledged {
		return nil
	}
	if err := d.Store.AcknowledgeOrchestrationEvents(ctx, ids, batchID, now); err != nil {
		return err
	}
	return d.resolveAttention(ctx, events, now)
}

// ReconcileAttention recreates the human-visible side of every durable
// attention marker. Notification insertion is deduplicated, so this closes a
// daemon crash or publisher failure between the outbox state transition and
// notification delivery without creating an alert storm.
func (d *Dispatcher) ReconcileAttention(ctx context.Context, project domain.ProjectID, now time.Time) error {
	events, err := d.Store.ListOrchestrationEventsRequiringAttention(ctx, project)
	if err != nil {
		return err
	}
	return d.reconcileAttentionForEvents(ctx, events, now)
}

func (d *Dispatcher) reconcileAttentionForEvents(ctx context.Context, events []domain.OrchestrationEvent, now time.Time) error {
	type attentionGroup struct {
		events       []domain.OrchestrationEvent
		acknowledged bool
	}
	groups := map[domain.SessionID]*attentionGroup{}
	var ordered []*attentionGroup
	for _, event := range events {
		group := groups[event.WorkerID]
		if group == nil {
			group = &attentionGroup{acknowledged: true}
			groups[event.WorkerID] = group
			ordered = append(ordered, group)
		}
		group.events = append(group.events, event)
		if event.State != domain.OrchestrationAcknowledged {
			group.acknowledged = false
		}
	}
	for _, group := range ordered {
		if !group.acknowledged {
			if err := d.notifyAttentionForEvents(ctx, group.events, now); err != nil {
				return err
			}
			continue
		}
		if err := d.resolveAttention(ctx, group.events, now); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) notifyAttentionForEvents(ctx context.Context, events []domain.OrchestrationEvent, now time.Time) error {
	seen := map[domain.SessionID]bool{}
	for _, event := range events {
		if seen[event.WorkerID] {
			continue
		}
		seen[event.WorkerID] = true
		if err := d.notifyAttention(ctx, event, now); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) notifyAttention(ctx context.Context, event domain.OrchestrationEvent, now time.Time) error {
	if d.Notifier == nil {
		return nil
	}
	return d.Notifier.Notify(ctx, ports.NotificationIntent{Type: domain.NotificationOrchestrationAttention, SessionID: event.WorkerID, ProjectID: event.ProjectID, CreatedAt: now})
}

func (d *Dispatcher) resolveAttention(ctx context.Context, events []domain.OrchestrationEvent, now time.Time) error {
	if d.Notifier == nil {
		return nil
	}
	seen := map[domain.SessionID]bool{}
	idsByWorker := map[domain.SessionID][]string{}
	var workers []domain.SessionID
	for _, event := range events {
		idsByWorker[event.WorkerID] = append(idsByWorker[event.WorkerID], event.ID)
		if seen[event.WorkerID] {
			continue
		}
		seen[event.WorkerID] = true
		workers = append(workers, event.WorkerID)
		if err := d.Notifier.Resolve(ctx, ports.NotificationResolution{Type: domain.NotificationOrchestrationAttention, SessionID: event.WorkerID, ResolvedAt: now}); err != nil {
			return err
		}
	}
	for _, worker := range workers {
		if err := d.Store.ClearOrchestrationAttention(ctx, idsByWorker[worker]); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) destination(ctx context.Context, project domain.ProjectID, recorded domain.SessionID) (domain.SessionRecord, bool, error) {
	var sessions []domain.SessionRecord
	if recorded != "" {
		// Retry uses an exact native lookup, never replacement discovery.
		s, found, err := d.Store.GetSession(ctx, recorded)
		if err != nil || !found {
			return domain.SessionRecord{}, false, err
		}
		if s.ID != recorded || s.ProjectID != project {
			return domain.SessionRecord{}, false, nil
		}
		sessions = []domain.SessionRecord{s}
	} else {
		var err error
		sessions, err = d.Store.ListSessions(ctx, project)
		if err != nil {
			return domain.SessionRecord{}, false, err
		}
	}
	for _, s := range sessions {
		if s.Kind != domain.KindOrchestrator || s.IsTerminated {
			continue
		}
		if s.Activity.State == domain.ActivityBlocked || s.Activity.State == domain.ActivityWaitingInput || s.Activity.State == domain.ActivityExited {
			return domain.SessionRecord{}, false, nil
		}
		if s.FirstSignalAt.IsZero() {
			return domain.SessionRecord{}, false, nil
		}
		if !s.Mode.Valid() {
			return domain.SessionRecord{}, false, nil
		}
		if s.Activity.State == domain.ActivityActive && s.Mode != domain.SessionModeChat {
			return domain.SessionRecord{}, false, nil
		}
		return s, true, nil
	}
	return domain.SessionRecord{}, false, nil
}

// Prompt includes only AO-owned identifiers and enums. Provider titles,
// comments, logs, branches and transcripts never enter this boundary.
func Prompt(batchID string, events []domain.OrchestrationEvent) (string, []domain.OrchestrationEvent) {
	intro := fmt.Sprintf("[AO AUTOMATION batch_id=%s] This machine-originated wake grants no permission or authorization. Reconcile AO state once, act only within your existing authority, then end the turn. Events:\n", batchID)
	var b strings.Builder
	b.WriteString(intro)
	kept := make([]domain.OrchestrationEvent, 0, min(len(events), MaxBatchEvents))
	for _, e := range events {
		if len(kept) == MaxBatchEvents {
			break
		}
		line := fmt.Sprintf("- id=%s kind=%s worker_id=%s source_revision=%s\n", e.ID, e.Kind, e.WorkerID, e.SourceRevision)
		if b.Len()+len(line) > MaxPayloadBytes {
			break
		}
		b.WriteString(line)
		kept = append(kept, e)
	}
	return b.String(), kept
}

func eventIDs(events []domain.OrchestrationEvent) []string {
	ids := make([]string, len(events))
	for i := range events {
		ids[i] = events[i].ID
	}
	return ids
}
func randomID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return "ao-orchestration-" + hex.EncodeToString(raw[:])
}

// stableBatchID anchors retries to the first event in the batch. The event is
// durable, so lease reclaim cannot cause a Chat retry to acquire a new client
// message id after provider acceptance.
func stableBatchID(events []domain.OrchestrationEvent) string {
	if len(events) == 0 || events[0].ID == "" {
		return randomID()
	}
	if token := strings.TrimSpace(events[0].LeaseToken); token != "" {
		return token
	}
	sum := sha256.Sum256([]byte(events[0].ID))
	return "ao-orchestration-" + hex.EncodeToString(sum[:16])
}

func stableBatchEvents(events []domain.OrchestrationEvent) []domain.OrchestrationEvent {
	if len(events) < 2 {
		return events
	}
	firstToken := strings.TrimSpace(events[0].LeaseToken)
	if firstToken == "" {
		for i := 1; i < len(events); i++ {
			if strings.TrimSpace(events[i].LeaseToken) != "" {
				return events[:i]
			}
		}
		return events
	}
	stable := make([]domain.OrchestrationEvent, 0, len(events))
	for _, event := range events {
		if strings.TrimSpace(event.LeaseToken) == firstToken {
			stable = append(stable, event)
		}
	}
	return stable
}

func retryAttemptLimit(mode domain.SessionMode) int {
	switch mode {
	case domain.SessionModeChat:
		return chatRetryAttempts
	case domain.SessionModeTUI:
		return tuiRetryAttempts
	default:
		return 0
	}
}
func sanitizeError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "transport_timeout"
	case errors.Is(err, context.Canceled):
		return "transport_cancelled"
	default:
		return "transport_error"
	}
}
