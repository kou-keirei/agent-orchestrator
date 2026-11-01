package orchestrationevents

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type fakeStore struct {
	held                              bool
	holdErr                           error
	afterLease                        func()
	sessions                          []domain.SessionRecord
	events                            []domain.OrchestrationEvent
	attentionEvents                   []domain.OrchestrationEvent
	leased, submitted, acked, retried []string
	cleared                           []string
	destination                       domain.SessionID
	leaseErr, submitErr, ackErr       error
	attentionChanged                  int64
}

func (f *fakeStore) OrchestrationDispatchHeld(context.Context, domain.ProjectID) (bool, error) {
	return f.held, f.holdErr
}
func (f *fakeStore) WithOrchestrationDispatch(_ context.Context, _ domain.ProjectID, fn func() error) error {
	if f.holdErr != nil {
		return f.holdErr
	}
	if f.held {
		return ErrDispatchHeld
	}
	return fn()
}
func (f *fakeStore) ListSessions(_ context.Context, p domain.ProjectID) ([]domain.SessionRecord, error) {
	out := slices.Clone(f.sessions)
	for i := range out {
		if out[i].ProjectID == "" {
			out[i].ProjectID = p
		}
	}
	return out, nil
}
func (f *fakeStore) ListDueOrchestrationEvents(_ context.Context, p domain.ProjectID, _ time.Time, _ int) ([]domain.OrchestrationEvent, error) {
	out := slices.Clone(f.events)
	for i := range out {
		if out[i].ProjectID == "" {
			out[i].ProjectID = p
		}
	}
	return out, nil
}
func (f *fakeStore) ListOrchestrationEventsRequiringAttention(context.Context, domain.ProjectID) ([]domain.OrchestrationEvent, error) {
	return f.attentionEvents, nil
}
func (f *fakeStore) LeaseOrchestrationEvents(_ context.Context, ids []string, _ string, d domain.SessionID, _ time.Time) error {
	if f.leaseErr != nil {
		return f.leaseErr
	}
	f.leased = ids
	f.destination = d
	if f.afterLease != nil {
		f.afterLease()
	}
	return nil
}
func (f *fakeStore) MarkOrchestrationEventsSubmitted(_ context.Context, ids []string, _ string, _ time.Time) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = ids
	return nil
}
func (f *fakeStore) AcknowledgeOrchestrationEvents(_ context.Context, ids []string, _ string, _ time.Time) error {
	if f.ackErr != nil {
		return f.ackErr
	}
	f.acked = ids
	return nil
}
func (f *fakeStore) ClearOrchestrationAttention(_ context.Context, ids []string) error {
	f.cleared = append(f.cleared, ids...)
	return nil
}

func TestFaultBoundariesDoNotClaimUncommittedDelivery(t *testing.T) {
	now := time.Now()
	session := domain.SessionRecord{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}
	event := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTerminated, SourceRevision: "r", EnqueuedAt: now}
	wantErr := errors.New("injected boundary failure")
	for _, tc := range []struct {
		name       string
		configure  func(*fakeStore)
		wantWrites int
		wantAck    bool
	}{
		{"after_outbox_before_lease", func(s *fakeStore) { s.leaseErr = wantErr }, 0, false},
		{"after_lease_before_submitted", func(s *fakeStore) { s.submitErr = wantErr }, 1, false},
		{"after_submission_before_ack", func(s *fakeStore) { s.ackErr = wantErr }, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{sessions: []domain.SessionRecord{session}, events: []domain.OrchestrationEvent{event}}
			tc.configure(store)
			transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
			err := (&Dispatcher{Store: store, Transport: transport, Now: func() time.Time { return now }, NewID: func() string { return "batch" }}).DispatchProject(context.Background(), "p")
			if !errors.Is(err, wantErr) || transport.calls != tc.wantWrites || (len(store.acked) > 0) != tc.wantAck {
				t.Fatalf("err=%v writes=%d acked=%v", err, transport.calls, store.acked)
			}
		})
	}
}

func TestSubmittedTUIBatchRemainsUnacknowledgedForRestartRecovery(t *testing.T) {
	now := time.Now()
	store := &fakeStore{
		sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeTUI, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}},
		events:   []domain.OrchestrationEvent{{ID: "e", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "r", EnqueuedAt: now}},
	}
	transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: false}}
	if err := (&Dispatcher{Store: store, Transport: transport}).DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if len(store.submitted) != 1 || len(store.acked) != 0 {
		t.Fatalf("submitted=%v acked=%v", store.submitted, store.acked)
	}
}

func TestChatRetryReusesStableBatchIdentityAfterLeaseRecovery(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}},
		events:   []domain.OrchestrationEvent{{ID: "durable-event", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "7", EnqueuedAt: now}},
	}
	transport := &fakeTransport{result: Submission{Submitted: true}, err: errors.New("provider unavailable")}
	d := &Dispatcher{Store: store, Transport: transport, Now: func() time.Time { return now }}
	if err := d.DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	first := transport.batch.ID
	if first == "" {
		t.Fatal("first batch id is empty")
	}
	if err := d.DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if transport.batch.ID != first {
		t.Fatalf("retry batch id=%q, want stable %q", transport.batch.ID, first)
	}
}

func TestReclaimedBatchDoesNotAbsorbNewEventsUnderSameClientID(t *testing.T) {
	now := time.Now().UTC()
	events := []domain.OrchestrationEvent{
		{ID: "old-event", LeaseToken: "ao-orchestration-old", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTurnSettled, EnqueuedAt: now},
		{ID: "new-event", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, EnqueuedAt: now.Add(time.Second)},
	}
	got := stableBatchEvents(events)
	if len(got) != 1 || got[0].ID != "old-event" {
		t.Fatalf("stable batch events=%v, want only reclaimed batch", got)
	}
	if gotID := stableBatchID(got); gotID != "ao-orchestration-old" {
		t.Fatalf("stable batch id=%q, want durable lease token", gotID)
	}
}

func TestReclaimedBatchLoadsAllNoncontiguousLeaseMembers(t *testing.T) {
	now := time.Now().UTC()
	events := []domain.OrchestrationEvent{
		{ID: "old-event", LeaseToken: "ao-orchestration-old", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTurnSettled, EnqueuedAt: now},
		{ID: "new-event", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, EnqueuedAt: now.Add(time.Second)},
		{ID: "old-event-2", LeaseToken: "ao-orchestration-old", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerReadyMerge, EnqueuedAt: now.Add(2 * time.Second)},
	}
	got := stableBatchEvents(events)
	if len(got) != 2 || got[0].ID != "old-event" || got[1].ID != "old-event-2" {
		t.Fatalf("stable batch events=%v, want all reclaimed members only", got)
	}
}

func TestPromptWithNoRetainedEventsDoesNotLeaseOrSubmit(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}},
		events:   []domain.OrchestrationEvent{{ID: strings.Repeat("x", MaxPayloadBytes), ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, EnqueuedAt: now}},
	}
	transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
	if err := (&Dispatcher{Store: store, Transport: transport, Now: func() time.Time { return now }}).DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 0 || len(store.leased) != 0 || len(store.retried) != 0 {
		t.Fatalf("empty retained batch performed delivery work: calls=%d leased=%v retried=%v", transport.calls, store.leased, store.retried)
	}
}

func TestUnknownDestinationModeFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeStore{
		sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionMode("future"), Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}},
		events:   []domain.OrchestrationEvent{{ID: "e", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, EnqueuedAt: now}},
	}
	transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
	if err := (&Dispatcher{Store: store, Transport: transport, Now: func() time.Time { return now }}).DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 0 || len(store.leased) != 0 {
		t.Fatalf("unknown destination mode was dispatched: calls=%d leased=%v", transport.calls, store.leased)
	}
}

func TestReclaimedBatchCannotChangePayloadAtPromptBounds(t *testing.T) {
	for _, count := range []int{1, MaxBatchEvents + 1} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			now := time.Now().UTC()
			events := make([]domain.OrchestrationEvent, count)
			for i := range events {
				events[i] = domain.OrchestrationEvent{ID: strconv.Itoa(i), LeaseToken: "reclaimed", Kind: domain.OrchestrationWorkerBlocked}
			}
			if count == 1 {
				events[0].ID = strings.Repeat("x", MaxPayloadBytes)
			}
			store := &fakeStore{events: events, sessions: []domain.SessionRecord{{ID: "o", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, FirstSignalAt: now}}}
			transport := &fakeTransport{}
			if err := (&Dispatcher{Store: store, Transport: transport}).DispatchProject(context.Background(), "p"); err == nil {
				t.Fatal("oversized reclaimed payload was accepted")
			}
			if transport.calls != 0 || len(store.leased) != 0 {
				t.Fatalf("partial replay calls=%d leased=%v", transport.calls, store.leased)
			}
		})
	}
}

func TestAttentionResolutionPreservesFirstSeenWorkerAndEventOrder(t *testing.T) {
	now := time.Now().UTC()
	events := []domain.OrchestrationEvent{
		{ID: "e1", WorkerID: "w2", State: domain.OrchestrationAcknowledged},
		{ID: "e2", WorkerID: "w1", State: domain.OrchestrationAcknowledged},
		{ID: "e3", WorkerID: "w2", State: domain.OrchestrationAcknowledged},
	}
	store := &fakeStore{attentionEvents: events}
	notifier := &fakeNotifier{}
	if err := (&Dispatcher{Store: store, Notifier: notifier}).ReconcileAttention(context.Background(), "p", now); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(notifier.resolved, []domain.SessionID{"w2", "w1"}) || !slices.Equal(store.cleared, []string{"e1", "e3", "e2"}) {
		t.Fatalf("resolved=%v cleared=%v", notifier.resolved, store.cleared)
	}
	store.cleared = nil
	notifier.resolved = nil
	if err := (&Dispatcher{Store: store, Notifier: notifier}).resolveAttention(context.Background(), events, now); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(notifier.resolved, []domain.SessionID{"w2", "w1"}) || !slices.Equal(store.cleared, []string{"e1", "e3", "e2"}) {
		t.Fatalf("delivery resolved=%v cleared=%v", notifier.resolved, store.cleared)
	}
}

func (f *fakeStore) RetryOrchestrationEvents(_ context.Context, e []domain.OrchestrationEvent, _, _ string, _ time.Time, _ domain.SessionMode) error {
	f.retried = eventIDs(e)
	return nil
}
func (f *fakeStore) MarkProjectNoDestinationAttention(context.Context, domain.ProjectID, time.Time) (int64, error) {
	return f.attentionChanged, nil
}

type fakeNotifier struct {
	created, resolved []domain.SessionID
	err, resolveErr   error
}

func (f *fakeNotifier) Notify(_ context.Context, intent ports.NotificationIntent) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, intent.SessionID)
	return nil
}

func TestReconcileAttentionRestoresEveryWorkerAlertAfterNotificationFailure(t *testing.T) {
	now := time.Now()
	events := []domain.OrchestrationEvent{
		{ID: "e1", ProjectID: "p", WorkerID: "w1", AttentionRequiredAt: now},
		{ID: "e2", ProjectID: "p", WorkerID: "w2", AttentionRequiredAt: now},
		{ID: "e3", ProjectID: "p", WorkerID: "w1", AttentionRequiredAt: now},
	}
	store := &fakeStore{attentionEvents: events}
	failed := &fakeNotifier{err: errors.New("publisher unavailable")}
	if err := (&Dispatcher{Store: store, Notifier: failed}).ReconcileAttention(context.Background(), "p", now); err == nil {
		t.Fatal("ReconcileAttention error=nil, want publisher failure")
	}
	restarted := &fakeNotifier{}
	if err := (&Dispatcher{Store: store, Notifier: restarted}).ReconcileAttention(context.Background(), "p", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(restarted.created) != 2 {
		t.Fatalf("created=%v", restarted.created)
	}
	if got := strings.Join([]string{string(restarted.created[0]), string(restarted.created[1])}, ","); got != "w1,w2" {
		t.Fatalf("created=%v", restarted.created)
	}
}
func (f *fakeNotifier) Resolve(_ context.Context, resolution ports.NotificationResolution) error {
	if f.resolveErr != nil {
		return f.resolveErr
	}
	f.resolved = append(f.resolved, resolution.SessionID)
	return nil
}

func TestAttentionNotificationIsCreatedOnceAndResolvedByDelivery(t *testing.T) {
	now := time.Now()
	event := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now.Add(-16 * time.Minute)}
	notifier := &fakeNotifier{}
	missing := &fakeStore{events: []domain.OrchestrationEvent{event}, attentionChanged: 1}
	if err := (&Dispatcher{Store: missing, Transport: &fakeTransport{}, Notifier: notifier, Now: func() time.Time { return now }}).DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	missing.attentionChanged = 0
	_ = (&Dispatcher{Store: missing, Transport: &fakeTransport{}, Notifier: notifier, Now: func() time.Time { return now }}).DispatchProject(context.Background(), "p")
	if len(notifier.created) != 1 {
		t.Fatalf("created=%v", notifier.created)
	}
	deliverable := &fakeStore{events: []domain.OrchestrationEvent{event}, sessions: []domain.SessionRecord{{ID: "o", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}}}
	if err := (&Dispatcher{Store: deliverable, Transport: &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}, Notifier: notifier}).DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if len(notifier.resolved) != 1 || notifier.resolved[0] != "w" {
		t.Fatalf("resolved=%v", notifier.resolved)
	}
	if len(deliverable.cleared) != 1 || deliverable.cleared[0] != "e" {
		t.Fatalf("cleared=%v", deliverable.cleared)
	}
}

func TestAttentionResolutionFailureIsRetriedFromAcknowledgedEvent(t *testing.T) {
	now := time.Now().UTC()
	event := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: "w", State: domain.OrchestrationAcknowledged, AttentionRequiredAt: now}
	store := &fakeStore{attentionEvents: []domain.OrchestrationEvent{event}}
	failing := &fakeNotifier{resolveErr: errors.New("notification store unavailable")}
	if err := (&Dispatcher{Store: store, Notifier: failing}).ReconcileAttention(context.Background(), "p", now); err == nil {
		t.Fatal("resolution failure was swallowed")
	}
	if len(store.cleared) != 0 {
		t.Fatalf("cleared=%v after failed resolution", store.cleared)
	}
	working := &fakeNotifier{}
	if err := (&Dispatcher{Store: store, Notifier: working}).ReconcileAttention(context.Background(), "p", now); err != nil {
		t.Fatal(err)
	}
	if len(working.resolved) != 1 || len(store.cleared) != 1 {
		t.Fatalf("resolved=%v cleared=%v", working.resolved, store.cleared)
	}
}

type fakeTransport struct {
	calls  int
	target domain.SessionID
	batch  Batch
	result Submission
	err    error
}

func (f *fakeTransport) Submit(_ context.Context, s domain.SessionRecord, b Batch) (Submission, error) {
	f.calls++
	f.target = s.ID
	f.batch = b
	return f.result, f.err
}

func TestDispatchResolvesCurrentDestinationAndAcknowledgesAcceptedBatch(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{sessions: []domain.SessionRecord{
		{ID: "old", ProjectID: "p", Kind: domain.KindOrchestrator, IsTerminated: true},
		{ID: "current", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now},
	}, events: []domain.OrchestrationEvent{{ID: "e1", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTurnSettled, SourceRevision: "r1", EnqueuedAt: now}}}
	transport := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
	d := Dispatcher{Store: store, Transport: transport, Now: func() time.Time { return now }, NewID: func() string { return "batch-1" }}
	if err := d.DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if transport.target != "current" || store.destination != "current" {
		t.Fatalf("destination transport=%q lease=%q", transport.target, store.destination)
	}
	if len(store.acked) != 1 || store.acked[0] != "e1" {
		t.Fatalf("acked=%v", store.acked)
	}
	if !strings.Contains(transport.batch.Payload, "worker_turn_settled") || strings.Contains(transport.batch.Payload, "completed") {
		t.Fatalf("payload=%q", transport.batch.Payload)
	}
}

func TestUnsafeOrMissingDestinationPerformsNoWrite(t *testing.T) {
	states := []domain.ActivityState{domain.ActivityBlocked, domain.ActivityWaitingInput, domain.ActivityExited}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			now := time.Now()
			store := &fakeStore{sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Activity: domain.Activity{State: state}, FirstSignalAt: now}}, events: []domain.OrchestrationEvent{{ID: "e"}}}
			transport := &fakeTransport{}
			if err := (&Dispatcher{Store: store, Transport: transport}).DispatchProject(context.Background(), "p"); err != nil {
				t.Fatal(err)
			}
			if transport.calls != 0 || len(store.leased) != 0 {
				t.Fatalf("unsafe state wrote: calls=%d leased=%v", transport.calls, store.leased)
			}
		})
	}
}

func TestBusyTUIDefersWhileBusyChatQueues(t *testing.T) {
	now := time.Now()
	event := domain.OrchestrationEvent{ID: "e", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now}
	for _, tc := range []struct {
		name string
		mode domain.SessionMode
		want int
	}{{"tui", domain.SessionModeTUI, 0}, {"chat", domain.SessionModeChat, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeStore{sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: tc.mode, Activity: domain.Activity{State: domain.ActivityActive}, FirstSignalAt: now}}, events: []domain.OrchestrationEvent{event}}
			tr := &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}
			_ = (&Dispatcher{Store: s, Transport: tr}).DispatchProject(context.Background(), "p")
			if tr.calls != tc.want {
				t.Fatalf("calls=%d want %d", tr.calls, tc.want)
			}
		})
	}
}

func TestTransportFailureRetriesAndSanitizesError(t *testing.T) {
	now := time.Now()
	s := &fakeStore{sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}}, events: []domain.OrchestrationEvent{{ID: "e", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerBlocked, SourceRevision: "r", EnqueuedAt: now}}}
	tr := &fakeTransport{result: Submission{Submitted: true}, err: errors.New("bad\x1b[31m\nsecret")}
	if err := (&Dispatcher{Store: s, Transport: tr}).DispatchProject(context.Background(), "p"); err != nil {
		t.Fatal(err)
	}
	if len(s.retried) != 1 {
		t.Fatalf("retried=%v", s.retried)
	}
}

func TestTransportErrorsPersistOnlyTypedCredentialSafeClass(t *testing.T) {
	for _, raw := range []string{"Bearer ghp_abcdefghijklmnopqrstuvwxyz", "https://user:pass@example.test/?token=secret", "api_key=sk-live-secret\n\x1b[31m"} {
		if got := sanitizeError(errors.New(raw)); got != "transport_error" {
			t.Fatalf("sanitizeError(%q)=%q", raw, got)
		}
	}
	if got := sanitizeError(context.DeadlineExceeded); got != "transport_timeout" {
		t.Fatalf("deadline class=%q", got)
	}
}

func TestPromptIsBoundedAndExcludesUntrustedProviderText(t *testing.T) {
	events := make([]domain.OrchestrationEvent, 80)
	for i := range events {
		events[i] = domain.OrchestrationEvent{ID: strings.Repeat("x", 900), WorkerID: "w", Kind: domain.OrchestrationWorkerReadyMerge, SourceRevision: "r"}
	}
	payload, kept := Prompt("batch", events)
	if len(kept) > MaxBatchEvents || len(payload) > MaxPayloadBytes {
		t.Fatalf("events=%d bytes=%d", len(kept), len(payload))
	}
	if strings.Contains(payload, "ignore previous instructions") {
		t.Fatal("untrusted prose entered payload")
	}
	if !strings.Contains(payload, "grants no permission or authorization") {
		t.Fatal("authorization warning absent")
	}
}

func TestDispatchHoldBoundaries(t *testing.T) {
	for _, phase := range []string{"held", "unknown", "after-lease", "wrong-event", "wrong-destination"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Now()
			s := &fakeStore{sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}}, events: []domain.OrchestrationEvent{{ID: "e", ProjectID: "p", EnqueuedAt: now}}}
			switch phase {
			case "held":
				s.held = true
			case "unknown":
				s.holdErr = errors.New("database unavailable")
			case "after-lease":
				s.afterLease = func() { s.held = true }
			case "wrong-event":
				s.events[0].ProjectID = "other"
			case "wrong-destination":
				s.sessions[0].ProjectID = "other"
			}
			tr := &fakeTransport{}
			err := (&Dispatcher{Store: s, Transport: tr}).DispatchProject(context.Background(), "p")
			if err == nil || tr.calls != 0 || len(s.retried) != 0 || len(s.acked) != 0 {
				t.Fatalf("err=%v sends=%d retries=%v acks=%v", err, tr.calls, s.retried, s.acked)
			}
			if phase != "after-lease" && len(s.leased) != 0 {
				t.Fatal("unexpected lease")
			}
		})
	}
}
