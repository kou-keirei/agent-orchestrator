package chat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
)

// These are source-contract tests, not installed cold-start/wake E2E tests.
// New models passive service construction; the injected restorer models the
// manager boundary by publishing an unstarted controller backed by a fake
// conversation. No provider driver, event loop, daemon, runtime, SQLite, native
// user session, credentials, or network is involved. Manager recovery and actual
// provider restoration remain separate qualification requirements.
type coldDemandStore struct {
	// Unimplemented operations fail loudly if this bounded path expands.
	Store
	records       map[domain.SessionID]domain.SessionRecord
	conversations map[domain.SessionID]domain.ConversationRecord
	appends       []domain.SessionID
	bindings      int
}

func (f *coldDemandStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	rec, ok := f.records[id]
	return rec, ok, nil
}

func (f *coldDemandStore) ConversationForSession(_ context.Context, id domain.SessionID) (domain.ConversationRecord, error) {
	rec, ok := f.conversations[id]
	if !ok {
		return domain.ConversationRecord{}, domain.ErrNoConversation
	}
	return rec, nil
}

func (f *coldDemandStore) HasUnsettledConversationTurns(context.Context, string) (bool, error) {
	return false, nil
}

func (f *coldDemandStore) HasPendingConversationInteractions(context.Context, string) (bool, error) {
	return false, nil
}

func (f *coldDemandStore) LoadConversationSnapshot(_ context.Context, id string) (ConversationRows, error) {
	for _, rec := range f.conversations {
		if rec.ID == id {
			return ConversationRows{Conversation: rec, Messages: []domain.ConversationMessage{{
				ID: "history-" + id, Text: "retained history",
			}}}, nil
		}
	}
	return ConversationRows{}, domain.ErrNoConversation
}

func (f *coldDemandStore) LoadConversationSnapshotPage(ctx context.Context, id string, _, _ int64) (ConversationRows, error) {
	return f.LoadConversationSnapshot(ctx, id)
}

func (f *coldDemandStore) AppendUserMessage(_ context.Context, conversationID string, id domain.SessionID, _ string, _ domain.ConversationMessage, _ string, _ time.Time) (bool, error) {
	if f.conversations[id].ID != conversationID || f.conversations[id].SessionID != id {
		return false, domain.ErrNoConversation
	}
	f.appends = append(f.appends, id)
	return true, nil
}

func (f *coldDemandStore) BindTurnToProvider(context.Context, string, string, time.Time) error {
	f.bindings++
	return nil
}

// Keep session-list fakes separate: session.Store and chat.Store own different
// contracts, and no persistence or controller behavior is added to either.
type coldDemandListStore struct {
	sessionsvc.Store
	chat *coldDemandStore
}

func (f coldDemandListStore) ListSessions(_ context.Context, project domain.ProjectID) ([]domain.SessionRecord, error) {
	var out []domain.SessionRecord
	for _, rec := range f.chat.records {
		if rec.ProjectID == project {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (coldDemandListStore) ListActiveAgentSwitches(context.Context) ([]domain.AgentSwitch, error) {
	return nil, nil
}

func (coldDemandListStore) ListPRFactsForSessions(context.Context, []domain.SessionID) (map[domain.SessionID][]domain.PRFacts, error) {
	return nil, nil
}

func (coldDemandListStore) ListCurrentHeadReviewRunsForSessions(context.Context, []domain.SessionID) (map[domain.SessionID][]domain.CurrentHeadReviewRun, error) {
	return nil, nil
}

type coldDemandRegistry struct {
	ports.ChatDriverRegistry
	calls int
}

func (f *coldDemandRegistry) Driver(domain.AgentHarness) (ports.ChatDriver, error) {
	f.calls++
	return nil, errors.New("provider driver must not be opened by this fixture")
}

type coldDemandConversation struct {
	ports.ChatConversation
	id    string
	caps  ports.ChatCapabilities
	sends []ports.ChatUserMessage
}

func (f *coldDemandConversation) ProviderConversationID() string {
	return f.id
}
func (f *coldDemandConversation) Capabilities() ports.ChatCapabilities {
	return f.caps
}
func (f *coldDemandConversation) SendTurn(_ context.Context, msg ports.ChatUserMessage) (ports.ChatTurnRef, error) {
	f.sends = append(f.sends, msg)
	return ports.ChatTurnRef{ProviderTurnID: "fake-turn"}, nil
}

func newColdDemandFixture(t *testing.T) (*Service, *coldDemandStore, *coldDemandRegistry) {
	t.Helper()
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st := &coldDemandStore{
		records:       make(map[domain.SessionID]domain.SessionRecord),
		conversations: make(map[domain.SessionID]domain.ConversationRecord),
	}
	for _, id := range []domain.SessionID{"cold-target", "cold-other", "cold-hidden"} {
		rec := domain.SessionRecord{
			ID:        id,
			ProjectID: "cold-project",
			Kind:      domain.KindWorker,
			Harness:   domain.HarnessCodex,
			Mode:      domain.SessionModeChat,
			Activity:  domain.Activity{State: domain.ActivityIdle},
			CreatedAt: now,
			UpdatedAt: now,
		}
		rec.Metadata.WorkspacePath = t.TempDir()
		rec.Metadata.ProviderConversationID = "provider-" + string(id)
		if id == "cold-hidden" {
			rec.HiddenAt = &now
		}
		st.records[id] = rec
		st.conversations[id] = domain.ConversationRecord{ID: "conversation-" + string(id), SessionID: id}
	}
	registry := &coldDemandRegistry{}
	seq := 0
	svc := New(Options{
		Store:      st,
		Sessions:   st,
		Reader:     st,
		PageReader: st,
		Drivers:    registry,
		Now:        func() time.Time { return now },
		NewID: func() string {
			seq++
			return fmt.Sprintf("cold-id-%d", seq)
		},
	})
	return svc, st, registry
}

func TestColdWorkerDemandWakePassiveReads(t *testing.T) {
	ctx := context.Background()
	svc, st, registry := newColdDemandFixture(t)
	restores := 0
	svc.SetControllerRestorer(func(context.Context, domain.SessionID) error {
		restores++
		return errors.New("passive reads must not restore")
	})
	list := sessionsvc.NewWithDeps(sessionsvc.Deps{Store: coldDemandListStore{chat: st}, Clock: svc.now})
	for _, includeHidden := range []bool{false, true, false, true} {
		rows, err := list.List(ctx, sessionsvc.ListFilter{ProjectID: "cold-project", IncludeHidden: includeHidden})
		want := 2
		if includeHidden {
			want = 3
		}
		if err != nil || len(rows) != want {
			t.Fatalf("list IncludeHidden=%v: rows=%d err=%v", includeHidden, len(rows), err)
		}
		for _, row := range rows {
			if !reflect.DeepEqual(row.SessionRecord, st.records[row.ID]) {
				t.Fatalf("list changed worker identity: %+v", row.SessionRecord)
			}
		}
		for id, original := range st.records {
			before := st.conversations[id]
			for _, read := range []func() (Snapshot, error){
				func() (Snapshot, error) { return svc.Snapshot(ctx, id) },
				func() (Snapshot, error) { return svc.SnapshotPage(ctx, id, 0, 20) },
			} {
				snapshot, err := read()
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.SessionID != id || snapshot.Controller != ports.ChatControllerCold ||
					snapshot.Capabilities != nil || !reflect.DeepEqual(snapshot.Conversation, before) ||
					len(snapshot.Messages) != 1 || snapshot.Messages[0].Text != "retained history" {
					t.Fatalf("passive read changed cold identity/history: %+v", snapshot)
				}
			}
			if !reflect.DeepEqual(st.records[id], original) || !reflect.DeepEqual(st.conversations[id], before) {
				t.Fatalf("passive read mutated durable identity for %s", id)
			}
		}
	}
	if restores != 0 || registry.calls != 0 || len(svc.controllers) != 0 || len(st.appends) != 0 || st.bindings != 0 {
		t.Fatalf("passive read caused work: restores=%d drivers=%d controllers=%d appends=%v bindings=%d",
			restores, registry.calls, len(svc.controllers), st.appends, st.bindings)
	}
}

func TestColdWorkerDemandWakeAddressedSend(t *testing.T) {
	for _, scenario := range []string{"authorized target", "stale owner", "unknown capabilities"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			svc, st, registry := newColdDemandFixture(t)
			var restored []domain.SessionID
			provider := &coldDemandConversation{id: st.records["cold-target"].Metadata.ProviderConversationID}
			var staleOwner error
			if scenario == "stale owner" {
				conversation := st.conversations["cold-target"]
				conversation.SessionID = "cold-other"
				st.conversations["cold-target"] = conversation
			}
			svc.SetControllerRestorer(func(_ context.Context, id domain.SessionID) error {
				restored = append(restored, id)
				if scenario == "stale owner" {
					// Exercise the existing continuity-owner predicate and propagate
					// its refusal through the injected manager boundary. This does
					// not exercise the manager's own restore implementation.
					needs, err := svc.ChatNeedsController(ctx, id)
					if !needs || err == nil {
						t.Fatal("stale continuity owner was accepted")
					}
					staleOwner = err
					return staleOwner
				}
				floor := ports.PermissionModeAuto
				if scenario == "unknown capabilities" {
					// Nil negotiated capabilities cannot prove preventive read-only.
					floor = ports.PermissionModeReadOnly
				}
				controller := newController(id, st.conversations[id], "fake-generation", domain.HarnessCodex,
					floor, provider, st, nil, slog.New(slog.DiscardHandler), svc.newID, svc.now, nil, nil)
				svc.mu.Lock()
				svc.controllers[id] = controller
				svc.mu.Unlock()
				return nil
			})
			msg := ports.ChatUserMessage{Text: "addressed instruction", ClientMessageID: "cold-send", Origin: domain.MessageOriginHuman}
			turn, err := svc.Send(ctx, "cold-target", msg)
			switch scenario {
			case "authorized target":
				if err != nil || turn.ID == "" || turn.HandledBySessionID != "cold-target" ||
					turn.ConversationID != st.conversations["cold-target"].ID ||
					len(provider.sends) != 1 || provider.sends[0].Text != msg.Text ||
					provider.sends[0].ClientMessageID != msg.ClientMessageID ||
					!reflect.DeepEqual(st.appends, []domain.SessionID{"cold-target"}) || st.bindings != 1 {
					t.Fatalf("addressed dispatch: turn=%+v err=%v sends=%+v appends=%v bindings=%d",
						turn, err, provider.sends, st.appends, st.bindings)
				}
			case "stale owner":
				if staleOwner == nil || !errors.Is(err, staleOwner) || !errors.Is(err, ports.ErrChatControllerRestore) || len(svc.controllers) != 0 {
					t.Fatalf("stale restore refusal lost: %v", err)
				}
			case "unknown capabilities":
				if !errors.Is(err, ports.ErrChatPermissionModeUnsupported) {
					t.Fatalf("unknown read-only capability was not refused: %v", err)
				}
			}
			if scenario != "authorized target" && (len(provider.sends) != 0 || len(st.appends) != 0 || st.bindings != 0 || turn.ID != "") {
				t.Fatalf("refused input reached delivery: sends=%d appends=%v bindings=%d turn=%+v",
					len(provider.sends), st.appends, st.bindings, turn)
			}
			if !reflect.DeepEqual(restored, []domain.SessionID{"cold-target"}) || registry.calls != 0 {
				t.Fatalf("restore escaped addressed target: restores=%v drivers=%d", restored, registry.calls)
			}
			for _, id := range []domain.SessionID{"cold-other", "cold-hidden"} {
				snapshot, readErr := svc.Snapshot(ctx, id)
				if readErr != nil || snapshot.Controller != ports.ChatControllerCold || snapshot.Capabilities != nil {
					t.Fatalf("unaddressed worker %s changed: %+v err=%v", id, snapshot, readErr)
				}
			}
			if len(restored) != 1 || registry.calls != 0 {
				t.Fatalf("post-send history triggered extra work: restores=%v drivers=%d", restored, registry.calls)
			}
		})
	}
}
