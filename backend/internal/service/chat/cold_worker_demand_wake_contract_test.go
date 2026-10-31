package chat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
	sessionmanager "github.com/aoagents/agent-orchestrator/backend/internal/session_manager"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

// These are source-contract tests, not installed cold-start/wake E2E tests.
// New models passive service construction; the injected restorer models the
// manager boundary by publishing an unstarted controller backed by a fake
// conversation. The additional durable-delivery and manager tests use only
// t.TempDir SQLite and fake provider boundaries. No real provider, daemon,
// runtime, native user session, provider credentials, or network is involved.
// Installed recovery and actual provider restoration remain unqualified.
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

// Use production persistence for duplicate delivery: a fake returning false on
// its second call would not establish that the client ID is durably deduplicated.
func coldDemandDurableStore(t *testing.T, rec domain.SessionRecord) (*sqlite.Store, domain.SessionRecord) {
	t.Helper()
	ctx := context.Background()
	st := sqlitetest.MustOpenAt(t, t.TempDir())
	if err := st.UpsertProject(ctx, domain.ProjectRecord{
		ID: string(rec.ProjectID), Path: rec.Metadata.WorkspacePath, RegisteredAt: rec.CreatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	rec.Metadata.Branch = "ao/cold-contract"
	rec.Metadata.ControllerGeneration = "saved-generation"
	rec.Metadata.Permissions = ports.PermissionModeAuto
	created, err := st.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	rec = created // SQLite owns session numbering; retain its assigned identity.
	if _, err := st.CreateConversation(ctx, "durable-cold-conversation", domain.ConversationScopeSession,
		rec.ProjectID, rec.ID, rec.CreatedAt); err != nil {
		t.Fatal(err)
	}
	current, found, err := st.GetSession(ctx, rec.ID)
	if err != nil || !found {
		t.Fatalf("read durable fixture: found=%v err=%v", found, err)
	}
	return st, current
}

func TestColdWorkerDemandWakeLostAcknowledgementDuplicate(t *testing.T) {
	ctx := context.Background()
	svc, fake, registry := newColdDemandFixture(t)
	st, rec := coldDemandDurableStore(t, fake.records["cold-target"])
	svc.store, svc.sessions = st, st
	conversation, err := st.ConversationForSession(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	provider := &coldDemandConversation{id: rec.Metadata.ProviderConversationID}
	restores := 0
	svc.SetControllerRestorer(func(_ context.Context, id domain.SessionID) error {
		restores++
		if id != rec.ID {
			t.Fatalf("restored foreign session %s", id)
		}
		controller := newController(id, conversation, rec.Metadata.ControllerGeneration, rec.Harness,
			rec.Metadata.Permissions, provider, st, nil, slog.New(slog.DiscardHandler), svc.newID, svc.now, nil, nil)
		svc.mu.Lock()
		svc.controllers[id] = controller
		svc.mu.Unlock()
		return nil
	})
	msg := ports.ChatUserMessage{Text: "retry after lost acknowledgement", ClientMessageID: "cold-exact-client-id", Origin: domain.MessageOriginHuman}
	first, err := svc.Send(ctx, rec.ID, msg)
	if err != nil || first.ID == "" || first.HandledBySessionID != rec.ID {
		t.Fatalf("first send: turn=%+v err=%v", first, err)
	}
	// Simulate losing the response, then retry the identical request through the
	// real Send method while its first provider turn remains pending. Removing
	// the controller's !created guard would falsely return a new queued turn.
	duplicate, err := svc.Send(ctx, rec.ID, msg)
	if err != nil || !reflect.DeepEqual(duplicate, domain.ConversationTurn{}) {
		t.Fatalf("duplicate falsely acknowledged creation: turn=%+v err=%v", duplicate, err)
	}
	rows, err := st.LoadConversationSnapshot(ctx, conversation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Turns) != 1 || rows.Turns[0].ID != first.ID || rows.Turns[0].ProviderTurnID != "fake-turn" ||
		len(rows.Messages) != 1 || rows.Messages[0].ClientMessageID != msg.ClientMessageID || rows.Messages[0].Text != msg.Text {
		t.Fatalf("duplicate changed durable delivery: turns=%+v messages=%+v", rows.Turns, rows.Messages)
	}
	if restores != 1 || registry.calls != 0 || len(provider.sends) != 1 ||
		provider.sends[0].ClientMessageID != msg.ClientMessageID || provider.sends[0].Text != msg.Text {
		t.Fatalf("duplicate caused work: restores=%d drivers=%d sends=%+v", restores, registry.calls, provider.sends)
	}
}

type coldDemandNoAgents struct{}

func (coldDemandNoAgents) Agent(domain.AgentHarness) (ports.Agent, bool) { return nil, false }

// This is a synthetic browser credential, never a provider or installed AO
// credential. It enables the existing production owner-CAS before provider birth.
type coldDemandCapabilityIssuer struct{}

func (coldDemandCapabilityIssuer) Issue(domain.SessionID) (string, string, error) {
	return "fixture-browser-token", "fixture-browser-verifier", nil
}

type coldDemandOwnerStore struct {
	*sqlite.Store
	foreign bool
}

func (f coldDemandOwnerStore) ConversationForSession(ctx context.Context, id domain.SessionID) (domain.ConversationRecord, error) {
	rec, err := f.Store.ConversationForSession(ctx, id)
	if f.foreign && err == nil {
		rec.SessionID = "cold-foreign-session"
	}
	return rec, err
}

type coldDemandManagedLauncher struct {
	*Service
	beforeStart func(ports.ChatControllerStart)
	starts      int
}

func (f *coldDemandManagedLauncher) StartChat(ctx context.Context, cfg ports.ChatControllerStart) (ports.ChatControllerStarted, error) {
	f.starts++
	f.beforeStart(cfg)
	return f.Service.StartChat(ctx, cfg)
}

type coldDemandBirthDriver struct {
	ports.ChatDriver
	resumes []ports.ChatResumeConfig
	births  int
}

func (f *coldDemandBirthDriver) Resume(ctx context.Context, cfg ports.ChatResumeConfig) (ports.ChatConversation, error) {
	f.resumes = append(f.resumes, cfg)
	if cfg.PrepareEnv == nil {
		return nil, errors.New("fixture requires real manager launch preparation")
	}
	// Drivers execute this existing callback before creating a provider. The
	// callback, manager and SQLite perform all ownership checks, not this fake.
	if _, err := cfg.PrepareEnv(ctx); err != nil {
		return nil, err
	}
	f.births++
	return nil, errors.New("unexpected provider birth")
}

func (f *coldDemandBirthDriver) Start(context.Context, ports.ChatStartConfig) (ports.ChatConversation, error) {
	f.births++
	return nil, errors.New("unexpected fresh provider birth")
}

type coldDemandBirthRegistry struct{ driver *coldDemandBirthDriver }

func (f coldDemandBirthRegistry) Driver(domain.AgentHarness) (ports.ChatDriver, error) {
	return f.driver, nil
}
func (coldDemandBirthRegistry) SupportsChat(domain.AgentHarness) bool { return true }

func TestColdWorkerDemandWakeManagerOwnerRefusal(t *testing.T) {
	for _, scenario := range []string{"foreign continuity", "stale generation", "foreign native conversation"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			svc, fake, _ := newColdDemandFixture(t)
			st, original := coldDemandDurableStore(t, fake.records["cold-target"])
			observed := coldDemandOwnerStore{Store: st, foreign: scenario == "foreign continuity"}
			svc.store, svc.sessions = observed, st
			driver := &coldDemandBirthDriver{}
			svc.drivers = coldDemandBirthRegistry{driver: driver}
			launcher := &coldDemandManagedLauncher{Service: svc}
			launcher.beforeStart = func(cfg ports.ChatControllerStart) {
				if cfg.SessionID != original.ID || cfg.ProviderConversationID != original.Metadata.ProviderConversationID ||
					cfg.ExpectedControllerOwner != original.ControllerOwner() {
					t.Fatalf("manager selected foreign/stale restore identity: %+v", cfg.ExpectedControllerOwner)
				}
				// Change the durable owner after the manager has selected its
				// snapshot, before the real service invokes launch preparation.
				current := original
				if scenario == "stale generation" {
					current.Metadata.ControllerGeneration = "replacement-generation"
				} else if scenario == "foreign native conversation" {
					current.Metadata.ProviderConversationID = "foreign-native-conversation"
				}
				if err := st.UpdateSession(ctx, current); err != nil {
					t.Fatal(err)
				}
			}
			dataDir := t.TempDir()
			manager := sessionmanager.New(sessionmanager.Deps{
				Store:               st,
				Chat:                launcher,
				Agents:              coldDemandNoAgents{},
				DataDir:             dataDir,
				RunFilePath:         filepath.Join(dataDir, "fixture-running.json"),
				BrowserCapabilities: coldDemandCapabilityIssuer{},
				Executable:          func() (string, error) { return filepath.Join(dataDir, "ao.exe"), nil },
				Logger:              slog.New(slog.DiscardHandler),
			})
			svc.SetControllerRestorer(manager.EnsureChatController)
			turn, err := svc.Send(ctx, original.ID, ports.ChatUserMessage{
				Text: "must not reach a provider", ClientMessageID: "owner-refusal", Origin: domain.MessageOriginHuman,
			})
			if !errors.Is(err, ports.ErrChatControllerRestore) || !reflect.DeepEqual(turn, domain.ConversationTurn{}) {
				t.Fatalf("manager did not refuse stale/foreign ownership: turn=%+v err=%v", turn, err)
			}
			if scenario == "foreign continuity" {
				if !strings.Contains(err.Error(), "invalid Chat continuity owner") || launcher.starts != 0 || len(driver.resumes) != 0 {
					t.Fatalf("manager bypassed continuity refusal: starts=%d resumes=%d err=%v", launcher.starts, len(driver.resumes), err)
				}
			} else {
				if !strings.Contains(err.Error(), "session controller ownership changed before browser capability rotation") ||
					launcher.starts != 1 || len(driver.resumes) != 1 {
					t.Fatalf("real owner-CAS was not exercised: starts=%d resumes=%d err=%v", launcher.starts, len(driver.resumes), err)
				}
				resume := driver.resumes[0]
				if resume.SessionID != original.ID || resume.ProviderConversationID != original.Metadata.ProviderConversationID {
					t.Fatalf("wrong native resume identity: session=%s provider=%s", resume.SessionID, resume.ProviderConversationID)
				}
			}
			rows, readErr := st.LoadConversationSnapshot(ctx, "durable-cold-conversation")
			if readErr != nil || len(rows.Turns) != 0 || len(rows.Messages) != 0 || driver.births != 0 || svc.HasLiveChatController(original.ID) {
				t.Fatalf("unauthorized provider/work: births=%d turns=%d messages=%d live=%v err=%v",
					driver.births, len(rows.Turns), len(rows.Messages), svc.HasLiveChatController(original.ID), readErr)
			}
			current, found, readErr := st.GetSession(ctx, original.ID)
			if readErr != nil || !found {
				t.Fatalf("read refused owner: found=%v err=%v", found, readErr)
			}
			expected := original.ControllerOwner()
			if scenario == "stale generation" {
				expected.ControllerGeneration = "replacement-generation"
			} else if scenario == "foreign native conversation" {
				expected.ProviderConversationID = "foreign-native-conversation"
			}
			if current.ControllerOwner() != expected || current.Metadata.BrowserCapabilityVerifier != original.Metadata.BrowserCapabilityVerifier {
				t.Fatalf("refused restore changed durable ownership: got=%+v want=%+v", current.ControllerOwner(), expected)
			}
		})
	}
}
