package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
)

func TestMigration153RecoveryPreservesUnknownLegacyTerminalIdentity(t *testing.T) {
	ctx := context.Background()
	dataDir := migrationFixture(t, 152)(t)
	db, err := sql.Open("sqlite", databaseURI(dataDir)+pragmas)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now().UTC()
	if _, err := db.Exec(`INSERT INTO projects(id,path,registered_at) VALUES ('p','/p',?)`, now); err != nil {
		t.Fatal(err)
	}
	for i, row := range []struct {
		id, runtime, controller string
		cleanup                 int64
	}{
		{id: "restored-legacy", cleanup: 1},
		{id: "runtime-legacy", runtime: "legacy-launch", cleanup: 1},
		{id: "controller-legacy", controller: "legacy-controller", cleanup: 1},
		{id: "first-terminal"},
	} {
		if _, err := db.Exec(`INSERT INTO sessions(id,project_id,num,harness,activity_last_at,created_at,updated_at,is_terminated,revision,cleanup_generation,runtime_launch_id,controller_generation)
			VALUES (?,'p',?,'fake',?,?,?,1,7,?,?,?)`, row.id, i+1, now, now, now, row.cleanup, row.runtime, row.controller); err != nil {
			t.Fatal(err)
		}
	}
	// This source belongs to an earlier episode, not the restored termination.
	if _, err := db.Exec(`INSERT INTO orchestration_source_states(project_id,worker_id,kind,source_id,active,generation,updated_at)
		VALUES ('p','restored-legacy','worker_terminated','terminal-revision:3',1,1,?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pr(url,session_id,pr_state,is_merged,updated_at) VALUES ('https://example.test/pr/1','restored-legacy','merged',1,?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Open applies the real upgrade, including migration 153, to persisted rows.
	s, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	legacy, found, err := s.GetSession(ctx, "restored-legacy")
	if err != nil || !found || legacy.TerminalRevision != -1 || legacy.CleanupGeneration != 1 || legacy.Metadata.RuntimeLaunchID != "" || legacy.Metadata.ControllerGeneration != "" {
		t.Fatalf("migrated legacy=%+v found=%v err=%v", legacy, found, err)
	}
	t.Logf("migrated terminal_revision=%d runtime=%q controller=%q", legacy.TerminalRevision, legacy.Metadata.RuntimeLaunchID, legacy.Metadata.ControllerGeneration)
	dispatcher := &orchestrationevents.Dispatcher{Store: s}
	for i := 0; i < 2; i++ {
		if err := orchestrationevents.Recover(ctx, s, dispatcher); err != nil {
			t.Fatalf("valid legacy row prevents daemon startup recovery: %v", err)
		}
		events, err := s.ListOrchestrationEvents(ctx, "p", 10)
		if err != nil || len(events) != 4 {
			t.Fatalf("recovery %d events=%+v err=%v", i, events, err)
		}
		terminalWorkers := make(map[domain.SessionID]bool)
		merged := 0
		for _, event := range events {
			switch event.Kind {
			case domain.OrchestrationWorkerTerminated:
				if event.WorkerID == legacy.ID {
					t.Fatal("unknown legacy episode fabricated a terminal event")
				}
				terminalWorkers[event.WorkerID] = true
			case domain.OrchestrationPRMerged:
				if event.WorkerID != legacy.ID {
					t.Fatalf("merged event worker=%q", event.WorkerID)
				}
				merged++
			default:
				t.Fatalf("unexpected event=%+v", event)
			}
		}
		if len(terminalWorkers) != 3 || !terminalWorkers["runtime-legacy"] || !terminalWorkers["controller-legacy"] || !terminalWorkers["first-terminal"] || merged != 1 {
			t.Fatalf("terminal workers=%v merged=%d", terminalWorkers, merged)
		}
	}
	db, err = sql.Open("sqlite", databaseURI(dataDir)+pragmas)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	var source string
	var sources, active, generation int
	if err := db.QueryRow(`SELECT COUNT(*),source_id,active,generation FROM orchestration_source_states WHERE worker_id='restored-legacy' AND kind='worker_terminated'`).Scan(&sources, &source, &active, &generation); err != nil || sources != 1 || source != "terminal-revision:3" || active != 1 || generation != 1 {
		t.Fatalf("historic source mutated: count=%d source=%q active=%d generation=%d err=%v", sources, source, active, generation, err)
	}
	current, found, err := s.GetSession(ctx, legacy.ID)
	if err != nil || !found || current.TerminalRevision != -1 || current.Revision != legacy.Revision {
		t.Fatalf("recovery changed legacy identity: current=%+v err=%v", current, err)
	}
	if err := lifecycle.New(s, nil).MarkTerminated(ctx, legacy.ID); err == nil {
		t.Fatal("lifecycle accepted the unknown historic terminal identity")
	}
	// A later real restore/termination creates a new database-owned identity.
	current.IsTerminated = false
	if err := s.UpdateSession(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.New(s, nil).MarkTerminated(ctx, legacy.ID); err != nil {
		t.Fatal(err)
	}
	if err := orchestrationevents.Recover(ctx, s, dispatcher); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListOrchestrationEvents(ctx, "p", 10)
	if err != nil || len(events) != 5 {
		t.Fatalf("new terminal episode events=%+v err=%v", events, err)
	}
}

func TestRecoveryRejectsMalformedTerminalRevision(t *testing.T) {
	fixture := migrationFixture(t, 153)
	for _, tc := range []struct {
		name              string
		terminal, cleanup int64
	}{
		{name: "invalid-negative", terminal: -2, cleanup: 1},
		{name: "unknown-without-restore", terminal: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dataDir := fixture(t)
			db, err := sql.Open("sqlite", databaseURI(dataDir)+pragmas)
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { _ = db.Close() })
			now := time.Now().UTC()
			if _, err := db.Exec(`INSERT INTO projects(id,path,registered_at) VALUES ('p','/p',?)`, now); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO sessions(id,project_id,num,harness,activity_last_at,created_at,updated_at,is_terminated,revision,terminal_revision,cleanup_generation)
				VALUES ('malformed','p',1,'fake',?,?,?,1,7,?,?)`, now, now, now, tc.terminal, tc.cleanup); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := Open(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			err = orchestrationevents.Recover(ctx, s, &orchestrationevents.Dispatcher{Store: s})
			if err == nil || !strings.Contains(err.Error(), "terminal revision unavailable") {
				t.Fatalf("malformed identity recovery error=%v", err)
			}
			if events, err := s.ListOrchestrationEvents(ctx, "p", 10); err != nil || len(events) != 0 {
				t.Fatalf("malformed identity created events=%+v err=%v", events, err)
			}
		})
	}
}
