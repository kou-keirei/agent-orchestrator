package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func TestMigration0150DownPreservesLatestPromptCDC(t *testing.T) {
	db := openTestDB(t)
	upTo(t, db, 150)
	downTo(t, db, 149)

	var triggerSQL string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'sessions_cdc_update'`).Scan(&triggerSQL); err != nil {
		t.Fatalf("read rolled-back sessions_cdc_update: %v", err)
	}
	for _, predicate := range []string{
		"OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at",
		"(OLD.pinned_at IS NULL AND NEW.pinned_at IS NOT NULL)",
		"(OLD.pinned_at IS NOT NULL AND NEW.pinned_at IS NULL)",
	} {
		if !strings.Contains(triggerSQL, predicate) {
			t.Fatalf("rolled-back trigger missing preserved CDC predicate %q: %s", predicate, triggerSQL)
		}
	}
}

func TestMigration0150BurnedVersionRepairsSessionHidden(t *testing.T) {
	db := openTestDB(t)
	upTo(t, db, 149)
	if _, err := db.Exec(`INSERT INTO goose_db_version (version_id, is_applied) VALUES (150, 1)`); err != nil {
		t.Fatalf("seed burned migration 150: %v", err)
	}

	if err := migrate(db); err != nil {
		t.Fatalf("repair burned migration 150: %v", err)
	}
	var hiddenColumns int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name = 'hidden_at'`).Scan(&hiddenColumns); err != nil {
		t.Fatalf("inspect repaired hidden_at: %v", err)
	}
	if hiddenColumns != 1 {
		t.Fatalf("sessions.hidden_at count = %d, want 1", hiddenColumns)
	}
	var triggerSQL string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'sessions_cdc_update'`).Scan(&triggerSQL); err != nil {
		t.Fatalf("read repaired sessions_cdc_update: %v", err)
	}
	for _, fragment := range []string{
		"OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at",
		"OR OLD.hidden_at IS NOT NEW.hidden_at",
		"'hiddenAt', NEW.hidden_at",
	} {
		if !strings.Contains(triggerSQL, fragment) {
			t.Fatalf("repaired trigger missing %q: %s", fragment, triggerSQL)
		}
	}

	store := sqlitestore.NewStore(db, db)
	if err := store.UpsertProject(context.Background(), domain.ProjectRecord{
		ID: "mer", Path: "/tmp/mer", RegisteredAt: time.Now().UTC().Truncate(time.Second),
	}); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	record, err := store.CreateSession(context.Background(), domain.SessionRecord{
		ProjectID: "mer",
		Kind:      domain.KindWorker,
		Harness:   domain.HarnessClaudeCode,
		Activity:  domain.Activity{State: domain.ActivityActive, LastActivityAt: now},
		Metadata:  domain.SessionMetadata{Branch: "feat/hidden", WorkspacePath: "/tmp/hidden"},
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("create session on repaired schema: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM change_log WHERE session_id = ?`, record.ID); err != nil {
		t.Fatalf("clear create CDC: %v", err)
	}
	hiddenAt := now.Add(time.Minute)
	if ok, err := store.SetSessionHidden(context.Background(), record.ID, &hiddenAt); err != nil || !ok {
		t.Fatalf("hide repaired session: ok=%v err=%v", ok, err)
	}
	var eventType, payload string
	if err := db.QueryRow(`
SELECT event_type, payload FROM change_log
WHERE session_id = ? ORDER BY rowid DESC LIMIT 1`, record.ID).Scan(&eventType, &payload); err != nil {
		t.Fatalf("read hidden CDC: %v", err)
	}
	if eventType != "session_updated" || !strings.Contains(payload, `"hiddenAt"`) {
		t.Fatalf("hidden CDC = (%q, %q), want session_updated with hiddenAt", eventType, payload)
	}

	if err := migrate(db); err != nil {
		t.Fatalf("repeat migrate after burned repair: %v", err)
	}

	healthy := openTestDB(t)
	upTo(t, healthy, 150)
	var beforeHealthyTrigger string
	if err := healthy.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'sessions_cdc_update'`).Scan(&beforeHealthyTrigger); err != nil {
		t.Fatalf("read healthy sessions_cdc_update: %v", err)
	}
	if err := migrate(healthy); err != nil {
		t.Fatalf("migrate healthy database: %v", err)
	}
	var afterHealthyTrigger string
	if err := healthy.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'sessions_cdc_update'`).Scan(&afterHealthyTrigger); err != nil {
		t.Fatalf("read healthy sessions_cdc_update after migrate: %v", err)
	}
	if afterHealthyTrigger != beforeHealthyTrigger {
		t.Fatalf("healthy sessions_cdc_update changed during reconcile")
	}

	for _, tc := range []struct {
		name       string
		createStmt string
	}{
		{name: "missing", createStmt: ""},
		{name: "malformed", createStmt: `CREATE TRIGGER sessions_cdc_update AFTER UPDATE ON sessions BEGIN SELECT 1; END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := healthy.Exec(`DROP TRIGGER IF EXISTS sessions_cdc_update`); err != nil {
				t.Fatalf("drop stale healthy sessions_cdc_update: %v", err)
			}
			if tc.createStmt != "" {
				if _, err := healthy.Exec(tc.createStmt); err != nil {
					t.Fatalf("create malformed sessions_cdc_update: %v", err)
				}
			}
			if err := migrate(healthy); err != nil {
				t.Fatalf("repair %s session hidden CDC trigger: %v", tc.name, err)
			}
			var repairedTrigger string
			if err := healthy.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'sessions_cdc_update'`).Scan(&repairedTrigger); err != nil {
				t.Fatalf("read repaired %s sessions_cdc_update: %v", tc.name, err)
			}
			for _, fragment := range []string{
				"OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at",
				"OR OLD.hidden_at IS NOT NEW.hidden_at",
				"'hiddenAt', NEW.hidden_at",
			} {
				if !strings.Contains(repairedTrigger, fragment) {
					t.Fatalf("repaired %s trigger lacks %q: %s", tc.name, fragment, repairedTrigger)
				}
			}
		})
	}
}
