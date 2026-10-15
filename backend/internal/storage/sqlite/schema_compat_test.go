package sqlite

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// This is a small reproduction of the observed desktop lineage: the canonical
// schema through 149, foreign provisioning data, and foreign journal claims
// through 169. No production database or user data is needed by the tests.
func legacy169(t *testing.T) *sql.DB {
	t.Helper()
	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	upTo(t, db, 149)
	for _, statement := range []string{
		`ALTER TABLE sessions ADD COLUMN provision_state TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE foreign_history (id TEXT PRIMARY KEY, content TEXT NOT NULL)`,
		`INSERT INTO foreign_history VALUES ('native-history','keep verbatim')`,
		`INSERT INTO projects(id,path,registered_at) VALUES ('legacy','/legacy',CURRENT_TIMESTAMP)`,
		`INSERT INTO sessions(id,project_id,num,harness,activity_last_at,created_at,updated_at,is_terminated,revision,prompt) VALUES ('legacy-1','legacy',1,'fake',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1,7,'preserve prompt')`,
		`INSERT INTO notifications(id,session_id,type,title,created_at) VALUES ('notice','legacy-1','needs_input','preserve notification',CURRENT_TIMESTAMP)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var trigger string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE name='sessions_cdc_update'`).Scan(&trigger); err != nil {
		t.Fatal(err)
	}
	trigger = strings.Replace(trigger, "OR OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at", "OR OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at OR OLD.provision_state <> NEW.provision_state", 1)
	trigger = strings.Replace(trigger, "'autoReviewEnabled', NEW.auto_review_enabled", "'autoReviewEnabled', NEW.auto_review_enabled, 'provisionState', NEW.provision_state", 1)
	if _, err := db.Exec(`DROP TRIGGER sessions_cdc_update; ` + trigger); err != nil {
		t.Fatal(err)
	}
	for v := 150; v <= 169; v++ {
		if _, err := db.Exec(`INSERT INTO goose_db_version(version_id,is_applied) VALUES (?,1)`, v); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestLegacy169Compatibility(t *testing.T) {
	db := legacy169(t)
	var journalBefore string
	if err := db.QueryRow(`SELECT group_concat(id||':'||version_id||':'||is_applied||':'||tstamp,'|') FROM goose_db_version`).Scan(&journalBefore); err != nil {
		t.Fatal(err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	for table, columns := range map[string][]string{
		"sessions":                    {"hidden_at", "terminal_revision", "provision_state"},
		"review_run":                  {"target_binding_json", "reviewer_provenance_json", "launch_id", "provider_native_session_id", "provider_native_first_turn_id", "provider_native_completion_id", "provider_native_observed_at"},
		"orchestration_events":        {"id", "state"},
		"orchestration_source_states": {"source_id", "generation"},
		"reviewer_cleanup_obligation": {"handle_id", "status"},
	} {
		for _, column := range columns {
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, table, column).Scan(&n); err != nil || n != 1 {
				t.Fatalf("%s.%s missing: %v", table, column, err)
			}
		}
	}
	for query, want := range map[string]string{
		`SELECT content FROM foreign_history WHERE id='native-history'`: "keep verbatim",
		`SELECT prompt FROM sessions WHERE id='legacy-1'`:               "preserve prompt",
		`SELECT title FROM notifications WHERE id='notice'`:             "preserve notification",
	} {
		var got string
		if err := db.QueryRow(query).Scan(&got); err != nil || got != want {
			t.Fatalf("retention: %q, %v", got, err)
		}
	}
	var terminal, revision int
	if err := db.QueryRow(`SELECT terminal_revision,revision FROM sessions WHERE id='legacy-1'`).Scan(&terminal, &revision); err != nil || terminal != 7 || revision != 7 {
		t.Fatalf("terminal=%d revision=%d err=%v", terminal, revision, err)
	}
	if _, err := db.Exec(`INSERT INTO notifications(id,session_id,type,title,created_at) VALUES ('attention','legacy-1','orchestration_attention','new type',CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE sessions SET provision_state='ready' WHERE id='legacy-1'`); err != nil {
		t.Fatal(err)
	}
	var provisioningEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM change_log WHERE json_extract(payload,'$.provisionState')='ready'`).Scan(&provisioningEvents); err != nil || provisioningEvents != 1 {
		t.Fatalf("provisioning CDC=%d err=%v", provisioningEvents, err)
	}
	if err := migrate(db); err != nil {
		t.Fatalf("repeat startup: %v", err)
	}
	var journalAfter string
	if err := db.QueryRow(`SELECT group_concat(id||':'||version_id||':'||is_applied||':'||tstamp,'|') FROM goose_db_version`).Scan(&journalAfter); err != nil || journalAfter != journalBefore {
		t.Fatalf("journal changed: %v", err)
	}
	if rows, err := db.Query(`PRAGMA foreign_key_check`); err != nil {
		t.Fatal(err)
	} else {
		defer rows.Close()
		if rows.Next() {
			t.Fatal("foreign key violation")
		}
	}
}

func TestLegacy169CompatibilityRejectsAmbiguityBeforeWrites(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"foreign higher version", `INSERT INTO goose_db_version(version_id,is_applied) VALUES (170,1)`},
		{"out of order journal", `DELETE FROM goose_db_version WHERE version_id=153`},
		{"partial review evidence", `ALTER TABLE review_run ADD COLUMN target_binding_json TEXT NOT NULL DEFAULT '{}'`},
		{"foreign table collision", `CREATE TABLE orchestration_events(id TEXT)`},
		{"foreign notification column", `ALTER TABLE notifications ADD COLUMN foreign_data TEXT`},
		{"foreign notification index", `CREATE INDEX foreign_notification_index ON notifications(title)`},
		{"foreign hidden type", `ALTER TABLE sessions ADD COLUMN hidden_at INTEGER`},
		{"terminal column without trigger", `ALTER TABLE sessions ADD COLUMN terminal_revision INTEGER NOT NULL DEFAULT 0`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := legacy169(t)
			if _, err := db.Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			var before string
			if err := db.QueryRow(`SELECT group_concat(sql,'|') FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			if err := migrate(db); err == nil {
				t.Fatal("ambiguous lineage accepted")
			}
			var after string
			if err := db.QueryRow(`SELECT group_concat(sql,'|') FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name`).Scan(&after); err != nil || after != before {
				t.Fatalf("schema changed before rejection: %v", err)
			}
		})
	}
}

func TestLegacy169CompatibilityFreshDatabase(t *testing.T) {
	db := openTestDB(t)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(fmt.Errorf("repeat fresh migrate: %w", err))
	}
}

// These numbers already belong to the foreign desktop lineage. A future source
// migration must start above them, even though our embedded SQL ends at 156.
func TestLegacy169MigrationNumbersRemainReserved(t *testing.T) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		version, err := strconv.Atoi(strings.SplitN(entry.Name(), "_", 2)[0])
		if err != nil {
			t.Fatal(err)
		}
		if version >= 157 && version <= 169 {
			t.Fatalf("%s reuses a recorded foreign migration number; use a fresh version above 169", entry.Name())
		}
	}
}

// Opt-in, read-only backup input; every writable connection targets a fresh
// testing-owned copy. Set TEMP/TMP inside the worker before invoking this test.
func TestLegacy169BackupCopy(t *testing.T) {
	source := os.Getenv("AO_SQLITE_LEGACY_BACKUP")
	if source == "" {
		t.Skip("set AO_SQLITE_LEGACY_BACKUP for the independent-copy smoke")
	}
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	dir := t.TempDir()
	out, err := os.Create(filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, digest), in)
	closeErr := out.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	t.Logf("copied backup bytes=%d sha256=%x", n, digest.Sum(nil))
	db, err := sql.Open("sqlite", databaseURI(dir)+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot all original columns of the small identity/history tables, and
	// counts of large conversation tables; never print user content to logs.
	var queries []string
	for _, table := range []string{"sessions", "review_run", "review", "pr", "agent_model_catalog", "session_worktrees", "notifications", "goose_db_version"} {
		rows, err := db.Query(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
		if err != nil {
			t.Fatal(err)
		}
		var columns []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			columns = append(columns, `"`+strings.ReplaceAll(name, `"`, `""`)+`"`)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		queries = append(queries, `SELECT `+strings.Join(columns, ",")+` FROM `+table+` ORDER BY rowid`)
	}
	for _, table := range []string{"conversation_messages", "conversation_activities", "conversation_provider_events", "conversation_turns", "automations", "automation_runs", "reports", "report_outputs", "report_worker_interrupts", "cues"} {
		queries = append(queries, `SELECT COUNT(*) FROM `+table)
	}
	snapshot := func(db *sql.DB) string {
		t.Helper()
		hash := sha256.New()
		for _, query := range queries {
			rows, err := db.Query(query)
			if err != nil {
				t.Fatal(err)
			}
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				values := make([]any, len(columns))
				dest := make([]any, len(columns))
				for i := range values {
					dest[i] = &values[i]
				}
				if err := rows.Scan(dest...); err != nil {
					t.Fatal(err)
				}
				if err := json.NewEncoder(hash).Encode(values); err != nil {
					t.Fatal(err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			_ = rows.Close()
		}
		return fmt.Sprintf("%x", hash.Sum(nil))
	}
	before := snapshot(db)
	_ = db.Close()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := store.ListAllSessions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("migrated independent copy; sessions=%d", len(sessions))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(dir, "ao.db-wal")); err == nil && info.Size() != 0 {
		t.Fatal("scratch WAL not checkpointed; immutable snapshot would be incomplete")
	}
	db, err = sql.Open("sqlite", databaseURI(dir)+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	after := snapshot(db)
	if after != before {
		t.Fatalf("retention snapshot changed: before=%s after=%s", before, after)
	}
	t.Logf("retention snapshot sha256=%s", after)
}
