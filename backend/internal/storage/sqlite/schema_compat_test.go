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

	modernsqlite "modernc.org/sqlite"
	sqlitelib "modernc.org/sqlite/lib"
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
	trigger = strings.Replace(trigger, "'autoReviewEnabled', json(CASE WHEN NEW.auto_review_enabled THEN 'true' ELSE 'false' END)", "'autoReviewEnabled', json(CASE WHEN NEW.auto_review_enabled THEN 'true' ELSE 'false' END), 'provisionState', NEW.provision_state", 1)
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
		if err := rows.Err(); err != nil {
			t.Fatal(err)
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

// Snapshot all synthetic table values as well as schema, including CDC and
// Goose history. An eventual error is insufficient if an earlier step wrote.
func desktop169Snapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	hash := sha256.New()
	rows, err := db.Query(`SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master UNION ALL SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_temp_master ORDER BY type,name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var kind, name, owner, ddl string
		if err := rows.Scan(&kind, &name, &owner, &ddl); err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(hash).Encode([]string{kind, name, owner, ddl}); err != nil {
			t.Fatal(err)
		}
		if kind == "table" && (!strings.HasPrefix(name, "sqlite_") || name == "sqlite_sequence") {
			tables = append(tables, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `" ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			args := make([]any, len(columns))
			for i := range args {
				args[i] = &values[i]
			}
			if err := rows.Scan(args...); err != nil {
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

func rejectDesktop169Unchanged(t *testing.T, db *sql.DB) {
	t.Helper()
	before := desktop169Snapshot(t, db)
	if err := migrate(db); err == nil {
		t.Error("unsafe schema accepted")
	}
	if after := desktop169Snapshot(t, db); after != before {
		t.Errorf("compatibility wrote before rejection: before=%s after=%s", before, after)
	}
}

func TestLegacy169R2IncomingForeignKey(t *testing.T) {
	for _, parent := range []string{"Notifications", `"NOTIFICATIONS"`, "nOtIfIcAtIoNs"} {
		t.Run(parent, func(t *testing.T) {
			db := legacy169(t)
			if _, err := db.Exec(`PRAGMA foreign_keys=ON; CREATE TABLE foreign_child(id TEXT PRIMARY KEY, notice_id TEXT REFERENCES ` + parent + `(id) ON DELETE CASCADE, content TEXT); INSERT INTO foreign_child VALUES ('child','notice','retain child')`); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
			var value string
			if err := db.QueryRow(`SELECT content FROM foreign_child WHERE id='child'`).Scan(&value); err != nil || value != "retain child" {
				t.Errorf("child corrupted: value=%q err=%v", value, err)
			}
		})
	}
}

func TestLegacy169R2NotificationPhysicalColumns(t *testing.T) {
	for _, shape := range []string{"reordered", "generated hidden"} {
		t.Run(shape, func(t *testing.T) {
			db := legacy169(t)
			if shape == "reordered" {
				if _, err := db.Exec(`ALTER TABLE notifications RENAME TO original_notifications;
CREATE TABLE notifications(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,pr_url TEXT NOT NULL DEFAULT '',type TEXT NOT NULL CHECK(type IN ('needs_input','ready_to_merge','pr_merged','pr_closed_unmerged')),body TEXT NOT NULL DEFAULT '',title TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'unread' CHECK(status IN ('read','unread')),created_at TIMESTAMP NOT NULL,resolved_at TIMESTAMP);
INSERT INTO notifications(id,session_id,project_id,pr_url,type,title,body,status,created_at,resolved_at) SELECT id,session_id,project_id,pr_url,type,title,'retain body',status,created_at,resolved_at FROM original_notifications;
DROP TABLE original_notifications;`); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.Exec(`ALTER TABLE notifications ADD COLUMN foreign_generated TEXT GENERATED ALWAYS AS (title) VIRTUAL`); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
			var title, body string
			if err := db.QueryRow(`SELECT title,body FROM notifications WHERE id='notice'`).Scan(&title, &body); err != nil {
				t.Fatal(err)
			}
			wantBody := ""
			if shape == "reordered" {
				wantBody = "retain body"
			}
			if title != "preserve notification" || body != wantBody {
				t.Errorf("positional copy corrupted title=%q body=%q", title, body)
			}
		})
	}
}

func TestLegacy169R2ReviewIndex(t *testing.T) {
	const predicate = ` WHERE target_sha != '' AND status NOT IN ('failed', 'cancelled') AND (status = 'running' OR verdict NOT IN ('', 'changes_requested'))`
	for _, restart := range []bool{false, true} {
		for _, shape := range []string{"nonunique", "not partial", "wrong predicate", "foreign owner", "literal case"} {
			t.Run(fmt.Sprintf("restart=%v/%s", restart, shape), func(t *testing.T) {
				db := legacy169(t)
				columns := "session_id, pr_url, target_sha, harness"
				if restart {
					if err := migrate(db); err != nil {
						t.Fatal(err)
					}
					columns += ", reviewer_provenance_json"
				}
				ddl := "CREATE UNIQUE INDEX idx_review_run_session_pr_sha_harness ON review_run (" + columns + ")" + predicate
				switch shape {
				case "nonunique":
					ddl = strings.Replace(ddl, "CREATE UNIQUE INDEX", "CREATE INDEX", 1)
				case "not partial":
					ddl = strings.TrimSuffix(ddl, predicate)
				case "wrong predicate":
					ddl = strings.TrimSuffix(ddl, predicate) + ` WHERE target_sha != ''`
				case "literal case":
					ddl = strings.Replace(ddl, "'failed'", "'FAILED'", 1)
				case "foreign owner":
					if _, err := db.Exec(`CREATE TABLE foreign_reviews(session_id TEXT,pr_url TEXT,target_sha TEXT,harness TEXT,reviewer_provenance_json TEXT,status TEXT,verdict TEXT)`); err != nil {
						t.Fatal(err)
					}
					ddl = strings.Replace(ddl, "ON review_run", "ON foreign_reviews", 1)
				}
				if _, err := db.Exec(`DROP INDEX idx_review_run_session_pr_sha_harness; ` + ddl); err != nil {
					t.Fatal(err)
				}
				rejectDesktop169Unchanged(t, db)
			})
		}
	}
}

func TestLegacy169R2DestinationCollisions(t *testing.T) {
	for _, ddl := range []string{
		`CREATE INDEX idx_orchestration_events_due ON foreign_history(content)`,
		`CREATE INDEX IDX_ORCHESTRATION_EVENTS_PROJECT ON foreign_history(content)`,
		`CREATE TABLE notifications_before_orchestration_attention(id TEXT)`,
		`CREATE TRIGGER review_run_immutable_binding AFTER UPDATE ON foreign_history BEGIN SELECT 1; END`,
		`CREATE INDEX idx_reviewer_cleanup_obligation_unresolved ON foreign_history(content)`,
		`CREATE TABLE Orchestration_Events(id TEXT)`,
		`DROP TRIGGER sessions_cdc_update; CREATE TRIGGER sessions_cdc_update AFTER UPDATE ON foreign_history BEGIN SELECT 1; END`,
		`DROP INDEX idx_notifications_history; CREATE INDEX idx_notifications_history ON foreign_history(content)`,
	} {
		t.Run(ddl, func(t *testing.T) {
			db := legacy169(t)
			if _, err := db.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
		})
	}
}

// The declaration used by the observed live lineage, including migration 41's
// appended resolved_at column. Keep this control independent of the preflight.
const legacy169NotificationsDDL = `CREATE TABLE notifications (
id TEXT PRIMARY KEY,
session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
pr_url TEXT NOT NULL DEFAULT '',
type TEXT NOT NULL CHECK (type IN ('needs_input', 'ready_to_merge', 'pr_merged', 'pr_closed_unmerged')),
title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '',
status TEXT NOT NULL DEFAULT 'unread' CHECK (status IN ('read', 'unread')),
created_at TIMESTAMP NOT NULL, resolved_at TIMESTAMP)`

func TestLegacy169R3NotificationDeclarations(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{"real affinity", "title TEXT NOT NULL", "title REAL NOT NULL"},
		{"integer affinity", "title TEXT NOT NULL", "title INTEGER NOT NULL"},
		{"implicit unique", "title TEXT NOT NULL", "title TEXT NOT NULL UNIQUE"},
		{"composite unique", "resolved_at TIMESTAMP)", "resolved_at TIMESTAMP, UNIQUE(title,body))"},
		{"foreign check", "title TEXT NOT NULL", "title TEXT NOT NULL CHECK(length(title)>0)"},
		{"missing type check", "CHECK (type IN ('needs_input', 'ready_to_merge', 'pr_merged', 'pr_closed_unmerged'))", ""},
		{"changed type check", "'pr_closed_unmerged'", "'pr_closed_unmerged', 'foreign_kind'"},
		{"literal whitespace", "'pr_closed_unmerged'", "'pr_closed_ unmerged'"},
		{"foreign default", "body TEXT NOT NULL DEFAULT ''", "body TEXT NOT NULL DEFAULT 'foreign default'"},
		{"literal default whitespace", "body TEXT NOT NULL DEFAULT ''", "body TEXT NOT NULL DEFAULT ' '"},
		{"missing default", "pr_url TEXT NOT NULL DEFAULT ''", "pr_url TEXT NOT NULL"},
		{"nullable title", "title TEXT NOT NULL", "title TEXT"},
		{"composite primary key", "id TEXT PRIMARY KEY", "id TEXT"},
		{"missing foreign key", "project_id TEXT REFERENCES projects(id) ON DELETE CASCADE", "project_id TEXT"},
		{"changed foreign key", "REFERENCES projects(id) ON DELETE CASCADE", "REFERENCES projects(id) ON DELETE SET NULL"},
		{"foreign collation", "title TEXT NOT NULL", "title TEXT COLLATE NOCASE NOT NULL"},
		{"without rowid", "resolved_at TIMESTAMP)", "resolved_at TIMESTAMP) WITHOUT ROWID"},
		{"stored generated", "resolved_at TIMESTAMP)", "resolved_at TIMESTAMP, extra TEXT GENERATED ALWAYS AS (title) STORED)"},
		{"virtual generated", "resolved_at TIMESTAMP)", "resolved_at TIMESTAMP, extra TEXT GENERATED ALWAYS AS (title) VIRTUAL)"},
		{"strict storage", "TIMESTAMP", "TEXT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := legacy169(t)
			ddl := strings.ReplaceAll(legacy169NotificationsDDL, tc.old, tc.replacement)
			if tc.name == "composite primary key" {
				ddl = strings.TrimSuffix(ddl, ")") + ", PRIMARY KEY(id,title))"
			}
			if tc.name == "strict storage" {
				ddl += " STRICT"
			}
			if _, err := db.Exec(`DROP TABLE notifications; ` + ddl); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`CREATE INDEX idx_notifications_status_history ON notifications(status,created_at DESC,id DESC);
CREATE INDEX idx_notifications_history ON notifications(created_at DESC,id DESC);
CREATE UNIQUE INDEX idx_notifications_open_dedupe ON notifications(session_id,type,pr_url) WHERE status='unread' OR resolved_at IS NULL;
CREATE INDEX idx_notifications_unresolved ON notifications(resolved_at,created_at DESC,id DESC);`); err != nil {
				t.Fatal(err)
			}
			var title any = "preserve notification"
			if tc.name == "real affinity" || tc.name == "integer affinity" {
				title = 1.2345678901234567
			}
			if _, err := db.Exec(`INSERT INTO notifications(id,session_id,pr_url,type,title,body,created_at) VALUES ('notice','legacy-1','','needs_input',?,'',CURRENT_TIMESTAMP)`, title); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
			if tc.name == "real affinity" {
				var got any
				if err := db.QueryRow(`SELECT title FROM notifications WHERE id='notice'`).Scan(&got); err != nil || got != title {
					t.Errorf("numeric title changed: got=%v (%T) want=%v err=%v", got, got, title, err)
				}
			}
			if tc.name == "implicit unique" {
				if _, err := db.Exec(`INSERT INTO notifications(id,session_id,pr_url,type,title,created_at) VALUES ('duplicate','legacy-1','different-pr','needs_input','preserve notification',CURRENT_TIMESTAMP)`); err == nil {
					t.Error("foreign title uniqueness was lost")
				}
			}
		})
	}
}

func TestLegacy169R3NotificationDependencies(t *testing.T) {
	for _, reference := range []string{"notifications", "Notifications", `"NOTIFICATIONS"`, "[nOtIfIcAtIoNs]", "`notifications`", "'notifications'", `main."NOTIFICATIONS"`} {
		t.Run(reference, func(t *testing.T) {
			db := legacy169(t)
			if _, err := db.Exec(`CREATE VIEW foreign_notice_view AS SELECT id,title FROM ` + reference); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
			var title string
			if err := db.QueryRow(`SELECT title FROM foreign_notice_view WHERE id='notice'`).Scan(&title); err != nil || title != "preserve notification" {
				t.Errorf("dependent view invalidated: title=%q err=%v", title, err)
			}
		})
	}
	for _, ddl := range []string{
		`CREATE TRIGGER foreign_notice_trigger AFTER UPDATE ON foreign_history BEGIN UPDATE "NOTIFICATIONS" SET title=title; END`,
		`CREATE TEMP TRIGGER foreign_notice_trigger AFTER UPDATE ON main.foreign_history BEGIN UPDATE [Notifications] SET title=title; END`,
		`CREATE TEMP VIEW foreign_notice_view AS SELECT * FROM main."Notifications"`,
		`CREATE VIEW foreign_broken_view AS SELECT * FROM missing_foreign_table`,
	} {
		t.Run(ddl, func(t *testing.T) {
			db := legacy169(t)
			if _, err := db.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
		})
	}
}

func TestLegacy169R3OtherDestructiveActions(t *testing.T) {
	for _, ddl := range []string{
		`DROP INDEX idx_notifications_history; CREATE UNIQUE INDEX idx_notifications_history ON notifications(title)`,
		`CREATE TRIGGER foreign_session_update AFTER UPDATE ON sessions BEGIN UPDATE foreign_history SET content='unexpected backfill side effect'; END`,
		`DROP TRIGGER sessions_cdc_update; CREATE TRIGGER sessions_cdc_update AFTER UPDATE ON sessions BEGIN UPDATE foreign_history SET content='foreign CDC'; END`,
		`ALTER TABLE sessions ADD COLUMN terminal_revision INTEGER GENERATED ALWAYS AS (revision) VIRTUAL`,
		`ALTER TABLE review_run ADD COLUMN target_binding_json TEXT GENERATED ALWAYS AS ('{}') VIRTUAL`,
		`ALTER TABLE sessions ADD COLUMN TERMINAL_REVISION INTEGER NOT NULL DEFAULT 0`,
	} {
		t.Run(ddl, func(t *testing.T) {
			db := legacy169(t)
			if _, err := db.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			rejectDesktop169Unchanged(t, db)
		})
	}
}

func TestLegacy169R3CanonicalLiveShape(t *testing.T) {
	db := legacy169(t)
	if _, err := db.Exec(`CREATE VIEW unrelated_history AS SELECT content FROM foreign_history`); err != nil {
		t.Fatal(err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if err := migrate(db); err != nil {
		t.Fatalf("canonical restart: %v", err)
	}
	var content string
	if err := db.QueryRow(`SELECT content FROM unrelated_history`).Scan(&content); err != nil || content != "keep verbatim" {
		t.Fatalf("unrelated view changed: %q %v", content, err)
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
		defer rows.Close()
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
			defer rows.Close()
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
	var expectedSessions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&expectedSessions); err != nil {
		t.Fatal(err)
	}
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
	if len(sessions) != expectedSessions {
		t.Fatalf("session readability: got=%d want=%d", len(sessions), expectedSessions)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatalf("independent copy restart: %v", err)
	}
	sessions, err = store.ListAllSessions(t.Context())
	if err != nil || len(sessions) != expectedSessions {
		t.Fatalf("restart session readability: got=%d want=%d err=%v", len(sessions), expectedSessions, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("independent copy idempotent restart; sessions=%d", len(sessions))
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

// Exact captured historical SQL, independent of the production allowlist.
const historical169CDC = `CREATE TRIGGER sessions_cdc_update
AFTER UPDATE ON sessions
WHEN OLD.activity_state <> NEW.activity_state
    OR OLD.is_terminated <> NEW.is_terminated
    OR (OLD.first_signal_at IS NULL AND NEW.first_signal_at IS NOT NULL)
    OR OLD.preview_url <> NEW.preview_url
    OR OLD.preview_revision <> NEW.preview_revision
    OR OLD.display_name <> NEW.display_name
    OR OLD.terminate_on_pr_merge <> NEW.terminate_on_pr_merge
    OR OLD.is_pinned <> NEW.is_pinned
    OR OLD.pinned_at <> NEW.pinned_at
    OR (OLD.pinned_at IS NULL AND NEW.pinned_at IS NOT NULL)
    OR (OLD.pinned_at IS NOT NULL AND NEW.pinned_at IS NULL)
    OR OLD.session_mode <> NEW.session_mode
    OR OLD.auto_inject_review <> NEW.auto_inject_review
    OR OLD.auto_review_enabled <> NEW.auto_review_enabled
    OR OLD.harness <> NEW.harness
    OR OLD.runtime_launch_id <> NEW.runtime_launch_id
    OR OLD.agent_session_id <> NEW.agent_session_id
    OR OLD.native_transcript_path <> NEW.native_transcript_path
    OR OLD.auto_inject_ci <> NEW.auto_inject_ci
    OR OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at
    OR OLD.hidden_at IS NOT NEW.hidden_at
BEGIN
    INSERT INTO change_log (project_id, session_id, event_type, payload, created_at)
    VALUES (NEW.project_id, NEW.id, 'session_updated',
        json_object(
            'id', NEW.id,
            'activity', NEW.activity_state,
            'isTerminated', json(CASE WHEN NEW.is_terminated THEN 'true' ELSE 'false' END),
            'terminateOnPrMerge', json(CASE WHEN NEW.terminate_on_pr_merge THEN 'true' ELSE 'false' END),
            'previewUrl', NEW.preview_url,
            'previewRevision', NEW.preview_revision,
            'isPinned', json(CASE WHEN NEW.is_pinned THEN 'true' ELSE 'false' END),
            'mode', NEW.session_mode,
            'autoInjectReview', json(CASE WHEN NEW.auto_inject_review THEN 'true' ELSE 'false' END),
            'autoInjectCI', json(CASE WHEN NEW.auto_inject_ci THEN 'true' ELSE 'false' END),
            'autoReviewEnabled', json(CASE WHEN NEW.auto_review_enabled THEN 'true' ELSE 'false' END),
            'hiddenAt', NEW.hidden_at
        ),
        NEW.updated_at);
END`

func historical169DB(t *testing.T, reconciled bool) *sql.DB {
	t.Helper()
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(historical169CDC))); got != "a029ee1be09785960daaa41277c16e6d11150c97ccae9a46fa227b1ca4c48f0f" {
		t.Fatalf("historical fixture changed: %s", got)
	}
	db := legacy169(t)
	if reconciled {
		if err := migrate(db); err != nil {
			t.Fatal(err)
		}
	} else {
		mustExec(t, db, `ALTER TABLE sessions ADD COLUMN hidden_at TIMESTAMP`)
	}
	mustExec(t, db, `DROP TRIGGER sessions_cdc_update; `+historical169CDC)
	return db
}

func TestLegacy169HistoricalCDCUpgrade(t *testing.T) {
	for _, reconciled := range []bool{false, true} {
		t.Run(fmt.Sprintf("reconciled=%v", reconciled), func(t *testing.T) {
			db := historical169DB(t, reconciled)
			// Retain a real historical event byte-for-byte across startup.
			mustExec(t, db, `UPDATE sessions SET display_name='before upgrade' WHERE id='legacy-1'`)
			var before string
			if err := db.QueryRow(`SELECT json_group_array(json_array(seq,project_id,session_id,event_type,payload,created_at)) FROM change_log`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			if err := migrate(db); err != nil {
				t.Fatalf("historical startup: %v", err)
			}
			var after string
			if err := db.QueryRow(`SELECT json_group_array(json_array(seq,project_id,session_id,event_type,payload,created_at)) FROM change_log`).Scan(&after); err != nil || after != before {
				t.Fatalf("startup lost or manufactured CDC: before=%s after=%s err=%v", before, after, err)
			}
			// A second startup must change neither schema nor any durable value.
			snapshot := desktop169Snapshot(t, db)
			if err := migrate(db); err != nil {
				t.Fatalf("historical restart: %v", err)
			}
			if got := desktop169Snapshot(t, db); got != snapshot {
				t.Fatal("restart changed schema or data")
			}
			for _, set := range []string{
				`display_name='after upgrade'`, `activity_state='active'`, `is_terminated=0`,
				`first_signal_at=CURRENT_TIMESTAMP`, `preview_url='http://localhost'`, `preview_revision=1`,
				`terminate_on_pr_merge=1`, `is_pinned=1`, `pinned_at=CURRENT_TIMESTAMP`, `pinned_at=NULL`,
				`session_mode='chat'`, `auto_inject_review=0`, `auto_review_enabled=1`, `harness='codex'`,
				`runtime_launch_id='launch'`, `agent_session_id='native'`, `native_transcript_path='transcript'`,
				`auto_inject_ci=0`, `latest_user_prompt_at=CURRENT_TIMESTAMP`,
				`hidden_at=CURRENT_TIMESTAMP`, `hidden_at=NULL`, `provision_state='ready'`,
			} {
				t.Run(set, func(t *testing.T) {
					count := countChangeLog(t, db, "session_updated")
					mustExec(t, db, `UPDATE sessions SET `+set+` WHERE id='legacy-1'`)
					if got := countChangeLog(t, db, "session_updated"); got != count+1 {
						t.Fatalf("want exactly one event: before=%d after=%d", count, got)
					}
					var project, session, eventType, payload string
					if err := db.QueryRow(`SELECT project_id,session_id,event_type,payload FROM change_log ORDER BY seq DESC LIMIT 1`).Scan(&project, &session, &eventType, &payload); err != nil {
						t.Fatal(err)
					}
					var event map[string]any
					if err := json.Unmarshal([]byte(payload), &event); err != nil {
						t.Fatal(err)
					}
					if project != "legacy" || session != "legacy-1" || eventType != "session_updated" || event["id"] != "legacy-1" || len(event) != 13 {
						t.Fatalf("invalid CDC envelope/payload: %s %s %s %s", project, session, eventType, payload)
					}
					if _, ok := event["isTerminated"].(bool); !ok {
						t.Fatalf("boolean lost JSON type: %s", payload)
					}
					if _, ok := event["hiddenAt"]; !ok {
						t.Fatal("hiddenAt missing")
					}
					if value, ok := event["provisionState"]; !ok || (set == `provision_state='ready'` && value != "ready") {
						t.Fatalf("provisionState missing/wrong: %s", payload)
					}
				})
			}
			count := countChangeLog(t, db, "session_updated")
			mustExec(t, db, `UPDATE sessions SET prompt='not a CDC predicate',provision_state='ready' WHERE id='legacy-1'`)
			if got := countChangeLog(t, db, "session_updated"); got != count {
				t.Fatal("noise or unchanged provisioning emitted CDC")
			}
		})
	}
}

func TestLegacy169HistoricalCDCRejectsForeign(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{"foreign literal", "'session_updated'", "'SESSION_UPDATED'"},
		{"foreign payload", "'hiddenAt'", "'hidden_at'"},
		{"missing old predicate", "OR OLD.display_name <> NEW.display_name", ""},
		{"provision predicate only", "OR OLD.hidden_at IS NOT NEW.hidden_at", "OR OLD.hidden_at IS NOT NEW.hidden_at OR OLD.provision_state <> NEW.provision_state"},
		{"provision payload only", "'hiddenAt', NEW.hidden_at", "'hiddenAt', NEW.hidden_at, 'provisionState', NEW.provision_state"},
		{"extra side effect", "\nEND", "\nUPDATE foreign_history SET content='unexpected'; END"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := historical169DB(t, false)
			mustExec(t, db, `DROP TRIGGER sessions_cdc_update; `+strings.Replace(historical169CDC, tc.old, tc.replacement, 1))
			rejectDesktop169Unchanged(t, db)
		})
	}
	for _, tc := range []struct{ name, ddl string }{
		{"higher journal", `INSERT INTO goose_db_version(version_id,is_applied) VALUES (170,1)`},
		{"rolled back claim", `INSERT INTO goose_db_version(version_id,is_applied) VALUES (153,0)`},
		{"foreign backfill dependency", `CREATE TRIGGER foreign_update AFTER UPDATE ON sessions BEGIN UPDATE foreign_history SET content='unexpected'; END`},
		{"temporary dependency", `CREATE TEMP TRIGGER temp_update AFTER UPDATE ON main.sessions BEGIN SELECT 1; END`},
		{"foreign CDC owner", `DROP TRIGGER sessions_cdc_update; CREATE TRIGGER sessions_cdc_update AFTER UPDATE ON foreign_history BEGIN SELECT 1; END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := historical169DB(t, false)
			mustExec(t, db, tc.ddl)
			rejectDesktop169Unchanged(t, db)
		})
	}
}

func TestLegacy169HistoricalCDCTransactionalReplacement(t *testing.T) {
	db := historical169DB(t, true)
	before := desktop169Snapshot(t, db)
	// Native per-connection SQL length limit lets DROP succeed and forces the
	// larger CREATE to fail. The reconciler must roll the DROP back, with no
	// production data, global hooks, or product injection seam involved.
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	old, err := modernsqlite.Limit(conn, sqlitelib.SQLITE_LIMIT_SQL_LENGTH, len(historical169CDC)+1)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	err = reconcileSessionHiddenCDCTrigger(db)
	conn, connErr := db.Conn(t.Context())
	if connErr != nil {
		t.Fatal(connErr)
	}
	_, resetErr := modernsqlite.Limit(conn, sqlitelib.SQLITE_LIMIT_SQL_LENGTH, old)
	_ = conn.Close()
	if resetErr != nil {
		t.Fatal(resetErr)
	}
	if err == nil || !strings.Contains(err.Error(), "replay session hidden CDC trigger") {
		t.Fatalf("expected CREATE failure after DROP: %v", err)
	}
	if got := desktop169Snapshot(t, db); got != before {
		t.Fatal("failed replacement lost trigger or changed durable state")
	}
	// The rolled-back original still emits a complete historical event.
	count := countChangeLog(t, db, "session_updated")
	mustExec(t, db, `UPDATE sessions SET display_name='after rollback' WHERE id='legacy-1'`)
	if countChangeLog(t, db, "session_updated") != count+1 {
		t.Fatal("rollback left a CDC gap")
	}
	if err := migrate(db); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}
