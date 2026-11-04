package sqlite

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/pressly/goose/v3"
)

const legacyOutboxColumns = `id,project_id,worker_id,kind,source_revision,state,attempt_count,enqueued_at,next_attempt_at,lease_token,lease_expires_at,destination_session_id,submitted_at,acknowledged_at,attention_required_at,last_error`

func reportMigrationRows(t *testing.T, db *sql.DB, query string) [][]any {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]any
	for rows.Next() {
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		result = append(result, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func seedReportMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	db.SetMaxOpenConns(1)
	upTo(t, db, 219)
	for _, statement := range []string{
		`PRAGMA foreign_keys=ON`,
		`INSERT INTO projects(id,path,registered_at) VALUES('p','/p',CURRENT_TIMESTAMP)`,
		`INSERT INTO sessions(id,project_id,num,harness,activity_last_at,created_at,updated_at) VALUES('w','p',1,'fake',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('parent','p',2,'fake',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`,
		`INSERT INTO work_items VALUES('work','attempt','p','repo','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','parent','w','native','w',1,'/work','branch')`,
		`INSERT INTO work_item_leases VALUES('work',1,1,'lease',1000,2000,'[]','active')`,
		`INSERT INTO orchestration_source_states VALUES('p','w','worker_blocked','source',1,4,CURRENT_TIMESTAMP)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	for i, state := range []string{"pending", "leased", "submitted", "acknowledged", "dead_letter"} {
		kind := []string{"worker_turn_settled", "worker_blocked", "worker_ready_to_merge", "worker_terminated", "pr_merged"}[i]
		_, err := db.Exec(`INSERT INTO orchestration_events(`+legacyOutboxColumns+`) VALUES(?,'p','w',?,? ,?,8,'2026-11-01 01:02:03','2026-11-02 02:03:04','token','2026-11-03 03:04:05','parent','2026-11-01 04:05:06','2026-11-01 05:06:07','2026-11-01 06:07:08','retained diagnostic')`, fmt.Sprint(i), kind, fmt.Sprint(i), state)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkerReportMigrationPreservesOutboxAndForeignKeys(t *testing.T) {
	db := openTestDB(t)
	seedReportMigration(t, db)
	queries := []string{`SELECT ` + legacyOutboxColumns + ` FROM orchestration_events ORDER BY id`,
		`SELECT * FROM orchestration_source_states`, `SELECT * FROM work_items`, `SELECT * FROM work_item_leases`,
		`SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name`,
		`SELECT name,sql FROM sqlite_master WHERE name IN ('idx_orchestration_events_due','idx_orchestration_events_project') ORDER BY name`,
		`SELECT "table","from","to",on_update,on_delete FROM pragma_foreign_key_list('orchestration_events') WHERE "from"<>'worker_report_id' ORDER BY "from"`}
	before := make([][][]any, len(queries))
	for i, q := range queries {
		before[i] = reportMigrationRows(t, db, q)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	for i, q := range queries {
		if i == 4 { // Existing triggers retain exact SQL; the four new triggers are additive.
			q = `SELECT name,sql FROM sqlite_master WHERE type='trigger' AND name NOT IN ('worker_reports_immutable_update','worker_reports_immutable_delete','orchestration_worker_report_binding','orchestration_worker_report_immutable') ORDER BY name`
		}
		if got := reportMigrationRows(t, db, q); !reflect.DeepEqual(got, before[i]) {
			t.Fatalf("migration changed %s\nbefore=%v\nafter=%v", q, before[i], got)
		}
	}
	if got := reportMigrationRows(t, db, `PRAGMA foreign_key_check`); len(got) != 0 {
		t.Fatalf("foreign key violations: %v", got)
	}
	if _, err := db.Exec(`INSERT INTO orchestration_events(id,project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at) SELECT 'duplicate',project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at FROM orchestration_events LIMIT 1`); err == nil {
		t.Fatal("legacy unique constraint lost")
	}
	if _, err := db.Exec(`INSERT INTO orchestration_events(id,project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at) VALUES('foreign','missing','w','worker_blocked','r',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`); err == nil {
		t.Fatal("project FK lost")
	}
	// Mirror native session cleanup: CDC rows deliberately have restrictive FKs.
	if _, err := db.Exec(`DELETE FROM change_log WHERE session_id='parent'; DELETE FROM sessions WHERE id='parent'`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM orchestration_events WHERE destination_session_id IS NULL`).Scan(&n); err != nil || n != 5 {
		t.Fatalf("destination SET NULL lost: %d %v", n, err)
	}
	if _, err := db.Exec(`DELETE FROM change_log WHERE session_id='w'; DELETE FROM sessions WHERE id='w'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM orchestration_events`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("worker cascade lost: %d %v", n, err)
	}
	if err := migrate(db); err != nil {
		t.Fatalf("restart migration: %v", err)
	}
}

func TestWorkerReportMigrationCopyFailureRollsBack(t *testing.T) {
	db := openTestDB(t)
	seedReportMigration(t, db)
	if _, err := db.Exec(`PRAGMA ignore_check_constraints=ON; UPDATE orchestration_events SET state='invalid' WHERE id='0'; PRAGMA ignore_check_constraints=OFF`); err != nil {
		t.Fatal(err)
	}
	before := reportMigrationRows(t, db, `SELECT `+legacyOutboxColumns+` FROM orchestration_events ORDER BY id`)
	if err := migrate(db); err == nil {
		t.Fatal("invalid legacy row silently discarded")
	}
	if got := reportMigrationRows(t, db, `SELECT `+legacyOutboxColumns+` FROM orchestration_events ORDER BY id`); !reflect.DeepEqual(got, before) {
		t.Fatal("failed copy changed old rows")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('worker_reports','orchestration_events_with_reports')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial schema: %d %v", n, err)
	}
}

func TestWorkerReportMigrationIsForwardOnly(t *testing.T) {
	db := openTestDB(t)
	seedReportMigration(t, db)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	gooseMu.Lock()
	defer gooseMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatal(err)
	}
	if err := goose.DownTo(db, "migrations", 219); err == nil {
		t.Fatal("forward-only migration rolled back")
	}
	var version int64
	if err := db.QueryRow(`SELECT MAX(version_id) FROM goose_db_version WHERE is_applied=1`).Scan(&version); err != nil || version != 220 {
		t.Fatalf("failed downgrade altered journal: %d %v", version, err)
	}
	if got := reportMigrationRows(t, db, `SELECT id FROM orchestration_events ORDER BY id`); len(got) != 5 {
		t.Fatal("failed downgrade lost events")
	}
}
