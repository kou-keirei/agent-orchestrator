package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestTerminalRevisionMigrationPreservesCanonicalIdentityAndRollback(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "ao.db")+pragmas)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	upTo(t, db, 152)
	now := time.Now().UTC()
	if _, err := db.Exec(`INSERT INTO projects(id,path,registered_at) VALUES ('p','/p',?)`, now); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"recorded", "crash-window", "active", "restored-legacy"} {
		if _, err := db.Exec(`INSERT INTO sessions(id,project_id,num,harness,activity_last_at,created_at,updated_at,is_terminated,revision,cleanup_generation) VALUES (?,'p',?,'fake',?,?,?, ?,7,?)`, id, i+1, now, now, now, i != 2, i/3); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{"terminal-revision:3", "terminal-revision:4", "terminal-revision:999", "terminal-revision:4oops", "terminal-revision:05"} {
		if _, err := db.Exec(`INSERT INTO orchestration_source_states(project_id,worker_id,kind,source_id,active,generation,updated_at) VALUES ('p','recorded','worker_terminated',?,1,1,?)`, source, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO orchestration_source_states(project_id,worker_id,kind,source_id,active,generation,updated_at) VALUES ('p','restored-legacy','worker_terminated','terminal-revision:3',1,1,?)`, now); err != nil {
		t.Fatal(err)
	}
	var beforeCDC int
	if err := db.QueryRow(`SELECT COUNT(*) FROM change_log`).Scan(&beforeCDC); err != nil {
		t.Fatal(err)
	}
	upTo(t, db, 153)
	for id, want := range map[string]int64{"recorded": 3, "crash-window": 7, "active": 0, "restored-legacy": -1} {
		var revision, terminal int64
		if err := db.QueryRow(`SELECT revision,terminal_revision FROM sessions WHERE id=?`, id).Scan(&revision, &terminal); err != nil {
			t.Fatal(err)
		}
		if revision != 7 || terminal != want {
			t.Fatalf("%s revision=%d terminal=%d want=7,%d", id, revision, terminal, want)
		}
	}
	var afterCDC int
	if err := db.QueryRow(`SELECT COUNT(*) FROM change_log`).Scan(&afterCDC); err != nil || afterCDC != beforeCDC {
		t.Fatalf("migration CDC before=%d after=%d err=%v", beforeCDC, afterCDC, err)
	}
	for _, statement := range []string{
		`UPDATE sessions SET is_terminated=1 WHERE id='active'`,
		`UPDATE sessions SET updated_at=updated_at WHERE id='active'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var revision, terminal int64
	if err := db.QueryRow(`SELECT revision,terminal_revision FROM sessions WHERE id='active'`).Scan(&revision, &terminal); err != nil || revision != 9 || terminal != 8 {
		t.Fatalf("after observation revision=%d terminal=%d err=%v", revision, terminal, err)
	}
	if _, err := db.Exec(`UPDATE sessions SET is_terminated=0 WHERE id='active'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT terminal_revision FROM sessions WHERE id='active'`).Scan(&terminal); err != nil || terminal != 0 {
		t.Fatalf("restored terminal=%d err=%v", terminal, err)
	}
	if _, err := db.Exec(`UPDATE sessions SET is_terminated=1 WHERE id='active'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT revision,terminal_revision FROM sessions WHERE id='active'`).Scan(&revision, &terminal); err != nil || revision != 11 || terminal != 11 {
		t.Fatalf("new episode revision=%d terminal=%d err=%v", revision, terminal, err)
	}
	downTo(t, db, 152)
	if _, err := db.Exec(`UPDATE sessions SET updated_at=updated_at WHERE id='active'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT revision FROM sessions WHERE id='active'`).Scan(&revision); err != nil || revision != 12 {
		t.Fatalf("rollback revision=%d err=%v", revision, err)
	}
	upTo(t, db, 153)
}
