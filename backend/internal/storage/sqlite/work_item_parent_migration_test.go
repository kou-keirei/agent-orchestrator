package sqlite

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestWorkItemParentMigration222FreshUpgradeReopenAndForwardOnly(t *testing.T) {
	dir := t.TempDir()
	open := func() *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "parent.db"))+pragmas)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		return db
	}
	db := open()
	defer func() { _ = db.Close() }()
	upTo(t, db, 221)
	mustExec(t, db, `INSERT INTO work_items VALUES('work','attempt','p','repo','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','parent','worker','worker-native','worker',1,'/work','branch')`)
	mustExec(t, db, `INSERT INTO work_item_admissions VALUES('work','attempt','worker',1,'lease',1,'reserved')`)
	before := desktop169SnapshotExcept(t, db, map[string]bool{"goose_db_version": true, "sqlite_sequence": true})
	upTo(t, db, 222)
	if rows := reportMigrationRows(t, db, `SELECT * FROM work_item_parents`); len(rows) != 0 {
		t.Fatal("legacy provenance was backfilled")
	}
	if got := desktop169SnapshotExcept(t, db, map[string]bool{"goose_db_version": true, "sqlite_sequence": true, "work_item_parents": true}); got != before {
		t.Fatal("0222 changed prior schema/data")
	}
	mustExec(t, db, `INSERT INTO work_item_parents VALUES('work','attempt','parent','parent-native','generation','parent-native','scope','parent','generation','ao_live_original_parent_controller')`)
	retained := desktop169Snapshot(t, db)
	gooseMu.Lock()
	goose.SetBaseFS(migrationsFS)
	goose.SetLogger(goose.NopLogger())
	err := goose.SetDialect("sqlite3")
	if err == nil {
		err = goose.DownTo(db, "migrations", 221)
	}
	gooseMu.Unlock()
	if err == nil || !strings.Contains(err.Error(), "work_item_parent_0222_is_forward_only") {
		t.Fatalf("0222 guard: %v", err)
	}
	if desktop169Snapshot(t, db) != retained {
		t.Fatal("failed 0222 downgrade changed schema/data/journal")
	}
	for range 2 {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = open()
		if err := migrate(db); err != nil {
			t.Fatal(err)
		}
		if desktop169Snapshot(t, db) != retained {
			t.Fatal("reopen changed provenance or admission")
		}
	}
	var applied int
	for _, version := range []int{218, 219, 220, 221, 222} {
		if err := db.QueryRow(`SELECT is_applied FROM goose_db_version WHERE version_id=? ORDER BY id DESC LIMIT 1`, version).Scan(&applied); err != nil || applied != 1 {
			t.Fatalf("migration %d: %d %v", version, applied, err)
		}
	}
	if rows := reportMigrationRows(t, db, `PRAGMA foreign_key_check`); len(rows) != 0 {
		t.Fatal(rows)
	}
}
