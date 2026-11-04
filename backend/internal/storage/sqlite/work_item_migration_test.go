package sqlite

import "testing"

func TestWorkItemMigrationUpgradeAndRollback(t *testing.T) {
	db := openTestDB(t)
	upTo(t, db, 156)
	if _, err := db.Exec(`INSERT INTO projects(id,path,registered_at) VALUES('workitem-project','/fixture',CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	upTo(t, db, 218)
	const insert = `INSERT INTO work_items(id,attempt_id,project_id,repository,execution_base_sha,
original_parent_session_id,original_worker_session_id,owner_session_id,owner_generation,worktree_path,branch)
VALUES('work','attempt','workitem-project','__root__','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','parent','worker','worker',?,'/worktree','branch')`
	for _, generation := range []any{0, -1, 1.5, "unknown"} {
		if _, err := db.Exec(insert, generation); err == nil {
			t.Fatalf("invalid generation %v accepted", generation)
		}
	}
	if _, err := db.Exec(insert, 1); err != nil {
		t.Fatal(err)
	}
	// Retained references are not cascade-owned by live session/project rows.
	if _, err := db.Exec(`DELETE FROM projects WHERE id='workitem-project'`); err != nil {
		t.Fatal(err)
	}
	upTo(t, db, 218)
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM work_items WHERE id='work' AND owner_generation=1 AND original_native_session_id=''`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("repeated migration lost identity: count=%d, err=%v", count, err)
	}
	downTo(t, db, 156)
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='work_items'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("down migration: count=%d, err=%v", count, err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
}

func TestWorkItemMigrationFromDesktop169(t *testing.T) {
	db := legacy169(t)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('work_items')`).Scan(&count); err != nil || count != 12 {
		t.Fatalf("desktop upgrade: columns=%d, err=%v", count, err)
	}
	if err := migrate(db); err != nil {
		t.Fatalf("desktop restart: %v", err)
	}
}
