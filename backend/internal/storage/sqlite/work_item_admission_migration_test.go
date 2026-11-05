package sqlite

import (
	"reflect"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestWorkAdmissionMigrationCapacityAndRetention(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(1)
	upTo(t, db, 220)
	for _, statement := range []string{
		`PRAGMA foreign_keys=ON`,
		`INSERT INTO work_items VALUES('w1','a1','p','r','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','parent','worker','native','worker',1,'/w1','b1'),
		('w2','a2','p','r','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','parent','worker','native','worker',1,'/w2','b2')`,
		`INSERT INTO work_item_leases VALUES('w1',1,1,'lease',1000,2000,'[]','active')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	queries := []string{`SELECT * FROM work_items ORDER BY id`, `SELECT * FROM work_item_leases`,
		`SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name`,
		`SELECT * FROM worker_reports`, `SELECT * FROM orchestration_events`}
	before := make([][][]any, len(queries))
	for i, query := range queries {
		before[i] = reportMigrationRows(t, db, query)
	}
	upTo(t, db, 221)
	for i, query := range queries {
		if got := reportMigrationRows(t, db, query); !reflect.DeepEqual(got, before[i]) {
			t.Fatalf("upgrade changed %s: %v", query, got)
		}
	}
	if _, err := db.Exec(`INSERT INTO work_item_admissions VALUES('w1','a1','worker',1,'lease',1,'reserved')`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO work_item_admissions VALUES('w2','a2','worker',1,'lease',1,'reserved')`, // global slot
		`INSERT INTO work_item_admissions VALUES('w2','a1','worker',1,'lease',1,'released')`, // duplicate attempt
		`INSERT INTO work_item_admissions VALUES('foreign','a3','worker',1,'lease',1,'released')`,
		`UPDATE work_item_admissions SET owner_generation=0`,
		`UPDATE work_item_admissions SET lease_revision=0`,
		`UPDATE work_item_admissions SET state='unknown'`,
	} {
		if _, err := db.Exec(statement); err == nil {
			t.Fatalf("constraint accepted: %s", statement)
		}
	}
	if _, err := db.Exec(`UPDATE work_item_admissions SET state='released' WHERE work_item_id='w1';
		INSERT INTO work_item_admissions VALUES('w2','a2','worker',1,'lease',1,'reserved')`); err != nil {
		t.Fatal(err)
	}
	retained := reportMigrationRows(t, db, `SELECT * FROM work_item_admissions ORDER BY work_item_id`)
	// Exercise 0221's own Down, before applying the later forward-only 0222.
	err := func() error {
		gooseMu.Lock()
		defer gooseMu.Unlock()
		goose.SetBaseFS(migrationsFS)
		goose.SetLogger(goose.NopLogger())
		if err := goose.SetDialect("sqlite3"); err != nil {
			return err
		}
		return goose.DownTo(db, "migrations", 220)
	}()
	if err == nil || !strings.Contains(err.Error(), "work_item_admission_0221_is_forward_only") {
		t.Fatalf("0221's own rollback guard: %v", err)
	}
	if got := reportMigrationRows(t, db, `SELECT * FROM work_item_admissions ORDER BY work_item_id`); !reflect.DeepEqual(got, retained) {
		t.Fatalf("rollback changed reservations: %v", got)
	}
	if got := reportMigrationRows(t, db, `PRAGMA foreign_key_check`); len(got) != 0 {
		t.Fatalf("foreign key violations: %v", got)
	}
	var version int64
	if err := db.QueryRow(`SELECT version_id FROM goose_db_version ORDER BY id DESC LIMIT 1`).Scan(&version); err != nil || version != 221 {
		t.Fatalf("specific migration identity: %d %v", version, err)
	}
	if err := migrate(db); err != nil {
		t.Fatalf("forward/restart: %v", err)
	}
	if got := reportMigrationRows(t, db, `SELECT * FROM work_item_admissions ORDER BY work_item_id`); !reflect.DeepEqual(got, retained) {
		t.Fatalf("0222 changed reservations: %v", got)
	}
}
