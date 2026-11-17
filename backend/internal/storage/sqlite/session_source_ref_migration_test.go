package sqlite

import (
	"reflect"
	"strings"
	"testing"
)

func TestSessionSourceRefMigration224PreservesLegacyFacts(t *testing.T) {
	db := openTestDB(t)
	upTo(t, db, 223)
	mustExec(t, db, `INSERT INTO projects(id,path,registered_at) VALUES ('source','/source',CURRENT_TIMESTAMP)`)
	mustExec(t, db, `INSERT INTO sessions(id,project_id,num,kind,harness,activity_state,activity_last_at,created_at,updated_at,execution_base_sha,diff_base_sha,diff_base_ref)
		VALUES ('source-1','source',1,'worker','codex','idle',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','refs/heads/comparison')`)
	before := reportMigrationRows(t, db, `SELECT id,execution_base_sha,diff_base_sha,diff_base_ref,revision FROM sessions`)
	upTo(t, db, 224)
	if got := reportMigrationRows(t, db, `SELECT id,execution_base_sha,diff_base_sha,diff_base_ref,revision FROM sessions`); !reflect.DeepEqual(got, before) {
		t.Fatal("migration changed legacy source/comparison facts")
	}
	var ref string
	if err := db.QueryRow(`SELECT source_ref FROM sessions`).Scan(&ref); err != nil || ref != "" {
		t.Fatalf("legacy ref must remain unknown: %q %v", ref, err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name='source_ref' AND type='TEXT' AND "notnull"=1 AND dflt_value="''"`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("source_ref schema: count=%d err=%v", count, err)
	}
	mustExec(t, db, `UPDATE sessions SET source_ref='refs/heads/source' WHERE id='source-1'`)
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT source_ref FROM sessions`).Scan(&ref); err != nil || ref != "refs/heads/source" {
		t.Fatalf("repeat migration changed ref: %q %v", ref, err)
	}
	if _, err := db.Exec(`UPDATE sessions SET source_ref=NULL`); err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("nullable source ref admitted: %v", err)
	}
	downTo(t, db, 223)
	if got := reportMigrationRows(t, db, `SELECT id,execution_base_sha,diff_base_sha,diff_base_ref FROM sessions`); len(got) != 1 {
		t.Fatal("downgrade lost legacy session")
	}
	upTo(t, db, 224)
	if err := db.QueryRow(`SELECT source_ref FROM sessions`).Scan(&ref); err != nil || ref != "" {
		t.Fatalf("upgrade reconstructed removed ref: %q %v", ref, err)
	}
}
