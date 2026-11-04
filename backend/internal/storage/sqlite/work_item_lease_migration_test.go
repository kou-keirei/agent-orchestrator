package sqlite

import "testing"

func TestWorkLeaseMigrationUpgradeRollback(t *testing.T) {
	db := openTestDB(t)
	upTo(t, db, 218)
	if _, err := db.Exec(`INSERT INTO work_items VALUES('w','a','p','r','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','parent','worker','native','worker',1,'/w','b')`); err != nil {
		t.Fatal(err)
	}
	upTo(t, db, 219)
	if _, err := db.Exec(`INSERT INTO work_item_leases VALUES('w',1,1,'lease',1000,1000,'[]','active')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO work_item_leases VALUES('w',1,2,'other',1000,2000,'[]','active')`); err == nil {
		t.Fatal("second current lease accepted")
	}
	upTo(t, db, 219)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM work_item_leases WHERE lease_id='lease'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("restart: %d %v", n, err)
	}
	downTo(t, db, 218)
	if err := db.QueryRow(`SELECT count(*) FROM work_items WHERE original_native_session_id='native'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("identity lost on rollback: %d %v", n, err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}
}
