package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

// Exercise the actual fence with two independent SQLite connections. The small
// table isolates SQLite snapshot/lock behavior from the full admission fixture.
func TestOrdinarySendEvidenceSQLiteFence(t *testing.T) {
	for _, changedSnapshot := range []bool{false, true} {
		name := "excludes_writer"
		if changedSnapshot {
			name = "rejects_replaced_snapshot"
		}
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "fence.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`PRAGMA journal_mode=WAL;
				CREATE TABLE conversation_send_attempts(turn_id TEXT PRIMARY KEY, state TEXT);
				INSERT INTO conversation_send_attempts VALUES('turn', 'pending')`); err != nil {
				t.Fatal(err)
			}
			writer, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if changedSnapshot {
				var state string
				if err := tx.QueryRow(`SELECT state FROM conversation_send_attempts WHERE turn_id='turn'`).Scan(&state); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.ExecContext(t.Context(), `UPDATE conversation_send_attempts SET state='reserved'`); err != nil {
					t.Fatal(err)
				}
			}
			err = fenceOrdinarySendEvidence(t.Context(), tx, "turn")
			if changedSnapshot {
				var coded interface{ Code() int }
				if !errors.As(err, &coded) || coded.Code()&255 != 5 {
					t.Fatalf("stale snapshot acquired fence: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = writer.ExecContext(t.Context(), `UPDATE conversation_send_attempts SET state='reserved'`)
			var coded interface{ Code() int }
			if !errors.As(err, &coded) || coded.Code()&255 != 5 {
				t.Fatalf("concurrent writer crossed snapshot fence: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.ExecContext(t.Context(), `UPDATE conversation_send_attempts SET state='reserved'`); err != nil {
				t.Fatalf("snapshot leaked writer lock: %v", err)
			}
		})
	}
}
