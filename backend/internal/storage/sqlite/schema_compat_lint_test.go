package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestDesktop169ViewPrerequisites(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		reject    bool
	}{
		{"quoted name", `CREATE VIEW "odd""; SELECT 1;--" AS SELECT 1 AS value`, false},
		{"view chain", `CREATE VIEW first_view AS SELECT 1 AS value; CREATE VIEW second_view AS SELECT * FROM first_view`, false},
		{"missing column", `CREATE TABLE source(id); CREATE VIEW broken AS SELECT absent FROM source`, true},
		{"broken chain", `CREATE VIEW first_view AS SELECT * FROM missing; CREATE VIEW second_view AS SELECT * FROM first_view`, true},
		{"circular view", `CREATE VIEW circular AS SELECT * FROM circular`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(tc.ddl); err != nil {
				t.Fatal(err)
			}
			err := checkDesktop169NotificationDependencies(context.Background(), db)
			if (err != nil) != tc.reject {
				t.Fatalf("reject=%v, got %v", tc.reject, err)
			}
		})
	}
}

func TestDesktop169DependencyCloseError(t *testing.T) {
	for _, dependency := range []bool{false, true} {
		t.Run(map[bool]string{false: "exhausted", true: "early rejection"}[dependency], func(t *testing.T) {
			closeErr := errors.New("metadata close failed")
			rows := &compatCloseRows{closeErr: closeErr, dependency: dependency}
			db := sql.OpenDB(compatCloseConnector{rows: rows})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			err := checkDesktop169NotificationDependencies(context.Background(), db)
			if !errors.Is(err, closeErr) {
				t.Fatalf("close error lost: %v", err)
			}
			if dependency && !strings.Contains(err.Error(), "possible notifications dependency") {
				t.Fatalf("rejection lost: %v", err)
			}
			if !rows.closed {
				t.Fatal("metadata rows not closed")
			}
		})
	}
}

type compatCloseConnector struct{ rows *compatCloseRows }

func (c compatCloseConnector) Connect(context.Context) (driver.Conn, error) {
	return compatCloseConn(c), nil
}
func (c compatCloseConnector) Driver() driver.Driver { return compatCloseDriver{connector: c} }

type compatCloseDriver struct{ connector compatCloseConnector }

func (d compatCloseDriver) Open(string) (driver.Conn, error) {
	return d.connector.Connect(context.Background())
}

type compatCloseConn struct{ rows *compatCloseRows }

func (c compatCloseConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c compatCloseConn) Close() error              { return nil }
func (c compatCloseConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c compatCloseConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}

type compatCloseRows struct {
	closeErr                 error
	dependency, sent, closed bool
}

func (r *compatCloseRows) Columns() []string { return []string{"type", "name", "sql"} }
func (r *compatCloseRows) Close() error      { r.closed = true; return r.closeErr }
func (r *compatCloseRows) Next(values []driver.Value) error {
	if !r.dependency || r.sent {
		return io.EOF
	}
	r.sent = true
	values[0], values[1], values[2] = "view", "dependent", "CREATE VIEW dependent AS SELECT * FROM notifications"
	return nil
}
