package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/pressly/goose/v3"
)

// reconcileDesktop169 repairs the observed foreign desktop journal without
// deleting, renumbering or relabelling its claims as this source's migrations.
// Goose still owns SQL parsing and per-migration transactions. Only wholly
// absent effects of the unchanged embedded migrations are eligible for replay.
// Inspect the entire plan before writing; partial/colliding shapes need review.
func reconcileDesktop169(ctx context.Context, db *sql.DB) error {
	var journal int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name='goose_db_version' AND type='table'`).Scan(&journal); err != nil || journal == 0 {
		return err
	}
	version, err := goose.GetDBVersionContext(ctx, db)
	if err != nil || version <= 156 {
		return err
	}
	for v := 150; v <= 156; v++ {
		var applied int
		if err := db.QueryRowContext(ctx, `SELECT COALESCE((SELECT is_applied FROM goose_db_version WHERE version_id=? ORDER BY id DESC LIMIT 1),0)`, v).Scan(&applied); err != nil {
			return err
		}
		if applied != 1 {
			return fmt.Errorf("desktop schema compatibility: out-of-order or rolled-back journal at %d; manual lineage review required", v)
		}
	}
	plan := map[int64]bool{}
	for _, feature := range []struct {
		version  int64
		table    string
		columns  []string
		newTable bool
	}{
		{150, "sessions", []string{"hidden_at"}, false},
		{151, "orchestration_events", []string{"id", "project_id", "worker_id", "kind", "source_revision", "state", "attempt_count", "enqueued_at", "next_attempt_at", "lease_token", "lease_expires_at", "destination_session_id", "submitted_at", "acknowledged_at", "attention_required_at", "last_error"}, true},
		{151, "orchestration_source_states", []string{"project_id", "worker_id", "kind", "source_id", "active", "generation", "updated_at"}, true},
		{153, "sessions", []string{"terminal_revision"}, false},
		{154, "review_run", []string{"target_binding_json", "reviewer_provenance_json", "launch_id", "provider_native_session_id", "provider_native_first_turn_id", "provider_native_completion_id", "provider_native_observed_at"}, false},
		{156, "reviewer_cleanup_obligation", []string{"id", "session_id", "review_id", "run_id", "harness", "handle_id", "launch_id", "agent_session_id", "status", "created_at", "updated_at", "resolved_at"}, true},
	} {
		var present, tableExists int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, feature.table).Scan(&tableExists); err != nil {
			return err
		}
		if tableExists == 0 && !feature.newTable {
			return fmt.Errorf("desktop schema compatibility: missing prerequisite table %s", feature.table)
		}
		for _, column := range feature.columns {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, feature.table, column).Scan(&n); err != nil {
				return err
			}
			present += n
		}
		if (present != 0 && present != len(feature.columns)) || (feature.newTable && tableExists != 0 && present == 0) {
			return fmt.Errorf("desktop schema compatibility: partial or foreign %s shape for migration %d", feature.table, feature.version)
		}
		missing := present == 0
		if !missing && !feature.newTable {
			for _, column := range feature.columns {
				wantType, wantNotNull := "TEXT", 1
				if column == "hidden_at" || column == "provider_native_observed_at" {
					wantType, wantNotNull = "TIMESTAMP", 0
				}
				if column == "terminal_revision" {
					wantType = "INTEGER"
				}
				var gotType string
				var gotNotNull int
				if err := db.QueryRowContext(ctx, `SELECT upper(type), "notnull" FROM pragma_table_info(?) WHERE name=?`, feature.table, column).Scan(&gotType, &gotNotNull); err != nil {
					return err
				}
				if gotType != wantType || gotNotNull != wantNotNull {
					return fmt.Errorf("desktop schema compatibility: foreign %s.%s declaration", feature.table, column)
				}
			}
		}
		if prior, exists := plan[feature.version]; exists && prior != missing {
			return fmt.Errorf("desktop schema compatibility: partial migration %d", feature.version)
		}
		plan[feature.version] = missing
	}
	for _, object := range []struct {
		version int64
		names   []string
	}{
		{151, []string{"idx_orchestration_events_due", "idx_orchestration_events_project"}},
		{154, []string{"review_run_immutable_binding"}},
		{156, []string{"idx_reviewer_cleanup_obligation_unresolved"}},
	} {
		if plan[object.version] {
			continue
		}
		for _, name := range object.names {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name=?`, name).Scan(&n); err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("desktop schema compatibility: partial migration %d, missing %s", object.version, name)
			}
		}
	}
	if !plan[153] {
		var trigger string
		if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE name='sessions_revision_update' AND type='trigger'`).Scan(&trigger); err != nil {
			return err
		}
		if !strings.Contains(trigger, "terminal_revision = CASE") || !strings.Contains(trigger, "OLD.terminal_revision") {
			return fmt.Errorf("desktop schema compatibility: terminal column exists without its revision trigger; refusing partial repair")
		}
	}
	// A table copy must never discard foreign columns, triggers, indexes or
	// incoming references. The known four-type notifications schema has none.
	var notificationSQL string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='notifications'`).Scan(&notificationSQL); err != nil {
		return err
	}
	plan[152] = !strings.Contains(notificationSQL, "'orchestration_attention'")
	if plan[152] {
		var columns, foreignObjects, incoming int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('notifications')`).Scan(&columns); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE tbl_name='notifications' AND ((type='trigger') OR (type='index' AND sql IS NOT NULL AND name NOT IN ('idx_notifications_status_history','idx_notifications_history','idx_notifications_open_dedupe','idx_notifications_unresolved')))`).Scan(&foreignObjects); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master m, pragma_foreign_key_list(m.name) f WHERE m.type='table' AND f."table"='notifications'`).Scan(&incoming); err != nil {
			return err
		}
		if columns != 10 || foreignObjects != 0 || incoming != 0 {
			return fmt.Errorf("desktop schema compatibility: foreign notifications shape; refusing table copy")
		}
		rows, err := db.QueryContext(ctx, `SELECT id,session_id,project_id,pr_url,type,title,body,status,created_at,resolved_at FROM notifications LIMIT 0`)
		if err != nil {
			return err
		}
		_ = rows.Close()
	}
	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_index_info('idx_review_run_session_pr_sha_harness') ORDER BY seqno`)
	if err != nil {
		return err
	}
	var indexColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return err
		}
		indexColumns = append(indexColumns, name)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	switch strings.Join(indexColumns, ",") {
	case "session_id,pr_url,target_sha,harness":
		plan[155] = true
	case "session_id,pr_url,target_sha,harness,reviewer_provenance_json":
		plan[155] = false
	default:
		return fmt.Errorf("desktop schema compatibility: foreign review-run uniqueness index")
	}
	var needed bool
	for _, missing := range plan {
		needed = needed || missing
	}
	if !needed {
		return nil
	}
	if version != 169 {
		return fmt.Errorf("desktop schema compatibility: unqualified foreign journal version %d; expected observed lineage 169", version)
	}
	// The physical terminal backfill requires the original revision fence.
	if plan[153] {
		var trigger string
		if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE name='sessions_revision_update' AND type='trigger'`).Scan(&trigger); err != nil {
			return err
		}
		if normalizeSQLiteTriggerSQL(trigger) != normalizeSQLiteTriggerSQL(`CREATE TRIGGER sessions_revision_update AFTER UPDATE ON sessions WHEN NEW.revision = OLD.revision BEGIN UPDATE sessions SET revision = OLD.revision + 1 WHERE id = NEW.id; END`) {
			return fmt.Errorf("desktop schema compatibility: foreign session revision trigger")
		}
	}
	migrationDir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	entries, err := fs.ReadDir(migrationDir, ".")
	if err != nil {
		return err
	}
	var excluded []string
	for _, entry := range entries {
		v, err := strconv.ParseInt(strings.SplitN(entry.Name(), "_", 2)[0], 10, 64)
		if err != nil {
			return err
		}
		if !plan[v] {
			excluded = append(excluded, entry.Name())
		}
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationDir, goose.WithExcludeNames(excluded), goose.WithDisableVersioning(true), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("desktop schema compatibility: replay absent source effects: %w", err)
	}
	return nil
}
