package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"regexp"
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
		if !feature.newTable {
			var ordinary int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_list WHERE schema='main' AND name=? AND type='table' AND wr=0 AND strict=0`, feature.table).Scan(&ordinary); err != nil {
				return err
			}
			if ordinary != 1 {
				return fmt.Errorf("desktop schema compatibility: foreign %s storage options", feature.table)
			}
		}
		for _, column := range feature.columns {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_xinfo(?) WHERE name=? COLLATE NOCASE`, feature.table, column).Scan(&n); err != nil {
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
				var gotNotNull, hidden int
				if err := db.QueryRowContext(ctx, `SELECT upper(type), "notnull",hidden FROM pragma_table_xinfo(?) WHERE name=? COLLATE NOCASE`, feature.table, column).Scan(&gotType, &gotNotNull, &hidden); err != nil {
					return err
				}
				if gotType != wantType || gotNotNull != wantNotNull || hidden != 0 {
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
		var ordinary int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_list WHERE schema='main' AND name='notifications' AND type='table' AND wr=0 AND strict=0`).Scan(&ordinary); err != nil {
			return err
		}
		if ordinary != 1 {
			return fmt.Errorf("desktop schema compatibility: foreign notifications storage options")
		}
		// Names alone do not establish copy safety: affinity can change values,
		// and the replacement can silently discard constraints/storage options.
		// Qualify only the complete observed declaration, not SQL equivalence.
		const declaration = `(id TEXT PRIMARY KEY,
session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
pr_url TEXT NOT NULL DEFAULT '',
type TEXT NOT NULL CHECK(type IN ('needs_input','ready_to_merge','pr_merged','pr_closed_unmerged')),
title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '',
status TEXT NOT NULL DEFAULT 'unread' CHECK(status IN ('read','unread')),
created_at TIMESTAMP NOT NULL, resolved_at TIMESTAMP)`
		_, body, found := strings.Cut(notificationSQL, "(")
		if !found || desktop169SQL("("+body) != desktop169SQL(declaration) {
			return fmt.Errorf("desktop schema compatibility: foreign notifications declaration; refusing lossy table replacement")
		}
		// Migration 152 copies SELECT * positionally. table_info alone hides
		// generated columns and cannot establish the physical copy order.
		wantColumns := []string{"id", "session_id", "project_id", "pr_url", "type", "title", "body", "status", "created_at", "resolved_at"}
		rows, err := db.QueryContext(ctx, `SELECT cid,name,hidden FROM pragma_table_xinfo('notifications') ORDER BY cid`)
		if err != nil {
			return err
		}
		columnCount, ordered := 0, true
		for rows.Next() {
			var cid, hidden int
			var name string
			if err := rows.Scan(&cid, &name, &hidden); err != nil {
				_ = rows.Close()
				return err
			}
			if columnCount >= len(wantColumns) || cid != columnCount || hidden != 0 || name != wantColumns[columnCount] {
				ordered = false
			}
			columnCount++
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
		if !ordered || columnCount != len(wantColumns) {
			return fmt.Errorf("desktop schema compatibility: notifications physical column order differs; refusing positional copy")
		}
		if err := checkDesktop169NotificationIndexes(ctx, db); err != nil {
			return err
		}
		if err := checkDesktop169NotificationDependencies(ctx, db); err != nil {
			return err
		}
		var foreignObjects, incoming int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE tbl_name='notifications' AND ((type='trigger') OR (type='index' AND sql IS NOT NULL AND name NOT IN ('idx_notifications_status_history','idx_notifications_history','idx_notifications_open_dedupe','idx_notifications_unresolved')))`).Scan(&foreignObjects); err != nil {
			return err
		}
		// SQLite identifiers fold ASCII case, even for quoted REFERENCES names.
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master m, pragma_foreign_key_list(m.name) f WHERE m.type='table' AND f."table"='notifications' COLLATE NOCASE`).Scan(&incoming); err != nil {
			return err
		}
		if foreignObjects != 0 || incoming != 0 {
			return fmt.Errorf("desktop schema compatibility: foreign notifications shape; refusing table copy")
		}
	}
	var indexCount, ownedIndex, uniqueIndex, partialIndex int
	var indexSQL string
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(type='index' AND tbl_name='review_run' COLLATE NOCASE),0),COALESCE(MAX(sql),'') FROM sqlite_master WHERE name='idx_review_run_session_pr_sha_harness' COLLATE NOCASE`).Scan(&indexCount, &ownedIndex, &indexSQL); err != nil {
		return err
	}
	if indexCount != 1 || ownedIndex != 1 {
		return fmt.Errorf("desktop schema compatibility: foreign review-run index owner")
	}
	if err := db.QueryRowContext(ctx, `SELECT "unique",partial FROM pragma_index_list('review_run') WHERE name='idx_review_run_session_pr_sha_harness' COLLATE NOCASE`).Scan(&uniqueIndex, &partialIndex); err != nil {
		return err
	}
	if uniqueIndex != 1 || partialIndex != 1 {
		return fmt.Errorf("desktop schema compatibility: review-run index must be unique and partial")
	}
	// Accept the canonical predicate spelling with whitespace differences only.
	// Do not lowercase SQL string literals or attempt general SQL equivalence.
	where := strings.Index(strings.ToUpper(indexSQL), "WHERE")
	const predicate = "target_sha != '' AND status NOT IN ('failed', 'cancelled') AND (status = 'running' OR verdict NOT IN ('', 'changes_requested'))"
	if where < 0 || strings.Join(strings.Fields(strings.TrimSuffix(strings.TrimSpace(indexSQL[where+len("WHERE"):]), ";")), " ") != predicate {
		return fmt.Errorf("desktop schema compatibility: foreign review-run index predicate")
	}
	var foreignKeys int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_xinfo('idx_review_run_session_pr_sha_harness') WHERE "key"=1 AND ("desc"!=0 OR COALESCE(coll,'')!='BINARY')`).Scan(&foreignKeys); err != nil {
		return err
	}
	if foreignKeys != 0 {
		return fmt.Errorf("desktop schema compatibility: foreign review-run index key semantics")
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
	// Reserve every explicit destination of the selected Up SQL before the
	// first transaction. DROP/replace names may already exist only on their
	// intended owner. New names must be free across SQLite object namespaces.
	for _, destination := range []struct {
		version           int64
		name, kind, owner string
		replace           bool
	}{
		{150, "sessions_cdc_update", "trigger", "sessions", true},
		{151, "orchestration_events", "table", "orchestration_events", false},
		{151, "orchestration_source_states", "table", "orchestration_source_states", false},
		{151, "idx_orchestration_events_due", "index", "orchestration_events", false},
		{151, "idx_orchestration_events_project", "index", "orchestration_events", false},
		{152, "notifications_before_orchestration_attention", "table", "notifications_before_orchestration_attention", false},
		{152, "notifications", "table", "notifications", true},
		{152, "idx_notifications_open_dedupe", "index", "notifications", true},
		{152, "idx_notifications_unresolved", "index", "notifications", true},
		{152, "idx_notifications_status_history", "index", "notifications", true},
		{152, "idx_notifications_history", "index", "notifications", true},
		{153, "sessions_revision_update", "trigger", "sessions", true},
		{154, "review_run_immutable_binding", "trigger", "review_run", false},
		{155, "idx_review_run_session_pr_sha_harness", "index", "review_run", true},
		{156, "reviewer_cleanup_obligation", "table", "reviewer_cleanup_obligation", false},
		{156, "idx_reviewer_cleanup_obligation_unresolved", "index", "reviewer_cleanup_obligation", false},
	} {
		if destination.name == "notifications_before_orchestration_attention" && !plan[152] {
			continue
		}
		var count, foreign int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(NOT(type=? AND tbl_name=? COLLATE NOCASE)),0) FROM sqlite_master WHERE name=? COLLATE NOCASE`, destination.kind, destination.owner, destination.name).Scan(&count, &foreign); err != nil {
			return err
		}
		if foreign != 0 || count > 1 || (plan[destination.version] && !destination.replace && count != 0) {
			return fmt.Errorf("desktop schema compatibility: replay destination %s is already owned by another schema effect", destination.name)
		}
	}
	var needed bool
	for _, missing := range plan {
		needed = needed || missing
	}
	// The later schema reconciliation also replaces CDC on restart. Validate
	// its known owner/behavior even when no historical SQL needs replay.
	if err := checkDesktop169SessionTriggers(ctx, db, plan[150], plan[153], version); err != nil {
		return err
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

// Token equality is a conservative declaration allowlist, not a SQL parser or
// an equivalence claim. Preserve quoted bytes and identifier/token boundaries;
// only whitespace between tokens and a final semicolon are insignificant.
var desktop169Tokens = regexp.MustCompile(`'(?:[^']|'')*'|"(?:[^"]|"")*"|` + "`(?:[^`]|``)*`" + `|\[[^]]*\]|[[:alnum:]_]+|[^[:space:]]`)

func desktop169SQL(sqlText string) string {
	return strings.Join(desktop169Tokens.FindAllString(strings.TrimSuffix(strings.TrimSpace(sqlText), ";"), -1), "\x00")
}

func checkDesktop169NotificationIndexes(ctx context.Context, db *sql.DB) error {
	// Include implicit constraints. Only the canonical id primary-key index
	// and these four exact explicit indexes survive a canonical replacement.
	want := map[string]string{
		"idx_notifications_status_history": `CREATE INDEX idx_notifications_status_history ON notifications(status,created_at DESC,id DESC)`,
		"idx_notifications_history":        `CREATE INDEX idx_notifications_history ON notifications(created_at DESC,id DESC)`,
		"idx_notifications_open_dedupe":    `CREATE UNIQUE INDEX idx_notifications_open_dedupe ON notifications(session_id,type,pr_url) WHERE status='unread' OR resolved_at IS NULL`,
		"idx_notifications_unresolved":     `CREATE INDEX idx_notifications_unresolved ON notifications(resolved_at,created_at DESC,id DESC)`,
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('notifications')`).Scan(&count); err != nil {
		return err
	}
	if count != len(want)+1 {
		return fmt.Errorf("desktop schema compatibility: foreign notifications implicit/explicit indexes")
	}
	var primaryKeys int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_index_list('notifications') WHERE origin='pk' AND "unique"=1 AND partial=0`).Scan(&primaryKeys); err != nil {
		return err
	}
	if primaryKeys != 1 {
		return fmt.Errorf("desktop schema compatibility: foreign notifications primary key")
	}
	for name, ddl := range want {
		var actual string
		if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='notifications' AND name=?`, name).Scan(&actual); err != nil {
			return fmt.Errorf("desktop schema compatibility: inspect notifications index %s: %w", name, err)
		}
		if desktop169SQL(actual) != desktop169SQL(ddl) {
			return fmt.Errorf("desktop schema compatibility: foreign notifications index %s", name)
		}
	}
	return nil
}

func checkDesktop169NotificationDependencies(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT type,name,sql FROM sqlite_master WHERE type IN ('view','trigger')`)
	if err != nil {
		return err
	}
	var views []string
	for rows.Next() {
		var kind, name, ddl string
		if err := rows.Scan(&kind, &name, &ddl); err != nil {
			_ = rows.Close()
			return err
		}
		// Conservative text containment covers quoted/case-folded references,
		// including triggers on other owners and the first view in a chain.
		// False positives (e.g. literals/comments) require lineage review.
		if strings.Contains(strings.ToLower(ddl), "notifications") {
			_ = rows.Close()
			return fmt.Errorf("desktop schema compatibility: possible notifications dependency in %s %s", kind, name)
		}
		if kind == "view" {
			views = append(views, name)
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	// SQLite's ALTER TABLE validates unrelated views too. Prepare each without
	// reading rows so an existing broken view cannot cause a late replay error.
	for _, name := range views {
		rows, err := db.QueryContext(ctx, `SELECT * FROM "`+strings.ReplaceAll(name, `"`, `""`)+`" LIMIT 0`)
		if err != nil {
			return fmt.Errorf("desktop schema compatibility: invalid prerequisite view %s: %w", name, err)
		}
		_ = rows.Close()
	}
	return nil
}

func checkDesktop169SessionTriggers(ctx context.Context, db *sql.DB, hiddenMissing, terminalMissing bool, journalVersion int64) error {
	var temporary int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_temp_master`).Scan(&temporary); err != nil {
		return err
	}
	if temporary != 0 {
		return fmt.Errorf("desktop schema compatibility: unqualified connection-local schema dependencies")
	}
	var canonical string
	for _, repair := range schemaRepairs {
		if repair.version == 150 {
			for _, statement := range repair.postAdd {
				if strings.HasPrefix(statement, "CREATE TRIGGER sessions_cdc_update") {
					canonical = statement
				}
			}
		}
	}
	if canonical == "" {
		return fmt.Errorf("desktop schema compatibility: missing canonical session CDC definition")
	}
	// Migration 150 replaces the entire trigger without provisioning CDC, even
	// when the foreign desktop column already exists. The observed 169 lineage
	// can therefore legitimately have this exact definition. The existing
	// reconcileSessionHiddenCDCTrigger transaction upgrades it after preflight
	// and historical replay; no row backfill or second CDC emitter is needed.
	historical150 := canonical
	var provisioning int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name='provision_state'`).Scan(&provisioning); err != nil {
		return err
	}
	if hiddenMissing {
		canonical = strings.Replace(canonical, "OR OLD.hidden_at IS NOT NEW.hidden_at", "", 1)
		canonical = strings.Replace(canonical, ",\n            'hiddenAt', NEW.hidden_at", "", 1)
		if provisioning != 0 {
			canonical = strings.Replace(canonical, "OR OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at", "OR OLD.latest_user_prompt_at IS NOT NEW.latest_user_prompt_at OR OLD.provision_state <> NEW.provision_state", 1)
			canonical = strings.Replace(canonical, "'autoReviewEnabled', json(CASE WHEN NEW.auto_review_enabled THEN 'true' ELSE 'false' END)", "'autoReviewEnabled', json(CASE WHEN NEW.auto_review_enabled THEN 'true' ELSE 'false' END), 'provisionState', NEW.provision_state", 1)
		}
	} else if provisioning != 0 {
		canonical = strings.Replace(canonical, "OR OLD.hidden_at IS NOT NEW.hidden_at", "OR OLD.hidden_at IS NOT NEW.hidden_at OR OLD.provision_state <> NEW.provision_state", 1)
		canonical = strings.Replace(canonical, "'hiddenAt', NEW.hidden_at", "'hiddenAt', NEW.hidden_at, 'provisionState', NEW.provision_state", 1)
	}
	var actual string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name='sessions' AND name='sessions_cdc_update'`).Scan(&actual); err != nil {
		return err
	}
	knownHistorical := journalVersion == 169 && !hiddenMissing && provisioning != 0 && desktop169SQL(actual) == desktop169SQL(historical150)
	if desktop169SQL(actual) != desktop169SQL(canonical) && !knownHistorical {
		return fmt.Errorf("desktop schema compatibility: foreign session CDC behavior; refusing replacement")
	}
	if !terminalMissing {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='sessions' AND name NOT IN ('sessions_cdc_update','sessions_revision_update')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			return err
		}
		// These native headers cannot fire on the terminal_revision backfill.
		if strings.HasPrefix(desktop169SQL(ddl), desktop169SQL(`CREATE TRIGGER sessions_cdc_insert AFTER INSERT ON sessions BEGIN`)) ||
			strings.HasPrefix(desktop169SQL(ddl), desktop169SQL(`CREATE TRIGGER conversation_branch_root_provider_update AFTER UPDATE OF provider_conversation_id ON sessions`)+"\x00") {
			continue
		}
		return fmt.Errorf("desktop schema compatibility: unqualified session backfill trigger %s", name)
	}
	return rows.Err()
}
