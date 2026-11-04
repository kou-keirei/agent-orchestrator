-- +goose Up
-- Forward-only: immutable reports and pending delivery must survive rollback
-- of application binaries. Do not roll this data back to an older event CHECK.
CREATE TABLE worker_reports (
    id TEXT NOT NULL PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id),
    attempt_id TEXT NOT NULL,
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    report_type TEXT NOT NULL CHECK (report_type IN ('checkpoint','needs_input','stuck','done','failure','artifact')),
    envelope TEXT NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    source_revision TEXT NOT NULL UNIQUE,
    accepted_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_worker_reports_done ON worker_reports(work_item_id, attempt_id) WHERE report_type='done';
-- +goose StatementBegin
CREATE TRIGGER worker_reports_immutable_update BEFORE UPDATE ON worker_reports
BEGIN SELECT RAISE(ABORT, 'immutable worker report'); END;
CREATE TRIGGER worker_reports_immutable_delete BEFORE DELETE ON worker_reports
BEGIN SELECT RAISE(ABORT, 'immutable worker report'); END;
-- +goose StatementEnd

-- 0151 has no inbound FKs or table triggers. Rebuild only its event table;
-- retain every legacy column/value, outbound FK, uniqueness rule and index.
CREATE TABLE orchestration_events_with_reports (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('worker_turn_settled','worker_blocked','worker_ready_to_merge','worker_terminated','pr_merged','worker_report')),
    source_revision TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','leased','submitted','acknowledged','dead_letter')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 8),
    enqueued_at TIMESTAMP NOT NULL,
    next_attempt_at TIMESTAMP NOT NULL,
    lease_token TEXT,
    lease_expires_at TIMESTAMP,
    destination_session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
    submitted_at TIMESTAMP,
    acknowledged_at TIMESTAMP,
    attention_required_at TIMESTAMP,
    last_error TEXT NOT NULL DEFAULT '' CHECK (length(last_error) <= 512),
    worker_report_id TEXT UNIQUE REFERENCES worker_reports(id),
    CHECK ((kind='worker_report' AND worker_report_id IS NOT NULL) OR (kind<>'worker_report' AND worker_report_id IS NULL)),
    UNIQUE(project_id, worker_id, kind, source_revision)
);
INSERT INTO orchestration_events_with_reports
    (id,project_id,worker_id,kind,source_revision,state,attempt_count,enqueued_at,next_attempt_at,lease_token,lease_expires_at,destination_session_id,submitted_at,acknowledged_at,attention_required_at,last_error)
SELECT id,project_id,worker_id,kind,source_revision,state,attempt_count,enqueued_at,next_attempt_at,lease_token,lease_expires_at,destination_session_id,submitted_at,acknowledged_at,attention_required_at,last_error FROM orchestration_events;
DROP TABLE orchestration_events;
ALTER TABLE orchestration_events_with_reports RENAME TO orchestration_events;
CREATE INDEX idx_orchestration_events_due ON orchestration_events(state, next_attempt_at, enqueued_at);
CREATE INDEX idx_orchestration_events_project ON orchestration_events(project_id, state, enqueued_at);
-- +goose StatementBegin
CREATE TRIGGER orchestration_worker_report_binding BEFORE INSERT ON orchestration_events
WHEN NEW.kind='worker_report'
BEGIN
    SELECT RAISE(ABORT, 'worker report event binding') WHERE NOT EXISTS (
        SELECT 1 FROM worker_reports r WHERE r.id=NEW.worker_report_id
        AND r.event_id=NEW.id AND r.source_revision=NEW.source_revision
        AND json_extract(r.envelope,'$.Identity.ProjectID')=NEW.project_id
        AND json_extract(r.envelope,'$.WorkerSessionID')=NEW.worker_id
    );
END;
CREATE TRIGGER orchestration_worker_report_immutable BEFORE UPDATE ON orchestration_events
WHEN OLD.worker_report_id IS NOT NULL OR NEW.worker_report_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'immutable worker report event source')
    WHERE NEW.id IS NOT OLD.id OR NEW.project_id IS NOT OLD.project_id
       OR NEW.worker_id IS NOT OLD.worker_id OR NEW.kind IS NOT OLD.kind
       OR NEW.source_revision IS NOT OLD.source_revision OR NEW.worker_report_id IS NOT OLD.worker_report_id;
END;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE worker_reports_forward_only_guard (
    blocked INTEGER CONSTRAINT worker_reports_0220_is_forward_only CHECK (blocked=0)
);
INSERT INTO worker_reports_forward_only_guard VALUES (1);
-- +goose StatementEnd
