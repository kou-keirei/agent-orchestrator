-- +goose Up
-- +goose StatementBegin
-- No backfill: absent history cannot prove an original native call never ran.
CREATE TABLE conversation_send_dispatch_boundaries (
    turn_id TEXT PRIMARY KEY REFERENCES conversation_turns(id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    branch_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    client_message_id TEXT NOT NULL,
    admission_generation TEXT NOT NULL,
    admitted_at TIMESTAMP,
    call_generation TEXT NOT NULL DEFAULT '',
    call_committed_at TIMESTAMP,
    CHECK ((call_generation = '' AND call_committed_at IS NULL)
        OR (call_generation <> '' AND call_committed_at IS NOT NULL)),
    CHECK (admitted_at IS NOT NULL OR call_committed_at IS NOT NULL)
);

-- Admission identity is immutable; the only update is the one-shot call fence.
CREATE TRIGGER conversation_dispatch_boundary_immutable
BEFORE UPDATE ON conversation_send_dispatch_boundaries
WHEN NEW.turn_id IS NOT OLD.turn_id
  OR NEW.conversation_id IS NOT OLD.conversation_id
  OR NEW.branch_id IS NOT OLD.branch_id
  OR NEW.session_id IS NOT OLD.session_id
  OR NEW.client_message_id IS NOT OLD.client_message_id
  OR NEW.admission_generation IS NOT OLD.admission_generation
  OR NEW.admitted_at IS NOT OLD.admitted_at
  OR OLD.call_committed_at IS NOT NULL
  OR NEW.call_committed_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'immutable ordinary send dispatch boundary');
END;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER conversation_dispatch_boundary_immutable;
DROP TABLE conversation_send_dispatch_boundaries;
-- +goose StatementEnd
