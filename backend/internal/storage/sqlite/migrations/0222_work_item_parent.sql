-- +goose Up
-- Absence is a durable legacy HOLD. Never infer or backfill original provenance.
-- References survive session cleanup, just like work_items and their leases.
CREATE TABLE work_item_parents (
    work_item_id TEXT NOT NULL PRIMARY KEY REFERENCES work_items(id),
    attempt_id TEXT NOT NULL UNIQUE,
    original_parent_session_id TEXT NOT NULL CHECK (original_parent_session_id <> ''),
    native_session_id TEXT NOT NULL CHECK (native_session_id <> ''),
    controller_generation TEXT NOT NULL CHECK (controller_generation <> ''),
    provider_conversation_id TEXT NOT NULL CHECK (provider_conversation_id <> ''),
    provider_scope_id TEXT NOT NULL CHECK (provider_scope_id <> ''),
    issued_by_session_id TEXT NOT NULL CHECK (issued_by_session_id = original_parent_session_id),
    issued_by_controller_generation TEXT NOT NULL CHECK (issued_by_controller_generation = controller_generation),
    provenance TEXT NOT NULL CHECK (provenance = 'ao_live_original_parent_controller')
);

-- +goose Down
-- Losing origin could authorize retargeting after rollback.
-- +goose StatementBegin
CREATE TABLE work_item_parent_forward_only_guard (
    blocked INTEGER CONSTRAINT work_item_parent_0222_is_forward_only CHECK (blocked=0)
);
INSERT INTO work_item_parent_forward_only_guard VALUES (1);
-- +goose StatementEnd
