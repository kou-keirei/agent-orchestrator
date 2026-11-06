-- +goose Up
-- Retain one reservation per admitted WorkItem/attempt, including after release.
-- The partial unique index is a database-wide P0 slot, not a per-Store throttle.
-- Expiry never deletes occupancy or implies native worker death.
CREATE TABLE work_item_admissions (
    work_item_id TEXT NOT NULL PRIMARY KEY REFERENCES work_items(id),
    attempt_id TEXT NOT NULL UNIQUE,
    owner_session_id TEXT NOT NULL,
    owner_generation INTEGER NOT NULL CHECK (typeof(owner_generation) = 'integer' AND owner_generation > 0),
    lease_id TEXT NOT NULL,
    lease_revision INTEGER NOT NULL CHECK (typeof(lease_revision) = 'integer' AND lease_revision > 0),
    state TEXT NOT NULL CHECK (state IN ('reserved', 'released'))
);
CREATE UNIQUE INDEX idx_work_item_admission_capacity_one ON work_item_admissions(state) WHERE state='reserved';

-- +goose Down
-- Dropping retained occupancy could authorize a duplicate worker after rollback.
-- +goose StatementBegin
CREATE TABLE work_item_admission_forward_only_guard (
    blocked INTEGER CONSTRAINT work_item_admission_0221_is_forward_only CHECK (blocked=0)
);
INSERT INTO work_item_admission_forward_only_guard VALUES (1);
-- +goose StatementEnd
