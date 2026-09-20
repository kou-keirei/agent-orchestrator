-- +goose Up
-- Persist the exact commit authorized for session execution separately from
-- comparison-only diff-base metadata.
ALTER TABLE sessions ADD COLUMN execution_base_sha TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE sessions DROP COLUMN execution_base_sha;
