-- +goose Up
-- Pin the resolved session effort so restore and controller transitions do not
-- silently re-resolve a project's later default. The boolean is required
-- because an empty value can mean an intentional provider-default selection.
ALTER TABLE sessions ADD COLUMN effort TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN effort_resolved BOOLEAN NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE sessions DROP COLUMN effort_resolved;
ALTER TABLE sessions DROP COLUMN effort;
