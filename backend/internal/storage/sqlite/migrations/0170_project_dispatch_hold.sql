-- Versions 157-169 belong to the foreign desktop lineage.
-- +goose Up
CREATE TABLE IF NOT EXISTS project_dispatch_hold (
    project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    held INTEGER NOT NULL CHECK (held IN (0, 1))
);

-- +goose Down
DROP TABLE project_dispatch_hold;
