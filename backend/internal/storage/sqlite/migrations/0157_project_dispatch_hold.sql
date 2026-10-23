-- +goose Up
CREATE TABLE project_dispatch_hold (
    project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    held INTEGER NOT NULL CHECK (held IN (0, 1))
);

-- +goose Down
DROP TABLE project_dispatch_hold;
