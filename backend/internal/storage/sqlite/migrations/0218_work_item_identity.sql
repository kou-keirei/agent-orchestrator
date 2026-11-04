-- +goose Up
-- One current attempt/owner per logical WorkItem. These references deliberately
-- survive session/project cleanup; they are authorization facts, not live probes.
-- Versions 157-169 belong to the desktop journal; 170-217 also occur in local
-- migration history. Do not reuse the historical WorkItem migration 0182.
CREATE TABLE work_items (
    id TEXT NOT NULL PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    execution_base_sha TEXT NOT NULL,
    original_parent_session_id TEXT NOT NULL,
    original_worker_session_id TEXT NOT NULL,
    original_native_session_id TEXT NOT NULL DEFAULT '',
    owner_session_id TEXT NOT NULL,
    owner_generation INTEGER NOT NULL CHECK (typeof(owner_generation) = 'integer' AND owner_generation > 0),
    worktree_path TEXT NOT NULL UNIQUE,
    branch TEXT NOT NULL
);

-- +goose Down
DROP TABLE work_items;
