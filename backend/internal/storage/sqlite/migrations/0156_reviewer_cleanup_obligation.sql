-- Reviewer restore cleanup is durable even when no ReviewRun was attributable.
-- The row is cleanup evidence only; it must never become reviewer authority.

-- +goose Up
CREATE TABLE reviewer_cleanup_obligation (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    review_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    harness TEXT NOT NULL,
    handle_id TEXT NOT NULL,
    launch_id TEXT NOT NULL DEFAULT '',
    agent_session_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unresolved'
        CHECK (status IN ('unresolved', 'cleared')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

CREATE INDEX idx_reviewer_cleanup_obligation_unresolved
    ON reviewer_cleanup_obligation (session_id, created_at, id)
    WHERE status = 'unresolved';

-- +goose Down
DROP INDEX idx_reviewer_cleanup_obligation_unresolved;
DROP TABLE reviewer_cleanup_obligation;
