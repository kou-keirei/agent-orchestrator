-- +goose Up
ALTER TABLE sessions ADD COLUMN terminal_revision INTEGER NOT NULL DEFAULT 0;

-- A first terminal episode can preserve its earliest recorded canonical
-- revision, or use the durable upgrade-time revision if its event was lost.
-- For restored legacy rows the old source states do not identify which episode
-- they belong to. Keep that revision unknown (-1), rather than reuse stale
-- identity. Runtime/controller identities remain authoritative when present.
DROP TRIGGER sessions_revision_update;
UPDATE sessions SET terminal_revision = CASE WHEN cleanup_generation > 0 THEN -1 ELSE COALESCE(
    (SELECT MIN(CAST(substr(source_id, 19) AS INTEGER))
     FROM orchestration_source_states
     WHERE worker_id = sessions.id AND project_id = sessions.project_id
       AND kind = 'worker_terminated'
       AND source_id GLOB 'terminal-revision:[0-9]*'
       AND substr(source_id, 19) NOT GLOB '*[^0-9]*'
       AND source_id = 'terminal-revision:' || CAST(CAST(substr(source_id, 19) AS INTEGER) AS TEXT)
       AND CAST(substr(source_id, 19) AS INTEGER) <= sessions.revision),
    revision) END
WHERE is_terminated = 1;

-- Keep both facts in the existing database-owned revision write. The guard
-- prevents recursion, and the internal update does not emit an extra CDC fact.
-- +goose StatementBegin
CREATE TRIGGER sessions_revision_update
AFTER UPDATE ON sessions
WHEN NEW.revision = OLD.revision
BEGIN
    UPDATE sessions SET revision = OLD.revision + 1,
        terminal_revision = CASE
            WHEN OLD.is_terminated = 0 AND NEW.is_terminated = 1 THEN OLD.revision + 1
            WHEN NEW.is_terminated = 0 THEN 0
            ELSE OLD.terminal_revision
        END
    WHERE id = NEW.id;
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER sessions_revision_update;
ALTER TABLE sessions DROP COLUMN terminal_revision;
-- +goose StatementBegin
CREATE TRIGGER sessions_revision_update
AFTER UPDATE ON sessions
WHEN NEW.revision = OLD.revision
BEGIN
    UPDATE sessions SET revision = OLD.revision + 1 WHERE id = NEW.id;
END;
-- +goose StatementEnd
