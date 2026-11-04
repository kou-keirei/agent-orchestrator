-- name: LockWorkItemForLease :exec
-- Acquire SQLite's writer reservation before reading across independent opens.
UPDATE work_items SET id = id WHERE id = ?;

-- name: GetWorkItemLease :one
SELECT * FROM work_item_leases WHERE work_item_id = ?;

-- name: CreateWorkItemLease :exec
INSERT INTO work_item_leases(work_item_id, owner_generation, revision, lease_id, duration_ms, expires_at_ms, allowed_effects, state)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: CASWorkItemLease :execrows
UPDATE work_item_leases
SET owner_generation = sqlc.arg(owner_generation), revision = revision + 1,
    lease_id = sqlc.arg(lease_id), duration_ms = sqlc.arg(duration_ms),
    expires_at_ms = sqlc.arg(expires_at_ms), allowed_effects = sqlc.arg(allowed_effects), state = sqlc.arg(state)
WHERE work_item_id = sqlc.arg(work_item_id) AND revision = sqlc.arg(expected_revision);
