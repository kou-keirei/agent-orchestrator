-- name: CreateWorkItem :exec
INSERT INTO work_items (
    id, attempt_id, project_id, repository, execution_base_sha,
    original_parent_session_id, original_worker_session_id, original_native_session_id,
    owner_session_id, owner_generation, worktree_path, branch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;

-- name: GetWorkItem :one
SELECT * FROM work_items WHERE id = ?;

-- name: ClaimWorkItemOwner :execrows
UPDATE work_items SET owner_session_id = sqlc.arg(next_owner), owner_generation = owner_generation + 1
WHERE id = sqlc.arg(id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND project_id = sqlc.arg(project_id)
  AND repository = sqlc.arg(repository)
  AND execution_base_sha = sqlc.arg(execution_base_sha)
  AND original_parent_session_id = sqlc.arg(original_parent_session_id)
  AND original_worker_session_id = sqlc.arg(original_worker_session_id)
  AND original_native_session_id = sqlc.arg(original_native_session_id)
  AND owner_session_id = sqlc.arg(owner_session_id)
  AND owner_generation = sqlc.arg(owner_generation)
  AND worktree_path = sqlc.arg(worktree_path)
  AND branch = sqlc.arg(branch);
