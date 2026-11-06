-- name: ListStorageWorkspaces :many
-- Prefer registered per-repository worktrees; fall back to the session workspace.
-- Durable identity only: no filesystem, Git cleanliness, or backup claims.
SELECT 'workspace:' || hex(s.id) || ':' || hex(COALESCE(w.repo_name, '')) AS id,
       s.id AS session_id, COALESCE(w.worktree_path, s.workspace_path) AS path,
       s.cleanup_generation, s.revision, s.is_pinned
FROM sessions s LEFT JOIN session_worktrees w ON w.session_id = s.id
WHERE COALESCE(w.worktree_path, s.workspace_path, '') <> ''
  AND ('workspace:' || hex(s.id) || ':' || hex(COALESCE(w.repo_name, ''))) > sqlc.arg(after_id)
ORDER BY 1
LIMIT sqlc.arg(page_limit);
