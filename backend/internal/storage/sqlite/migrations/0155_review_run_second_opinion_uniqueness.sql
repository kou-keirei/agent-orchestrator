-- Config-only reviewer opinions are distinct immutable passes. Keep the
-- existing same-harness idempotency guarantee for the same resolved/requested
-- provenance, while allowing a changed configuration to review the same
-- commit again.

-- +goose Up
-- +goose StatementBegin
DROP INDEX idx_review_run_session_pr_sha_harness;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE UNIQUE INDEX idx_review_run_session_pr_sha_harness
    ON review_run (session_id, pr_url, target_sha, harness, reviewer_provenance_json)
    WHERE target_sha != ''
        AND status NOT IN ('failed', 'cancelled')
        AND (status = 'running' OR verdict NOT IN ('', 'changes_requested'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX idx_review_run_session_pr_sha_harness;
-- +goose StatementEnd

-- The upgraded index permits config-distinct opinions. Before restoring the
-- historical narrower key, retain one deterministic winner for each old key
-- so rollback succeeds on databases that already contain those opinions.
-- Prefer a completed/delivered verdict, then the newest row, matching the
-- repository's earlier review-run downgrade policy.
-- +goose StatementBegin
DELETE FROM review_run
WHERE target_sha != ''
  AND status NOT IN ('failed', 'cancelled')
  AND (status = 'running' OR verdict NOT IN ('', 'changes_requested'))
  AND rowid NOT IN (
    SELECT rowid FROM (
      SELECT rowid,
             ROW_NUMBER() OVER (
               PARTITION BY session_id, pr_url, target_sha, harness
               ORDER BY CASE status WHEN 'complete' THEN 0 WHEN 'delivered' THEN 0 WHEN 'running' THEN 1 ELSE 2 END,
                        created_at DESC,
                        rowid DESC
             ) AS rn
      FROM review_run
      WHERE target_sha != ''
        AND status NOT IN ('failed', 'cancelled')
        AND (status = 'running' OR verdict NOT IN ('', 'changes_requested'))
    )
    WHERE rn = 1
  );
-- +goose StatementEnd

-- +goose StatementBegin
CREATE UNIQUE INDEX idx_review_run_session_pr_sha_harness
    ON review_run (session_id, pr_url, target_sha, harness)
    WHERE target_sha != ''
        AND status NOT IN ('failed', 'cancelled')
        AND (status = 'running' OR verdict NOT IN ('', 'changes_requested'));
-- +goose StatementEnd
