-- name: GetWorkerReport :one
SELECT * FROM worker_reports WHERE id = ?;

-- name: HasWorkerDoneReport :one
SELECT count(*) FROM worker_reports WHERE work_item_id = ? AND attempt_id = ? AND report_type = 'done';

-- name: InsertWorkerReport :exec
INSERT INTO worker_reports (id,work_item_id,attempt_id,owner_generation,report_type,envelope,event_id,source_revision,accepted_at_ms)
VALUES (?,?,?,?,?,?,?,?,?);

-- name: InsertWorkerReportEvent :exec
INSERT INTO orchestration_events (id,project_id,worker_id,kind,source_revision,enqueued_at,next_attempt_at,worker_report_id)
VALUES (?,?,?,'worker_report',?,?,?,?);
