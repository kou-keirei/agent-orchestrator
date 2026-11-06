package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// ErrFinalizeSourceUnavailable distinguishes unavailable or invalid enumeration
// from a supported, successfully observed empty set.
var ErrFinalizeSourceUnavailable = errors.New("finalize source unavailable")

// ErrFinalizeSourceOverflow means the complete set exceeds the inclusive bound.
var ErrFinalizeSourceOverflow = errors.New("finalize source exceeds bound")

// FinalizeSourceUnavailableError preserves the source gap and underlying SQL or
// context error. No partial inventory is returned with this error.
type FinalizeSourceUnavailableError struct {
	Source string
	Err    error
}

func (e *FinalizeSourceUnavailableError) Error() string {
	return fmt.Sprintf("finalize %s source unavailable: %v", e.Source, e.Err)
}

func (e *FinalizeSourceUnavailableError) Unwrap() error { return e.Err }

// Is supports errors.Is while Unwrap retains cancellation and overflow causes.
func (e *FinalizeSourceUnavailableError) Is(target error) bool {
	return target == ErrFinalizeSourceUnavailable
}

// These are static, parameterized identity reads. The extra row is only an
// overflow sentinel: full records are not hydrated until enumeration succeeds.
const finalizeSessionIDsSQL = `SELECT id FROM sessions ORDER BY id COLLATE BINARY LIMIT ?`
const finalizePRIDsSQL = `SELECT url FROM pr WHERE session_id = ? ORDER BY url COLLATE BINARY LIMIT ?`
const finalizeWorktreeIDsSQL = `SELECT repo_name FROM session_worktrees WHERE session_id = ?
ORDER BY worktree_path COLLATE BINARY, repo_name COLLATE BINARY LIMIT ?`

// ListFinalizeSessions implements the bounded FINALIZE source contract without
// depending on service/session. It includes all sessions, even when cleanup
// facts are absent. Each call has one SQLite snapshot; later GetSession and
// GetSessionCleanupFacts readbacks remain necessary. This confers no APPLY or
// native release qualification. Bounds limit row cardinality, not field bytes.
func (s *Store) ListFinalizeSessions(ctx context.Context, maxItems int) (out []domain.SessionRecord, err error) {
	defer func() {
		if err != nil {
			out = nil
			err = &FinalizeSourceUnavailableError{"sessions", err}
		}
	}()
	tx, err := s.beginFinalizeRead(ctx, maxItems, 10000, "", false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	ids, err := finalizeIDs(ctx, tx, maxItems, finalizeSessionIDsSQL, maxItems+1)
	if err != nil {
		return nil, err
	}
	out = make([]domain.SessionRecord, 0, len(ids))
	q := gen.New(tx)
	for _, id := range ids {
		row, readErr := q.GetSession(ctx, domain.SessionID(id))
		if readErr != nil {
			return nil, readErr
		}
		if string(row.ID) != id {
			return nil, errors.New("session identity mismatch")
		}
		out = append(out, getSessionRowToRecord(row))
	}
	return out, ctx.Err()
}

// ListFinalizePRs returns every registered PR state in exact URL identity order.
// Its projection matches ListPRFactsBySession, including current-head human
// reviews and AO/external comment separation. Empty is supported; absent schema,
// invalid identity, overflow or SQL failure is a typed unavailable source.
func (s *Store) ListFinalizePRs(ctx context.Context, id domain.SessionID, maxItems int) (out []domain.PRFacts, err error) {
	defer func() {
		if err != nil {
			out = nil
			err = &FinalizeSourceUnavailableError{"prs", err}
		}
	}()
	tx, err := s.beginFinalizeRead(ctx, maxItems, 200, id, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	ids, err := finalizeIDs(ctx, tx, maxItems, finalizePRIDsSQL, id, maxItems+1)
	if err != nil {
		return nil, err
	}
	out = make([]domain.PRFacts, 0, len(ids))
	for _, url := range ids {
		var r gen.ListPRFactsBySessionRow
		err = tx.QueryRowContext(ctx, finalizePRFactSQL, id, url, id, url).Scan(
			&r.URL, &r.Number, &r.PRState, &r.ReviewDecision, &r.CIState,
			&r.Mergeability, &r.SourceBranch, &r.TargetBranch, &r.HeadSha,
			&r.UpdatedAt, &r.ExternalComments, &r.ReviewComments,
			&r.ExternalApproved, &r.ExternalChangesRequested,
		)
		if err != nil {
			return nil, err
		}
		if r.URL != url || r.Number < 1 {
			return nil, errors.New("invalid PR identity")
		}
		out = append(out, prFactsFromListRow(r))
	}
	return out, ctx.Err()
}

// ListFinalizeWorktrees returns registered worktrees only, by exact stored path.
// Lexical aliases and duplicate paths fail closed. This is not a filesystem,
// symlink/reparse, Git ownership or native release observation; those gaps remain
// with the FINALIZE evidence adapter and keep physical finalization HELD.
func (s *Store) ListFinalizeWorktrees(ctx context.Context, id domain.SessionID, maxItems int) (out []domain.SessionWorktreeRecord, err error) {
	defer func() {
		if err != nil {
			out = nil
			err = &FinalizeSourceUnavailableError{"worktrees", err}
		}
	}()
	tx, err := s.beginFinalizeRead(ctx, maxItems, 200, id, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	ids, err := finalizeIDs(ctx, tx, maxItems, finalizeWorktreeIDsSQL, id, maxItems+1)
	if err != nil {
		return nil, err
	}
	out = make([]domain.SessionWorktreeRecord, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	q := gen.New(tx)
	for _, repo := range ids {
		row, readErr := q.GetSessionWorktree(ctx, gen.GetSessionWorktreeParams{SessionID: id, RepoName: repo})
		if readErr != nil {
			return nil, readErr
		}
		if row.SessionID != id || row.RepoName != repo || !finalizeKey(row.Branch) ||
			!finalizeWorktreePath(row.WorktreePath) {
			return nil, errors.New("invalid worktree identity")
		}
		// Slash spelling and drive/UNC case aliases are ambiguous even when
		// running this read on a different OS. Do not resolve filesystem paths.
		key := strings.ReplaceAll(row.WorktreePath, "\\", "/")
		if strings.Contains(key, ":") || strings.HasPrefix(key, "//") {
			key = strings.ToLower(key)
		}
		if seen[key] {
			return nil, errors.New("duplicate worktree path")
		}
		seen[key] = true
		out = append(out, sessionWorktreeFromGen(row))
	}
	return out, ctx.Err()
}

func (s *Store) beginFinalizeRead(ctx context.Context, maxItems, ceiling int, id domain.SessionID, scoped bool) (*sql.Tx, error) {
	if maxItems < 1 || maxItems > ceiling {
		return nil, fmt.Errorf("bound must be 1..%d", ceiling)
	}
	if scoped && (!finalizeKey(string(id)) || len(id) > 256) {
		return nil, errors.New("invalid exact session ID")
	}
	if s == nil || s.readDB == nil {
		return nil, errors.New("missing read database")
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	if scoped {
		var found string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM sessions WHERE id = ? LIMIT 1`, id).Scan(&found); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if found != string(id) {
			_ = tx.Rollback()
			return nil, errors.New("session identity mismatch")
		}
	}
	return tx, nil
}

func finalizeIDs(ctx context.Context, tx *sql.Tx, maxItems int, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := make([]string, 0, maxItems)
	seen := make(map[string]bool, maxItems)
	for rows.Next() {
		if len(ids) == maxItems {
			return nil, ErrFinalizeSourceOverflow
		}
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if !finalizeKey(id) || (query == finalizeSessionIDsSQL && len(id) > 256) || seen[id] {
			return nil, errors.New("invalid or duplicate exact identity")
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return ids, ctx.Err()
}

func finalizeKey(key string) bool {
	return key != "" && utf8.ValidString(key) && strings.TrimSpace(key) == key &&
		!strings.ContainsFunc(key, unicode.IsControl)
}

func finalizeWorktreePath(value string) bool {
	if !finalizeKey(value) {
		return false
	}
	p := strings.ReplaceAll(value, "\\", "/")
	// Accept native absolute POSIX, drive and UNC spellings, but never clean
	// an alias into an accepted identity. Preserve the original stored bytes.
	if len(p) > 2 && p[1] == ':' && ((p[0] >= 'A' && p[0] <= 'Z') || (p[0] >= 'a' && p[0] <= 'z')) {
		p = p[2:]
	} else if strings.HasPrefix(p, "//") {
		p = p[1:]
	}
	return strings.HasPrefix(p, "/") && p != "/" && path.Clean(p) == p
}

// Static single-PR form of queries/pr.sql ListPRFactsBySession. Both current_pr
// and the final SELECT bind the exact session and URL; associated review/comment
// tables are queried as facts, never materialized as unbounded Go slices.
const finalizePRFactSQL = `WITH current_pr AS (
    SELECT url, head_sha
    FROM pr
    WHERE pr.session_id = ? AND pr.url = ?
),
eligible_external_review AS (
    SELECT pr_reviews.pr_url, pr_reviews.author, pr_reviews.review_id, pr_reviews.state, pr_reviews.submitted_at
    FROM pr_reviews
    JOIN current_pr ON current_pr.url = pr_reviews.pr_url
    WHERE pr_reviews.is_bot = 0
      AND pr_reviews.state IN ('approved', 'changes_requested')
      AND (pr_reviews.target_sha = '' OR pr_reviews.target_sha = current_pr.head_sha)
      AND NOT EXISTS (
          SELECT 1
          FROM review_run
          WHERE review_run.github_review_id != ''
            AND review_run.github_review_id = pr_reviews.review_id
      )
),
external_review AS (
    SELECT eligible_external_review.pr_url, eligible_external_review.state
    FROM eligible_external_review
    WHERE NOT EXISTS (
        SELECT 1
        FROM eligible_external_review newer
        WHERE newer.pr_url = eligible_external_review.pr_url
          AND newer.author = eligible_external_review.author
          AND (
              newer.submitted_at > eligible_external_review.submitted_at
              OR (newer.submitted_at = eligible_external_review.submitted_at AND newer.review_id > eligible_external_review.review_id)
          )
    )
)
SELECT
    pr.url,
    pr.number,
    pr.pr_state,
    pr.review_decision,
    pr.ci_state,
    pr.mergeability,
    pr.source_branch,
    pr.target_branch,
    pr.head_sha,
    pr.updated_at,
    EXISTS (
        SELECT 1
        FROM pr_comment
        WHERE pr_comment.pr_url = pr.url
          AND pr_comment.resolved = 0
          AND pr_comment.is_bot = 0
          AND NOT EXISTS (
              SELECT 1
              FROM review_run
              WHERE pr_comment.review_id != ''
                AND review_run.github_review_id != ''
                AND review_run.github_review_id = pr_comment.review_id
          )
    ) AS external_comments,
    EXISTS (
        SELECT 1
        FROM pr_comment
        WHERE pr_comment.pr_url = pr.url
          AND pr_comment.resolved = 0
          AND pr_comment.is_bot = 0
    ) AS review_comments,
    EXISTS (
        SELECT 1
        FROM external_review
        WHERE external_review.pr_url = pr.url
          AND external_review.state = 'approved'
    ) AS external_approved,
    EXISTS (
        SELECT 1
        FROM external_review
        WHERE external_review.pr_url = pr.url
          AND external_review.state = 'changes_requested'
    ) AS external_changes_requested
FROM pr
WHERE pr.session_id = ? AND pr.url = ?
LIMIT 1;`
