package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// RetentionCensusOptions requires explicit resource limits; no unlimited defaults
// are accepted. CallerSnapshotID is a label, not evidence of an external snapshot.
// HighWaterID optionally narrows the transaction's observed ID ceiling (including
// zero for an empty window). It cannot bind a later transaction to an old snapshot.
type RetentionCensusOptions struct {
	CallerSnapshotID string
	HighWaterID      *int64
	PageRows         int
	MaxPages         int
	MaxReportBytes   int
	MaxDuration      time.Duration
}

// RetentionSourceGap names missing source contracts, independently of scan coverage.
type RetentionSourceGap string

const (
	// RetentionGapOwnership includes conversation/branch/epoch/turn/item binding.
	RetentionGapOwnership RetentionSourceGap = "EVENT_OWNERSHIP_UNKNOWN"
	// RetentionGapProjection indicates no complete per-event projection mapping.
	RetentionGapProjection RetentionSourceGap = "PROJECTION_COMPLETENESS_UNKNOWN"
	// RetentionGapCheckpoint indicates no covering event-window checkpoint mapping.
	RetentionGapCheckpoint RetentionSourceGap = "COVERING_CHECKPOINT_UNKNOWN"
	// RetentionGapReplay includes independent native, branch and host replay needs.
	RetentionGapReplay RetentionSourceGap = "REPLAY_OBLIGATIONS_UNKNOWN"
	// RetentionGapNativeACK cannot be resolved from payloads or terminal-looking turns.
	RetentionGapNativeACK RetentionSourceGap = "NATIVE_ACK_UNKNOWN"
	// RetentionGapSettlement indicates no event-specific proof of turn settlement.
	RetentionGapSettlement RetentionSourceGap = "TURN_SETTLEMENT_UNKNOWN"
	// RetentionGapHolds includes reviewer, provenance, recovery and policy holds.
	RetentionGapHolds RetentionSourceGap = "RETENTION_HOLDS_UNKNOWN"
	// RetentionGapExternalSnapshot means a caller label/high-water is not attestation.
	RetentionGapExternalSnapshot RetentionSourceGap = "EXTERNAL_SNAPSHOT_BINDING_UNKNOWN"
	// RetentionGapMethod indicates unsupported type or overlong method metadata.
	RetentionGapMethod RetentionSourceGap = "METHOD_METADATA_UNKNOWN"
	// RetentionGapPayload indicates a non-text/blob payload with no byte estimate.
	RetentionGapPayload RetentionSourceGap = "PAYLOAD_BYTE_ESTIMATE_UNKNOWN"
)

var (
	// ErrRetentionCensusCursor rejects stale, foreign or out-of-order page cursors.
	ErrRetentionCensusCursor = errors.New("invalid retention census cursor")
	// ErrRetentionCensusClosed indicates completion, exhaustion, cancellation or Close.
	ErrRetentionCensusClosed = errors.New("retention census closed")
)

// RetentionCensusCursor is valid only on its still-open native read transaction.
// It is not a durable resume token; opening another census invalidates its use.
type RetentionCensusCursor struct {
	SnapshotID string `json:"snapshot_id"`
	AfterID    int64  `json:"after_id"`
	Page       int    `json:"page"`
}

// RetentionCensusCount describes only rows scanned in this page. MethodKnown=false
// groups unreportable methods separately from a legitimate empty method string.
type RetentionCensusCount struct {
	Method                  string `json:"method"`
	MethodKnown             bool   `json:"method_known"`
	Class                   string `json:"class"`
	Rows                    int64  `json:"rows"`
	IdentifiedRows          int64  `json:"identified_rows"`
	LogicalPayloadBytes     int64  `json:"logical_payload_bytes"`
	PayloadBytesKnownRows   int64  `json:"payload_bytes_known_rows"`
	PayloadBytesUnknownRows int64  `json:"payload_bytes_unknown_rows"`
}

// RetentionCensusPage reports logical content lengths, never allocated/free disk
// space or deletion savings. SnapshotID is a process-local transaction identity;
// Revision is the same immutable identity, not a database-wide change counter.
// WindowComplete means traversal only: CoverageComplete always remains false.
type RetentionCensusPage struct {
	Version                   string                 `json:"version"`
	SnapshotID                string                 `json:"snapshot_id"`
	Revision                  string                 `json:"revision"`
	CallerSnapshotID          string                 `json:"caller_snapshot_id"`
	SnapshotBinding           string                 `json:"snapshot_binding"`
	SourceMaxID               int64                  `json:"source_max_id"`
	HighWaterID               int64                  `json:"high_water_id"`
	Page                      int                    `json:"page"`
	ScannedRows               int64                  `json:"scanned_rows"`
	TotalScannedRows          int64                  `json:"total_scanned_rows"`
	UnknownRows               int64                  `json:"unknown_rows"`
	UnknownMethodRows         int64                  `json:"unknown_method_rows"`
	Counts                    []RetentionCensusCount `json:"counts"`
	SourceGaps                []RetentionSourceGap   `json:"source_gaps"`
	WindowComplete            bool                   `json:"window_complete"`
	CoverageComplete          bool                   `json:"coverage_complete"`
	UnscannedRows             string                 `json:"unscanned_rows"`
	StopReason                string                 `json:"stop_reason"`
	DeletionAuthority         string                 `json:"deletion_authority"`
	FilesystemReclaimEstimate string                 `json:"filesystem_reclaim_estimate"`
	Next                      *RetentionCensusCursor `json:"next"`
}

// RetentionCensus owns one bounded SQLite read transaction. Call Close when
// abandoning traversal; completion, errors and the lifetime deadline also release
// it. No payload, provider ID, branch label or turn state is retrieved. All rows
// retain ao16.retention-preview.v1's UNKNOWN_REVIEW_REQUIRED classification.
//
// Source map: migrations 0066/0087/0101 and conversations.sql persist archive
// associations and dedupe IDs, but no typed per-event turn/item/ownership epoch or
// covering projection/replay checkpoint. Session-native checkpoint observations
// do not bind this event window. A separately reviewed native owner contract is
// still required; this census neither invents ACKs nor joins current branch or
// turn state to claim historical ownership. SQLite owns snapshot semantics.
type RetentionCensus struct {
	mu        sync.Mutex
	tx        *sql.Tx
	ctx       context.Context
	cancel    context.CancelFunc
	options   RetentionCensusOptions
	id        string
	sourceMax int64
	highWater int64
	next      *RetentionCensusCursor
	pages     int
	scanned   int64
	closed    bool
}

// BeginRetentionCensus pins a snapshot with an indexed MAX(id) read. All pages
// use this transaction, so inserts/updates/cascades committed later are excluded.
// It does not open files, run migrations, alter pragmas or write database rows.
func (s *Store) BeginRetentionCensus(ctx context.Context, options RetentionCensusOptions) (*RetentionCensus, error) {
	if len(options.CallerSnapshotID) > 128 || options.PageRows < 1 || options.PageRows > 1000 ||
		options.MaxPages < 1 || options.MaxPages > 1000 || options.MaxReportBytes < 8192 ||
		options.MaxReportBytes > 1<<20 || options.MaxDuration <= 0 || options.MaxDuration > time.Minute ||
		(options.HighWaterID != nil && *options.HighWaterID < 0) {
		return nil, errors.New("invalid retention census bounds")
	}
	ctx, cancel := context.WithTimeout(ctx, options.MaxDuration)
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("begin retention census: %w", err)
	}
	c := &RetentionCensus{tx: tx, ctx: ctx, cancel: cancel, options: options, id: uuid.NewString()}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM conversation_provider_events`).Scan(&c.sourceMax); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("pin retention census: %w", err)
	}
	c.highWater = c.sourceMax
	if options.HighWaterID != nil {
		if *options.HighWaterID > c.sourceMax {
			_ = c.Close()
			return nil, errors.New("retention high-water exceeds snapshot")
		}
		c.highWater = *options.HighWaterID
	}
	// Retain copied scalars only; subsequent caller pointer mutation has no effect.
	c.options.HighWaterID = nil
	return c, nil
}

// Close releases the snapshot without committing any transaction.
func (c *RetentionCensus) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked()
}

func (c *RetentionCensus) closeLocked() error {
	c.closed = true
	c.cancel()
	err := c.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

// Page requires nil for the first cursor and exactly the preceding Next cursor
// thereafter. PageRows bounds visited records; MaxPages bounds the whole scan.
// MaxReportBytes bounds JSON output. SQL returns at most 128 method bytes and a
// payload length per row, never payload content. A byte-limit failure returns no
// partial page and closes the snapshot. Context errors likewise fail closed.
func (c *RetentionCensus) Page(ctx context.Context, cursor *RetentionCensusCursor) (RetentionCensusPage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var empty RetentionCensusPage
	if c.closed {
		return empty, ErrRetentionCensusClosed
	}
	if err := c.ctx.Err(); err != nil {
		_ = c.closeLocked()
		return empty, err
	}
	if (cursor == nil) != (c.next == nil) || (cursor != nil && *cursor != *c.next) {
		return empty, ErrRetentionCensusCursor
	}
	// Either context can interrupt the query; cancellation ends the census.
	pageCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	p, err := c.pageLocked(pageCtx)
	if err != nil {
		_ = c.closeLocked()
		return empty, err
	}
	return p, nil
}

func (c *RetentionCensus) pageLocked(ctx context.Context) (RetentionCensusPage, error) {
	p := RetentionCensusPage{
		Version: "ao16.retention-census.v1", SnapshotID: c.id, Revision: c.id,
		CallerSnapshotID: c.options.CallerSnapshotID, SnapshotBinding: "SQLITE_READ_TRANSACTION",
		SourceMaxID: c.sourceMax, HighWaterID: c.highWater, Page: c.pages + 1,
		Counts: []RetentionCensusCount{}, UnscannedRows: "UNKNOWN", StopReason: "PAGE_ROWS",
		DeletionAuthority: "NONE", FilesystemReclaimEstimate: "UNKNOWN",
		SourceGaps: []RetentionSourceGap{RetentionGapCheckpoint, RetentionGapOwnership,
			RetentionGapExternalSnapshot, RetentionGapNativeACK, RetentionGapProjection,
			RetentionGapHolds, RetentionGapReplay, RetentionGapSettlement},
	}
	after := int64(math.MinInt64)
	comparison := ">="
	if c.next != nil {
		after = c.next.AfterID
		comparison = ">"
	}
	// CASE avoids transferring arbitrary method strings. octet_length(TEXT) and
	// length(BLOB) use SQLite's stored byte length, including embedded NULs,
	// without materializing payload content (unlike casting TEXT to BLOB).
	// Non-text/blob payloads are explicitly unknown, never silently cast to text.
	rows, err := c.tx.QueryContext(ctx, `SELECT id,
 CASE WHEN typeof(method) = 'text' AND octet_length(method) <= 128 THEN method ELSE NULL END,
 CASE WHEN typeof(provider_event_id) = 'text' AND octet_length(provider_event_id) > 0 THEN 1 ELSE 0 END,
 CASE typeof(payload_json) WHEN 'text' THEN octet_length(payload_json) WHEN 'blob' THEN length(payload_json) ELSE NULL END
 FROM conversation_provider_events WHERE id `+comparison+` ? AND id <= ? ORDER BY id LIMIT ?`, after, c.highWater, c.options.PageRows)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	counts := make(map[sql.NullString]*RetentionCensusCount)
	var unknownBytes bool
	for rows.Next() {
		var method sql.NullString
		var identified int64
		var bytes sql.NullInt64
		if err := rows.Scan(&after, &method, &identified, &bytes); err != nil {
			return p, err
		}
		if method.Valid && !utf8.ValidString(method.String) {
			method = sql.NullString{}
		}
		count := counts[method]
		if count == nil {
			count = &RetentionCensusCount{Method: method.String, MethodKnown: method.Valid, Class: "UNKNOWN_REVIEW_REQUIRED"}
			counts[method] = count
		}
		count.Rows++
		count.IdentifiedRows += identified
		if bytes.Valid {
			count.LogicalPayloadBytes += bytes.Int64
			count.PayloadBytesKnownRows++
		} else {
			count.PayloadBytesUnknownRows++
			unknownBytes = true
		}
		if !method.Valid {
			p.UnknownMethodRows++
		}
		p.ScannedRows++
	}
	if err := rows.Err(); err != nil {
		return p, err
	}
	if err := rows.Close(); err != nil {
		return p, err
	}
	var more bool
	if err := c.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM conversation_provider_events WHERE id > ? AND id <= ?)`, after, c.highWater).Scan(&more); err != nil {
		return p, err
	}
	for _, count := range counts {
		p.Counts = append(p.Counts, *count)
	}
	sort.Slice(p.Counts, func(i, j int) bool {
		if p.Counts[i].MethodKnown != p.Counts[j].MethodKnown {
			return !p.Counts[i].MethodKnown
		}
		return p.Counts[i].Method < p.Counts[j].Method
	})
	if p.UnknownMethodRows > 0 {
		p.SourceGaps = append(p.SourceGaps, RetentionGapMethod)
	}
	if unknownBytes {
		p.SourceGaps = append(p.SourceGaps, RetentionGapPayload)
	}
	sort.Slice(p.SourceGaps, func(i, j int) bool { return p.SourceGaps[i] < p.SourceGaps[j] })
	p.UnknownRows = p.ScannedRows
	p.TotalScannedRows = c.scanned + p.ScannedRows
	p.WindowComplete = !more
	if !more {
		p.UnscannedRows = "ZERO_IN_WINDOW"
		p.StopReason = "WINDOW_COMPLETE"
	} else if p.Page == c.options.MaxPages {
		p.StopReason = "PAGE_LIMIT"
	} else {
		p.Next = &RetentionCensusCursor{SnapshotID: c.id, AfterID: after, Page: p.Page}
	}
	b, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	if len(b) > c.options.MaxReportBytes {
		return p, errors.New("retention census report byte limit exceeded")
	}
	if err := ctx.Err(); err != nil {
		return p, err
	}
	c.pages++
	c.scanned = p.TotalScannedRows
	if p.Next == nil {
		if err := c.closeLocked(); err != nil {
			return p, err
		}
	} else {
		next := *p.Next
		c.next = &next
	}
	return p, nil
}
