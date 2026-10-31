package session

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// BulkVisibilityEvidence is caller-supplied evidence, never an observation made
// by this helper. Its zero value (and every unrecognized value) blocks eligibility.
type BulkVisibilityEvidence uint8

const (
	BulkVisibilityUnknown BulkVisibilityEvidence = iota
	BulkVisibilityNo
	BulkVisibilityYes
)

// BulkVisibilitySnapshot is a hypothetical input, not a durable eligibility
// contract. Readback revisions must be supplied from the same caller snapshot.
// Matching tokens do not prove atomicity or freshness and grant no apply authority.
type BulkVisibilitySnapshot struct {
	SessionID               string
	ProjectID               string
	Kind                    domain.SessionKind
	Hidden                  BulkVisibilityEvidence
	RowRevision             string
	RowRevisionReadback     string
	CleanupRevision         string
	CleanupRevisionReadback string
	Activity                domain.ActivityState
	Terminated              BulkVisibilityEvidence
	PendingApprovals        BulkVisibilityEvidence
	PendingTurns            BulkVisibilityEvidence
	UnreconciledReports     BulkVisibilityEvidence
	UnreconciledCI          BulkVisibilityEvidence
	ProviderCertain         BulkVisibilityEvidence
	OwnershipCertain        BulkVisibilityEvidence
	Dirty                   BulkVisibilityEvidence
	Ambiguous               BulkVisibilityEvidence
	Pinned                  BulkVisibilityEvidence
	UnderReview             BulkVisibilityEvidence
}

type BulkVisibilityReasonCode string

const (
	BulkVisibilityMissingEvidence BulkVisibilityReasonCode = "missing_evidence"
	BulkVisibilityInvalidEvidence BulkVisibilityReasonCode = "invalid_evidence"
	BulkVisibilityProtected       BulkVisibilityReasonCode = "protected"
	BulkVisibilityProjectMismatch BulkVisibilityReasonCode = "project_mismatch"
	BulkVisibilityRevisionChanged BulkVisibilityReasonCode = "revision_changed"
)

// Field identifies the exact supplied fact responsible for each reason.
type BulkVisibilityReason struct {
	Field string
	Code  BulkVisibilityReasonCode
}

// BulkVisibilityPreviewEntry preserves both supplied revision pairs for readback.
// It is a diagnostic value, not a command, lease, or authorization token.
type BulkVisibilityPreviewEntry struct {
	Snapshot BulkVisibilitySnapshot
	Reasons  []BulkVisibilityReason
}

const BulkVisibilityPreviewPageSize = 200

type BulkVisibilityPreview struct {
	Eligible           []BulkVisibilityPreviewEntry
	Blocked            []BulkVisibilityPreviewEntry
	AlreadyHidden      []BulkVisibilityPreviewEntry
	EligibleTotal      int
	BlockedTotal       int
	AlreadyHiddenTotal int
	// NextOffset is -1 at the end. Offsets are valid only for the same supplied
	// snapshot; a changed snapshot must be previewed again from offset zero.
	NextOffset int
}

// PreviewBulkVisibility classifies supplied facts without I/O or mutation. It
// sorts each category by exact session ID, then pages eligible, blocked, and
// already-hidden entries in that order, at most 200 entries TOTAL per page.
// Duplicate/blank IDs or invalid pagination reject the entire preview, including
// rows beyond the requested page. Hidden rows retain diagnostic protection reasons.
// No result can be used to apply Hide: a future backend apply boundary must own
// revision-CAS and transactionally authoritative eligibility revalidation.
func PreviewBulkVisibility(projectID string, snapshots []BulkVisibilitySnapshot, offset int) (BulkVisibilityPreview, error) {
	result := BulkVisibilityPreview{NextOffset: -1}
	if strings.TrimSpace(projectID) == "" {
		return result, fmt.Errorf("bulk visibility preview: project is required")
	}
	if offset < 0 || offset > len(snapshots) {
		return result, fmt.Errorf("bulk visibility preview: offset out of range")
	}
	seen := make(map[string]bool, len(snapshots))
	var eligible, blocked, hidden []BulkVisibilityPreviewEntry
	for _, snapshot := range snapshots {
		if strings.TrimSpace(snapshot.SessionID) == "" || strings.TrimSpace(snapshot.SessionID) != snapshot.SessionID {
			return result, fmt.Errorf("bulk visibility preview: invalid session ID")
		}
		if seen[snapshot.SessionID] {
			return result, fmt.Errorf("bulk visibility preview: duplicate session ID %q", snapshot.SessionID)
		}
		seen[snapshot.SessionID] = true
		entry := BulkVisibilityPreviewEntry{Snapshot: snapshot, Reasons: bulkVisibilityReasons(projectID, snapshot)}
		switch {
		case snapshot.Hidden == BulkVisibilityYes:
			hidden = append(hidden, entry)
		case len(entry.Reasons) != 0:
			blocked = append(blocked, entry)
		default:
			eligible = append(eligible, entry)
		}
	}
	result.EligibleTotal, result.BlockedTotal, result.AlreadyHiddenTotal = len(eligible), len(blocked), len(hidden)
	// Sort only fresh local slices; neither the caller's order nor facts change.
	for _, entries := range [][]BulkVisibilityPreviewEntry{eligible, blocked, hidden} {
		sort.Slice(entries, func(i, j int) bool { return entries[i].Snapshot.SessionID < entries[j].Snapshot.SessionID })
	}
	remaining, skip := BulkVisibilityPreviewPageSize, offset
	for i, entries := range [][]BulkVisibilityPreviewEntry{eligible, blocked, hidden} {
		if skip >= len(entries) {
			skip -= len(entries)
			continue
		}
		entries = entries[skip:]
		skip = 0
		if len(entries) > remaining {
			entries = entries[:remaining]
		}
		remaining -= len(entries)
		switch i {
		case 0:
			result.Eligible = entries
		case 1:
			result.Blocked = entries
		case 2:
			result.AlreadyHidden = entries
		}
	}
	if len(snapshots)-offset > BulkVisibilityPreviewPageSize {
		result.NextOffset = offset + BulkVisibilityPreviewPageSize
	}
	return result, nil
}

func bulkVisibilityReasons(projectID string, s BulkVisibilitySnapshot) []BulkVisibilityReason {
	var reasons []BulkVisibilityReason
	add := func(field string, code BulkVisibilityReasonCode) {
		reasons = append(reasons, BulkVisibilityReason{Field: field, Code: code})
	}
	if strings.TrimSpace(s.ProjectID) == "" {
		add("project", BulkVisibilityMissingEvidence)
	} else if s.ProjectID != projectID {
		add("project", BulkVisibilityProjectMismatch)
	}
	switch s.Kind {
	case domain.KindWorker:
	case domain.KindOrchestrator:
		add("kind", BulkVisibilityProtected)
	case "":
		add("kind", BulkVisibilityMissingEvidence)
	default:
		add("kind", BulkVisibilityInvalidEvidence)
	}
	for _, revision := range []struct{ field, expected, readback string }{
		{"row_revision", s.RowRevision, s.RowRevisionReadback},
		{"cleanup_revision", s.CleanupRevision, s.CleanupRevisionReadback},
	} {
		if strings.TrimSpace(revision.expected) == "" || strings.TrimSpace(revision.readback) == "" {
			add(revision.field, BulkVisibilityMissingEvidence)
		} else if revision.expected != revision.readback {
			add(revision.field, BulkVisibilityRevisionChanged)
		}
	}
	switch s.Activity {
	case domain.ActivityIdle, domain.ActivityExited:
	case "":
		add("activity", BulkVisibilityMissingEvidence)
	case domain.ActivityActive, domain.ActivityWaitingInput, domain.ActivityBlocked:
		add("activity", BulkVisibilityProtected)
	default:
		add("activity", BulkVisibilityInvalidEvidence)
	}
	// Fixed order makes multi-reason output deterministic, including unknowns.
	for _, fact := range []struct {
		field       string
		value, safe BulkVisibilityEvidence
	}{
		{"hidden", s.Hidden, BulkVisibilityNo},
		{"terminated", s.Terminated, BulkVisibilityYes},
		{"pending_approvals", s.PendingApprovals, BulkVisibilityNo},
		{"pending_turns", s.PendingTurns, BulkVisibilityNo},
		{"unreconciled_reports", s.UnreconciledReports, BulkVisibilityNo},
		{"unreconciled_ci", s.UnreconciledCI, BulkVisibilityNo},
		{"provider_certain", s.ProviderCertain, BulkVisibilityYes},
		{"ownership_certain", s.OwnershipCertain, BulkVisibilityYes},
		{"dirty", s.Dirty, BulkVisibilityNo},
		{"ambiguous", s.Ambiguous, BulkVisibilityNo},
		{"pinned", s.Pinned, BulkVisibilityNo},
		{"under_review", s.UnderReview, BulkVisibilityNo},
	} {
		switch fact.value {
		case BulkVisibilityUnknown:
			add(fact.field, BulkVisibilityMissingEvidence)
		case BulkVisibilityNo, BulkVisibilityYes:
			if fact.value != fact.safe && fact.field != "hidden" {
				add(fact.field, BulkVisibilityProtected)
			}
		default:
			add(fact.field, BulkVisibilityInvalidEvidence)
		}
	}
	return reasons
}
