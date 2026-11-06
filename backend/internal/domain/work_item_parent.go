package domain

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidWorkItemParent rejects incomplete or foreign target bindings.
	ErrInvalidWorkItemParent = errors.New("invalid original work item parent")
	// ErrWorkItemParentHeld means no original-parent delivery authority is available.
	ErrWorkItemParentHeld = errors.New("original work item parent HOLD")
	// ErrWorkItemParentBlocksPR identifies an unimplemented issuance boundary.
	ErrWorkItemParentBlocksPR = errors.New("original work item parent BLOCKS_PR")
)

// WorkItemParentHoldReason identifies why the original parent cannot be used.
type WorkItemParentHoldReason string

const (
	// WorkItemParentLegacyMissing preserves unknown origin on pre-binding rows.
	WorkItemParentLegacyMissing WorkItemParentHoldReason = "legacy_original_parent_target_missing"
	// WorkItemParentIssuanceUnbound rejects issuance without a controller-bound
	// internal capability. Durable session metadata alone is not live evidence.
	WorkItemParentIssuanceUnbound WorkItemParentHoldReason = "original_parent_schema_and_issuer_unbound"
	// WorkItemParentControllerMismatch refuses foreign, replaced or stale origin.
	WorkItemParentControllerMismatch WorkItemParentHoldReason = "original_parent_controller_mismatch"
)

// WorkItemParentHold is a typed refusal, never a substitute parent target.
type WorkItemParentHold struct {
	Reason WorkItemParentHoldReason
}

func (h *WorkItemParentHold) Error() string {
	if h.Reason == WorkItemParentIssuanceUnbound {
		return fmt.Sprintf("%s: %s", ErrWorkItemParentBlocksPR, h.Reason)
	}
	return fmt.Sprintf("%s: %s", ErrWorkItemParentHeld, h.Reason)
}

// Is keeps all refusals recognizable as HOLD and unbound issuance as BLOCKS_PR.
func (h *WorkItemParentHold) Is(err error) bool {
	return err == ErrWorkItemParentHeld ||
		(err == ErrWorkItemParentBlocksPR && h.Reason == WorkItemParentIssuanceUnbound)
}

// WorkItemParentTarget is the immutable issuance-time destination contract for
// a later report consumer. NativeSessionID belongs to the ORIGINAL PARENT, not
// WorkItemRecord.OriginalNativeSessionID (which belongs to the worker).
// These fields describe AO-owned live-controller observations. Constructing or
// validating this value does not establish their authority; only transactional
// readback from a trusted issuer may do so.
type WorkItemParentTarget struct {
	WorkItemID              WorkItemID
	AttemptID               WorkAttemptID
	OriginalParentSessionID SessionID
	NativeSessionID         string
	ControllerGeneration    string
	ProviderConversationID  string
	// ProviderScopeID preserves the native conversation's opaque ID namespace.
	ProviderScopeID string
	// IssuedBySessionID and IssuedByControllerGeneration must identify the same
	// original parent controller, never a worker or replacement coordinator.
	IssuedBySessionID            SessionID
	IssuedByControllerGeneration string
	// Provenance is limited to a positively observed live original controller.
	// Restored/unattributed native history cannot be upgraded to this value.
	Provenance string
}

// WorkItemParentLiveProvenance describes the required AO-owned observation.
const WorkItemParentLiveProvenance = "ao_live_original_parent_controller"

// ValidateFor checks shape and immutable work/attempt/parent membership only.
// Owner generations, leases and candidate HEAD are deliberately not part of the
// destination: ownership transfer must never retarget the original parent.
// Callers must obtain t from WorkItemParentReader, never HTTP/report payloads.
func (t WorkItemParentTarget) ValidateFor(work WorkItemIdentity) error {
	if err := work.Validate(); err != nil {
		return err
	}
	for _, value := range []string{string(t.WorkItemID), string(t.AttemptID),
		string(t.OriginalParentSessionID), t.NativeSessionID, t.ControllerGeneration,
		t.ProviderConversationID, t.ProviderScopeID, string(t.IssuedBySessionID),
		t.IssuedByControllerGeneration} {
		if value == "" || strings.TrimSpace(value) != value ||
			strings.ContainsAny(value, "\x00\r\n") || !utf8.ValidString(value) {
			return ErrInvalidWorkItemParent
		}
	}
	if t.WorkItemID != work.ID || t.AttemptID != work.AttemptID ||
		t.OriginalParentSessionID != work.OriginalParentSessionID ||
		t.IssuedBySessionID != t.OriginalParentSessionID ||
		t.IssuedByControllerGeneration != t.ControllerGeneration ||
		t.Provenance != WorkItemParentLiveProvenance {
		return ErrInvalidWorkItemParent
	}
	return nil
}
