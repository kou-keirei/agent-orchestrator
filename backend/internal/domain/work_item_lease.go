package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrInvalidWorkLease reports a malformed grant or transition.
	ErrInvalidWorkLease = errors.New("invalid work lease")
	// ErrWorkLeaseConflict reports a stale or incompatible lease transition.
	ErrWorkLeaseConflict = errors.New("work lease compare-and-swap conflict")
	// ErrWorkLeaseExpired denies advancement/effects after the logical deadline.
	ErrWorkLeaseExpired = errors.New("work lease expired")
	// ErrWorkEffectDenied reports an effect absent from the durable allowlist.
	ErrWorkEffectDenied = errors.New("work effect denied")
)

// WorkLeaseGrant is an explicit caller-authorized allowlist, never a provider
// permission observation. An empty allowlist authorizes no effects.
type WorkLeaseGrant struct {
	ID             string
	Duration       time.Duration
	AllowedEffects []string
}

// Validate rejects ambiguous identifiers, duplicate effects and invalid duration.
func (g WorkLeaseGrant) Validate() error {
	if !validLeaseText(g.ID) || g.Duration < time.Millisecond || g.Duration%time.Millisecond != 0 {
		return ErrInvalidWorkLease
	}
	seen := make(map[string]bool)
	for _, effect := range g.AllowedEffects {
		if !validLeaseText(effect) || seen[effect] {
			return fmt.Errorf("%w: effect", ErrInvalidWorkLease)
		}
		seen[effect] = true
	}
	return nil
}

// WorkItemLease is the single current logical lease. Revision fences every
// transition; OwnerGeneration binds it to the accepted WorkItem owner. Expiry
// never transfers ownership or implies process death. Terminal rows are kept.
type WorkItemLease struct {
	WorkItemID      WorkItemID
	OwnerGeneration int64
	Revision        int64
	Grant           WorkLeaseGrant
	ExpiresAt       time.Time
	State           string
}

const (
	// WorkLeaseActive permits fresh checks until the stored deadline.
	WorkLeaseActive = "active"
	// WorkLeaseReleased requires explicit supersession before further effects.
	WorkLeaseReleased = "released"
	// WorkLeaseCompleted is terminal for this WorkItem.
	WorkLeaseCompleted = "completed"
)

func validLeaseText(s string) bool {
	return s != "" && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}
