package domain

import "errors"

// ErrWorkAdmissionHeld requires caller reconciliation rather than an automatic
// retry or a new worker. A timeout never proves that a prior worker stopped.
var ErrWorkAdmissionHeld = errors.New("work admission HOLD")

const (
	// WorkAdmissionReserved occupies the slot until an explicit release.
	WorkAdmissionReserved = "reserved"
	// WorkAdmissionReleased retains the closed attempt without occupying capacity.
	WorkAdmissionReleased = "released"
)

// WorkItemAdmission records occupancy of the single P0 worker slot in one AO
// database. Identity and lease authority remain in their existing stores. This
// receipt is bookkeeping, not a reusable dispatch or native permission token.
type WorkItemAdmission struct {
	WorkItemID      WorkItemID
	AttemptID       WorkAttemptID
	OwnerSessionID  SessionID
	OwnerGeneration int64
	LeaseID         string
	LeaseRevision   int64
	State           string
}
