package domain

import (
	"errors"
	"time"
)

// ConversationSendOwner names an exact ordinary send and the currently owning
// controller. Admission generation may be older for an untouched queued turn.
type ConversationSendOwner struct {
	ConversationID       string
	TurnID               string
	ClientMessageID      string
	BranchID             string
	SessionID            SessionID
	ControllerGeneration string
}

// ConversationSendState records the durable disposition of an ordinary send.
type ConversationSendState string

const (
	// ConversationSendPending is untouched work eligible for one reservation.
	ConversationSendPending ConversationSendState = "pending"
	// ConversationSendReserved records a send reserved for possible delivery.
	ConversationSendReserved ConversationSendState = "reserved"
	// ConversationSendLegacy records absent attempt history requiring owner acknowledgement.
	ConversationSendLegacy ConversationSendState = "legacy"
	// ConversationSendBound records a send bound to a correlated provider turn.
	ConversationSendBound ConversationSendState = "bound"
	// ConversationSendNotDelivered records correlated evidence of non-delivery.
	ConversationSendNotDelivered ConversationSendState = "not_delivered"
)

// ConversationSendAttempt is internal restart/recovery readback, never an ACK.
// Reserved means possibly delivered, even if the process died before SendTurn.
// Legacy means absent attempt history: owner acknowledgement is required before
// any recovery decision. Neither disposition authorizes replay or queue drain.
type ConversationSendAttempt struct {
	ConversationSendOwner
	State      ConversationSendState
	ReservedAt *time.Time
}

var (
	// ErrConversationSendUncertain requires correlated evidence before resolution.
	ErrConversationSendUncertain = errors.New("ordinary send requires correlated delivery resolution")
	// ErrConversationSendConflict reports an ordinary send owner or attempt conflict.
	ErrConversationSendConflict = errors.New("ordinary send owner or attempt conflict")
)
