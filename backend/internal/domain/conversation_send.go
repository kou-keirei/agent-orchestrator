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

type ConversationSendState string

const (
	ConversationSendPending      ConversationSendState = "pending"
	ConversationSendReserved     ConversationSendState = "reserved"
	ConversationSendLegacy       ConversationSendState = "legacy"
	ConversationSendBound        ConversationSendState = "bound"
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
	ErrConversationSendUncertain = errors.New("ordinary send requires correlated delivery resolution")
	ErrConversationSendConflict  = errors.New("ordinary send owner or attempt conflict")
)
