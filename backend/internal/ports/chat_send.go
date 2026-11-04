package ports

import "errors"

// ErrChatDeliveryUncertain means a prompt may have reached the provider. Keep
// its durable attempt; a new client ID or controller is not delivery evidence.
var ErrChatDeliveryUncertain error = chatDeliveryUncertain{}

type chatDeliveryUncertain struct{}

func (chatDeliveryUncertain) Error() string         { return "chat delivery is uncertain" }
func (chatDeliveryUncertain) DeliveryUnknown() bool { return true }

// IsChatDeliveryUnknown recognizes wrapped provider and service uncertainty.
func IsChatDeliveryUnknown(err error) bool {
	var unknown interface{ DeliveryUnknown() bool }
	return errors.As(err, &unknown) && unknown.DeliveryUnknown()
}

// ChatSendNonDelivery is an optional SendTurn error contract. The native driver
// guarantees that this exact invocation conclusively did not deliver, and supplies
// its request-correlated evidence. Timeouts, transport loss, missing references,
// generic lifecycle events and interrupt refusals do not implement this contract.
type ChatSendNonDelivery interface {
	error
	NonDeliveryEvidence() string
}
