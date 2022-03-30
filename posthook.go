package brev

import "time"

type PosthookEvent string

func (p PosthookEvent) String() string {
	return string(p)
}

// EventDelivered Message has been successfully delivered to the receiving server.
const EventDelivered PosthookEvent = "delivered"

//EventDeferred Recipient's email server temporarily rejected message.
const EventDeferred PosthookEvent = "deferred"

//EventBounce Receiving server could not or would not accept message.
const EventBounce PosthookEvent = "bounce"

//EventFailed for some reason, sending the email failed
const EventFailed PosthookEvent = "failed"

//EventDropped the message was dropped by brev
const EventDropped PosthookEvent = "dropped"

//EventSpam Recipient marked a message as spam.
const EventSpam PosthookEvent = "spam"

//EventUnsubscribe Recipient clicked on message's subscription management link. You need to enable Subscription Tracking for getting this type of event.
const EventUnsubscribe PosthookEvent = "unsubscribe"

type Posthook struct {
	MessageId     string        `json:"message_id"`
	TransactionId int64         `json:"transaction_id"`
	Emails        []string      `json:"emails"`
	CreatedAt     time.Time     `json:"CreatedAt"`
	Event         PosthookEvent `json:"event"`
	Info          string        `json:"info"`
}
