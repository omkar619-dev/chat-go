package broker

import "time"

// ChatMessage is the wire format of a single chat message — the exact JSON we
// publish to Redis (for live delivery to browsers) AND to Kafka (for durable
// persistence). Keeping it in ONE place means the producer and the consumer
// can never drift apart on the shape of a message.
//
// room_id and user_id make each message self-contained: the persister can write
// a complete Postgres row without looking anything else up. username is carried
// only for the browser to display — it isn't stored (we join to users on read).
//
// SentAt is stamped by the gateway the moment the message is received, and
// carried all the way into Postgres. It must NOT be re-derived at insert time:
// if the persister is lagging or restarting, "now" is minutes off from when the
// user actually pressed Enter.
type ChatMessage struct {
	RoomID   int64     `json:"room_id"`
	UserID   int64     `json:"user_id"`
	Username string    `json:"username"`
	Body     string    `json:"body"`
	SentAt   time.Time `json:"sent_at"`
}
