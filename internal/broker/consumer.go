package broker

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Consumer reads messages from a Kafka topic as a member of a named consumer
// group. The group is how Kafka remembers which messages we've already handled
// (our committed "offset"), so a restart resumes where we left off instead of
// re-reading the whole log.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer builds a group consumer for one topic on one broker.
func NewConsumer(brokerAddr, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset, // a brand-new group reads the whole log from the start
		}),
	}
}

// Fetch blocks until the next message arrives (or ctx is cancelled). It does
// NOT commit the offset — we commit only AFTER we've durably handled the
// message. That ordering gives us at-least-once delivery: we never silently
// lose a message, at the cost of a possible duplicate if we crash mid-way.
func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// Commit records that msg has been fully processed, so a restart resumes AFTER
// it. Call this only once the message is safely in Postgres.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

// Close shuts the reader down (also flushes any pending offset commits).
func (c *Consumer) Close() error {
	return c.reader.Close()
}
