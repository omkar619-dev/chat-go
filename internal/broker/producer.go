package broker

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer writes messages to a Kafka topic (our durable message log).
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a Kafka producer pointed at one broker and one topic.
func NewProducer(brokerAddr, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerAddr),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // same key -> same partition -> ordered
			AllowAutoTopicCreation: true,          // create the topic on first write if missing

			// kafka-go defaults BatchTimeout to ONE SECOND, and WriteMessages is
			// synchronous: it blocks until the batch flushes. A lone message never
			// fills the 100-message batch, so every send waited the full second —
			// inside the gateway's read loop. Measured 2026-08-09: a single sender
			// was capped at almost exactly 1 message/second, and asking for 50/s
			// changed nothing.
			//
			// 10ms still batches under concurrent load (which is the point of
			// batching) while keeping an interactive send path interactive. Kept
			// synchronous on purpose: Async would report errors through a callback
			// instead of a return value, and the four-outcome dual-write reporting
			// in ws.go depends on knowing whether THIS message reached Kafka.
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Publish appends one message to the log. The key decides which partition it
// lands in; using room_id as the key keeps a room's messages ordered.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

// Close flushes any buffered messages and shuts the writer down.
func (p *Producer) Close() error {
	return p.writer.Close()
}
