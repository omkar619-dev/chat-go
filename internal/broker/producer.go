package broker

import (
	"context"

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
