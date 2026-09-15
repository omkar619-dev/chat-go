package broker

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// LagReader asks Kafka how far a consumer group is behind, WITHOUT being that
// consumer.
//
// The first attempt at this asked each consumer to report its own lag, and
// kafka-go refused outright: ReadLag is "unavailable when GroupID is set". That
// looked like a library limitation and is actually the correct answer to the
// wrong question.
//
// A consumer that has DIED cannot report anything. Any number computed inside
// the consumer process — kafka-go's Reader.Lag(), or the high water mark from
// the last fetched message — freezes at its final value exactly when the
// failure being detected occurs, and a frozen zero is indistinguishable from a
// healthy consumer keeping up. That is precisely the H5 outage: the persister
// was down for hours and every signal anyone had said things were fine.
//
// So lag is measured from a third party that is still running, using only
// Kafka's own bookkeeping: the newest offset in each partition, and the offset
// each group has committed. Both live on the broker, and neither needs the
// consumer to be alive to answer.
type LagReader struct {
	client     *kafka.Client
	brokerAddr string
	topic      string
}

func NewLagReader(brokerAddr, topic string) *LagReader {
	return &LagReader{
		client:     &kafka.Client{Addr: kafka.TCP(brokerAddr)},
		brokerAddr: brokerAddr,
		topic:      topic,
	}
}

// Lag returns the total number of messages the group has not yet committed,
// summed across every partition of the topic.
func (l *LagReader) Lag(ctx context.Context, group string) (int64, error) {
	parts, err := kafka.LookupPartitions(ctx, "tcp", l.brokerAddr, l.topic)
	if err != nil {
		return 0, fmt.Errorf("list partitions: %w", err)
	}
	if len(parts) == 0 {
		return 0, fmt.Errorf("topic %q has no partitions", l.topic)
	}

	// Ask for the first AND last offset of every partition in one request.
	// First is needed for a group that has never committed anything: its lag is
	// the whole retained log, not zero, because these consumers start at
	// FirstOffset. Reporting zero there would hide a consumer that has never
	// run at all — the worst case to miss.
	offsetReqs := make([]kafka.OffsetRequest, 0, len(parts)*2)
	ids := make([]int, 0, len(parts))
	for _, p := range parts {
		offsetReqs = append(offsetReqs, kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
		ids = append(ids, p.ID)
	}

	offsets, err := l.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   kafka.TCP(l.brokerAddr),
		Topics: map[string][]kafka.OffsetRequest{l.topic: offsetReqs},
	})
	if err != nil {
		return 0, fmt.Errorf("list offsets: %w", err)
	}

	committed, err := l.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr:    kafka.TCP(l.brokerAddr),
		GroupID: group,
		Topics:  map[string][]int{l.topic: ids},
	})
	if err != nil {
		return 0, fmt.Errorf("fetch committed offsets for %q: %w", group, err)
	}
	if committed.Error != nil {
		return 0, fmt.Errorf("fetch committed offsets for %q: %w", group, committed.Error)
	}

	// Index the group's committed offset by partition so the two responses can
	// be joined. Kafka does not promise the partitions come back in any order.
	commitByPartition := make(map[int]int64, len(ids))
	for _, p := range committed.Topics[l.topic] {
		if p.Error != nil {
			return 0, fmt.Errorf("partition %d for %q: %w", p.Partition, group, p.Error)
		}
		commitByPartition[p.Partition] = p.CommittedOffset
	}

	var total int64
	for _, p := range offsets.Topics[l.topic] {
		if p.Error != nil {
			return 0, fmt.Errorf("offsets for partition %d: %w", p.Partition, p.Error)
		}

		from, ok := commitByPartition[p.Partition]
		// -1 is Kafka's "this group has never committed here". Measure from the
		// start of the log instead.
		if !ok || from < 0 {
			from = p.FirstOffset
		}

		if behind := p.LastOffset - from; behind > 0 {
			total += behind
		}
		// Negative is not an error worth reporting: it means the group committed
		// past what this request saw, which is just two reads at slightly
		// different instants. Clamping at zero is the honest reading.
	}

	return total, nil
}
