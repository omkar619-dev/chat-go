package broker

import "time"

// Backoff returns how long to wait before retry number `attempt` (1-based):
// 1s, 2s, 4s, 8s, 16s, then capped at 30s.
//
// Why retry at all instead of skipping a failed message? Because Kafka offsets
// record HOW FAR we got, not WHICH messages we handled: CommitMessages(msg)
// commits msg.Offset+1. So if we skipped a failed message and later committed a
// LATER one, that commit would permanently step over the failure — silent data
// loss, exactly what the durable log exists to prevent.
//
// The tradeoff is head-of-line blocking: a message that can never succeed stalls
// its partition forever. The production answer is a bounded retry count plus a
// dead-letter topic; for now, blocking loudly beats losing data silently.
func Backoff(attempt int) time.Duration {
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	d := time.Duration(1<<shift) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
