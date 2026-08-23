// Package presence answers one question: who is online in a room right now?
//
// The problem it solves: we run more than one gateway. Each gateway only knows
// about the people connected to itself. Gateway A cannot see gateway B's users.
// So neither one can show a complete online list on its own.
//
// The fix: every gateway writes "these people are connected to me" into Redis,
// which all gateways share. Then anyone can read the full list from there.
//
// Why it writes the same thing over and over, every 15 seconds: a gateway that
// crashes cannot tell us its users left. It just dies. So instead of trusting it
// to say goodbye, we make it keep saying "still here". If the writing stops, the
// entries get old, and we ignore anything older than 45 seconds. Silence means
// gone. Like a night watchman punching a clock — if the punches stop, you know
// without anyone phoning you.
package presence

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/omkar619-dev/chat-go/internal/hub"
)

const (
	// How often each gateway re-writes its list.
	beat = 15 * time.Second

	// How old an entry may be before we treat that person as offline. Three
	// heartbeats, so two can go missing (a slow network, a busy moment) before
	// anybody is wrongly shown as gone.
	stale = 45 * time.Second
)

// Key is the Redis key holding one room's online list.
func Key(roomID int64) string { return fmt.Sprintf("presence:room:%d", roomID) }

// Tracker writes this gateway's online users into Redis on a timer.
type Tracker struct {
	rdb *redis.Client
	hub *hub.Hub
}

func New(rdb *redis.Client, h *hub.Hub) *Tracker {
	return &Tracker{rdb: rdb, hub: h}
}

// Run writes the list every 15 seconds until ctx is cancelled. Meant to be
// started with `go tracker.Run(ctx)` from main.
func (t *Tracker) Run(ctx context.Context) {
	ticker := time.NewTicker(beat)
	defer ticker.Stop()

	t.write(ctx) // once immediately, so a fresh gateway appears without a 15s wait

	for {
		select {
		case <-ctx.Done():
			return // the gateway is shutting down
		case <-ticker.C:
			t.write(ctx)
		}
	}
}

// write pushes one snapshot of "who is connected to this gateway" into Redis.
func (t *Tracker) write(ctx context.Context) {
	// Ask the hub who is here. This hands back a COPY, so the hub's lock is
	// already released before we touch the network below. Holding it across a
	// Redis call would stall message delivery in every room.
	rooms := t.hub.Rooms()
	if len(rooms) == 0 {
		return // nobody connected to this gateway; nothing to say
	}

	now := time.Now().Unix()
	cutoff := now - int64(stale.Seconds())

	// A pipeline sends all the commands below in ONE round trip instead of one
	// trip each. With many rooms that is the difference between one network
	// wait and dozens.
	pipe := t.rdb.Pipeline()

	for _, r := range rooms {
		key := Key(r.RoomID)

		// Write every user in this room with ONE command, not one command per
		// user. Same lesson as the subscription fix: cost should scale with
		// rooms, not with people.
		entries := make([]redis.Z, 0, len(r.UserIDs))
		for _, uid := range r.UserIDs {
			// Score = "last seen at". Storing the time as the score is what makes
			// "who was seen in the last 45 seconds" a cheap range lookup later.
			entries = append(entries, redis.Z{Score: float64(now), Member: uid})
		}
		pipe.ZAdd(ctx, key, entries...)

		// Tidy up anyone whose entry has gone stale. Not required for
		// correctness — the read filters by time anyway — but it stops the list
		// growing forever with people who left months ago.
		pipe.ZRemRangeByScore(ctx, key, "-inf", "("+strconv.FormatInt(cutoff, 10))

		// If this gateway dies, nothing will ever prune this key again. The TTL
		// makes the whole thing disappear on its own instead of lingering.
		pipe.Expire(ctx, key, stale*2)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		// Log and carry on. A presence system that gives up the first time the
		// network hiccups is worse than none: everyone silently shows as offline
		// forever, and there is one line in the log from an hour ago explaining
		// why. The next tick simply tries again.
		log.Printf("presence: %v", err)
	}
}
