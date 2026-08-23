// Package hub keeps ONE Redis subscription per room per gateway process and fans
// each message out to that room's local sockets.
//
// Before this existed, internal/httpapi/ws.go called Redis Subscribe inside the
// per-connection handler. A subscribed Redis connection cannot be pooled — once
// it issues SUBSCRIBE it can no longer serve ordinary commands — so every
// WebSocket cost a dedicated Redis connection AND received its own copy of every
// message. Measured 2026-08-09:
//
//	500 sockets -> connected_clients went 2 -> 502 -> 2   (exactly 1:1)
//	a 40-byte message cost 172 bytes per subscriber out of Redis, flat across
//	50 / 100 / 250 / 500 subscribers, so the total is linear in subscriber count
//
// At 10,000 users in one room that is 1.72 MB of Redis egress for a single
// 40-byte message, and ~9,998 users exhausts Redis's default maxclients of
// 10,000 with no messages flowing at all. maxclients is server-wide, so adding
// gateways shares that budget rather than expanding it.
//
// With one subscription per room per process, the same message costs 172 bytes
// times the number of gateway PROCESSES — independent of how many users are
// connected.
//
// This package deliberately knows nothing about WebSockets. It deals in payload
// bytes and channels; the caller owns the socket. That keeps the concurrency
// logic testable on its own, which matters because it is the hardest code here.
package hub

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// Channel is the Redis pub/sub channel name for a room.
//
// Exported so the PUBLISH side uses the same string as the SUBSCRIBE side. A
// mismatch here delivers nothing at all and presents as a silently broken
// socket, which is a miserable thing to debug.
func Channel(roomID int64) string { return fmt.Sprintf("room:%d", roomID) }

// Subscription is one socket's view of a room. The caller ranges over C and
// writes whatever arrives to its WebSocket.
type Subscription struct {
	// C delivers messages published to this room. It is CLOSED when the
	// subscription ends — either because the caller called Close, or because the
	// caller was too slow and the hub dropped it. Ranging over it therefore ends
	// on its own; the caller does not need a separate done signal.
	C <-chan []byte

	// evicted is closed ONLY when the hub drops this subscription for being too
	// slow. Never closed on a normal Close.
	//
	// Necessary because the reader cannot be relied on to notice C closing: by
	// definition an evicted reader is blocked writing to its socket, so it is not
	// at the top of its range loop and will not get there until its context dies.
	// A cleanup path that only runs when the goroutine is unstuck is no cleanup
	// path at all, since "stuck" is the only case it exists for. Waiting on this
	// channel from a separate goroutine works regardless.
	evicted chan struct{}

	hub    *Hub
	roomID int64
	userID int64       // carried only so presence can report who is here
	c      chan []byte // the writable end of C
	slow   atomic.Bool
	once   sync.Once // close(c) exactly once, from either Close or broadcast
}

// Evicted is closed when the hub drops this subscription for being too slow.
// The caller should tear down its socket when it fires — otherwise the client
// stays connected, receives nothing, and is told nothing.
func (s *Subscription) Evicted() <-chan struct{} { return s.evicted }

// Slow reports whether this subscription ended because the reader could not keep
// up, rather than because it was closed normally.
//
// The caller needs this to decide whether advancing the read watermark would be
// a LIE. A forced disconnect happens precisely when messages were still sitting
// in the buffer undelivered — writing "read up to now" would then mark unread
// the exact messages the user was disconnected for missing, and /catchup would
// refuse to mention them.
func (s *Subscription) Slow() bool { return s.slow.Load() }

// room is one Redis subscription plus the set of local sockets sharing it.
type room struct {
	// ps is kept so teardown can CLOSE it directly.
	//
	// Cancelling the pump's context is not enough. ReceiveMessage blocks on a
	// socket read, and cancelling a context does not interrupt a read already in
	// flight — the goroutine stays parked until data arrives. The subscription
	// then never closes, and when a later message finally wakes that orphaned
	// pump it broadcasts into whatever room is in the map NOW, so every member
	// receives two copies. Observed 2026-08-09 as duplicated messages in the UI
	// with PUBSUB NUMSUB room:1 reporting 2 for a single gateway process.
	//
	// Closing the connection is what actually interrupts the read.
	ps      *redis.PubSub
	cancel  context.CancelFunc
	members map[*Subscription]struct{}
}

// Hub owns every room's subscription for this process.
type Hub struct {
	rdb *redis.Client
	buf int // per-subscriber buffer depth

	// One mutex guards both the room map and every room's member set.
	//
	// Coarse on purpose. The only work done while holding it is a map lookup and
	// one NON-BLOCKING channel send per member, which is nanoseconds each — a
	// 10,000-member fan-out is tens of microseconds. A per-room lock would remove
	// cross-room contention but needs two-level locking against the map, and that
	// is a deadlock waiting to happen. If contention ever shows up in a profile
	// it is worth doing; guessing at it now is not.
	mu    sync.Mutex
	rooms map[int64]*room
}

// New builds a Hub. buffer is how many messages may queue for one socket before
// the hub gives up on it; 64 is a reasonable starting point and is now a number
// that can be MEASURED with cmd/loadtest rather than guessed at.
func New(rdb *redis.Client, buffer int) *Hub {
	if buffer <= 0 {
		buffer = 64
	}
	return &Hub{rdb: rdb, buf: buffer, rooms: make(map[int64]*room)}
}

// Join adds a socket to a room, creating the room's Redis subscription if this
// is the first member.
//
// userID is not used for delivery — the hub fans out to sockets, not to people —
// but presence needs to answer "who is here", and the hub is the only thing that
// knows. One user may hold several subscriptions (two tabs, phone and laptop).
func (h *Hub) Join(roomID, userID int64) *Subscription {
	ch := make(chan []byte, h.buf)
	s := &Subscription{C: ch, c: ch, hub: h, roomID: roomID, userID: userID, evicted: make(chan struct{})}

	h.mu.Lock()
	defer h.mu.Unlock()

	r, ok := h.rooms[roomID]
	if !ok {
		// First member in: create the ONE subscription for this room.
		//
		// context.Background(), NOT the joiner's request context. The subscription
		// belongs to the ROOM, not to whoever happened to arrive first — tying it
		// to their request would kill the entire room's feed the moment that one
		// user closed their tab. It is cancelled by Close when the last member
		// leaves, and by nothing else.
		ctx, cancel := context.WithCancel(context.Background())
		ps := h.rdb.Subscribe(ctx, Channel(roomID))
		r = &room{ps: ps, cancel: cancel, members: make(map[*Subscription]struct{})}
		h.rooms[roomID] = r
		go h.pump(ctx, roomID, r)
	}
	r.members[s] = struct{}{}
	return s
}

// pump is the single reader of one room's Redis subscription.
//
// Uses ReceiveMessage rather than PubSub.Channel(). Channel() spawns its own
// goroutine with an internal buffer (100 by default) and SILENTLY DROPS messages
// when that buffer fills — a drop policy nobody chose, buried in a library
// default. ReceiveMessage blocks instead, so back-pressure surfaces here where
// we can decide what to do about it.
func (h *Hub) pump(ctx context.Context, roomID int64, r *room) {
	defer r.ps.Close()
	for {
		msg, err := r.ps.ReceiveMessage(ctx)
		if err != nil {
			// Subscription closed by teardown, or Redis is unreachable and
			// go-redis has given up reconnecting.
			return
		}
		// Pass r, not just roomID: broadcast must be able to tell whether this
		// pump still owns the room. See the identity check there.
		h.broadcast(roomID, r, []byte(msg.Payload))
	}
}

// broadcast delivers one payload to every local member of a room.
//
// The SAME byte slice goes to every member — a zero-copy fan-out. The contract
// is that nobody mutates it, which holds because the only consumer marshals it
// into a frame and writes it.
func (h *Hub) broadcast(roomID int64, from *room, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Belt and braces against the leak fixed above: a pump that outlived its own
	// room instance must never deliver into the room that REPLACED it. Comparing
	// pointers is enough — a rebuilt room is a different allocation — and it
	// makes duplicate delivery impossible even if some future teardown path
	// forgets to close the subscription.
	if h.rooms[roomID] != from {
		return
	}
	r := from

	for s := range r.members {
		select {
		case s.c <- payload:
			// Queued for this socket's writer.
		default:
			// Buffer full: this reader cannot keep up.
			//
			// DISCONNECT rather than drop. Dropping leaves holes in their
			// conversation with no signal that anything went wrong — silent loss
			// behind a healthy-looking UI. Disconnecting is recoverable: they
			// reconnect and history replays from Postgres, so they lose nothing.
			// Blocking is not an option at all; that is head-of-line blocking,
			// one slow socket freezing the room.
			//
			// Deleting from a map while ranging over it is well defined in Go.
			// Logged HERE, at the moment of eviction, not where the socket finally
			// unwinds. The handler's own log line runs in a defer, so it timestamps
			// teardown rather than the decision — which made it impossible to tell
			// "evicted late" apart from "evicted promptly, closed late".
			log.Printf("hub: evicting slow reader from room %d (%d-message buffer full)",
				roomID, cap(s.c))
			s.slow.Store(true)
			delete(r.members, s)
			s.closeC()
			close(s.evicted) // only reachable once: we just removed s from members
		}
	}
}

// RoomMembers is a snapshot of who this process has connected to one room.
type RoomMembers struct {
	RoomID int64
	// UserIDs is DEDUPLICATED — one entry per person, not per socket. Someone
	// with a phone and a laptop open is present once, which is what presence
	// means and what keeps the ZADD small.
	UserIDs []int64
}

// Rooms snapshots every room this process currently holds members for.
//
// Returns a COPY, and the lock is released before the caller does anything with
// it. That matters: the caller is the presence heartbeat, which follows this
// with network calls to Redis. Holding the hub's mutex across a round trip would
// stall message delivery in EVERY room behind it — the mutex is only cheap
// because nothing slow ever happens under it. Copy, unlock, then do the slow
// thing.
func (h *Hub) Rooms() []RoomMembers {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]RoomMembers, 0, len(h.rooms))
	for roomID, r := range h.rooms {
		seen := make(map[int64]struct{}, len(r.members))
		ids := make([]int64, 0, len(r.members))
		for s := range r.members {
			if _, dup := seen[s.userID]; dup {
				continue
			}
			seen[s.userID] = struct{}{}
			ids = append(ids, s.userID)
		}
		out = append(out, RoomMembers{RoomID: roomID, UserIDs: ids})
	}
	return out
}

// Close removes this socket from its room, tearing the room down if it was the
// last member. Safe to call more than once.
func (s *Subscription) Close() {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()

	r, ok := h.rooms[s.roomID]
	if !ok {
		return // room already gone; broadcast dropped us and we were the last
	}
	if _, member := r.members[s]; member {
		delete(r.members, s)
		s.closeC()
	}
	if len(r.members) == 0 {
		// Last one out turns off the lights.
		//
		// CLOSE the subscription, don't just cancel the context. The pump is
		// blocked in ReceiveMessage on a socket read, and a cancelled context
		// does not interrupt a read already in flight — closing the connection
		// does. cancel() is still called so the context is not leaked, but it is
		// the Close that actually stops the goroutine.
		r.ps.Close()
		r.cancel()
		delete(h.rooms, s.roomID)
	}
}

// closeC closes the delivery channel exactly once. Both Close and broadcast can
// reach it — a socket can be dropped for slowness at the same moment its handler
// is unwinding — and closing a closed channel panics.
func (s *Subscription) closeC() {
	s.once.Do(func() { close(s.c) })
}
