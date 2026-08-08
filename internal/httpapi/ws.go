package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/omkar619-dev/chat-go/internal/auth"
	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// The wire format of a chat message lives in the broker package as
// broker.ChatMessage, shared by the producer here and the persister consumer.

// WS upgrades to a WebSocket and joins the connection to a room's Redis
// pub/sub channel: messages typed here are PUBLISHED; messages on the channel
// are pushed down this socket.
func (h *Handlers) WS(w http.ResponseWriter, r *http.Request) {
	// 1. Authenticate from the query string (browsers can't set WS headers).
	claims, err := auth.VerifyToken(r.URL.Query().Get("token"), h.JWTSecret)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or missing token")
		return
	}

	// 2. Which room — and is this user a member?
	roomID, err := strconv.ParseInt(r.URL.Query().Get("room"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or missing room")
		return
	}
	member, err := h.Queries.IsRoomMember(r.Context(), sqlc.IsRoomMemberParams{
		RoomID: roomID,
		UserID: claims.UserID,
	})
	if err != nil || !member {
		writeError(w, http.StatusForbidden, "not a member of this room")
		return
	}

	// 3. Upgrade to a WebSocket.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	ctx := r.Context()
	channel := fmt.Sprintf("room:%d", roomID)

	// Advance this user's read watermark when the socket closes. "Connected" is
	// our proxy for "present": anything delivered while you were here counts as
	// seen, so the gap /catchup summarises is exactly the time you were away.
	//
	// NOTE the context. ctx is r.Context(), which is ALREADY CANCELLED by the time
	// this defer runs — the connection closing is what cancelled it. A query on a
	// dead context fails instantly, so we detach with context.WithoutCancel and
	// give it its own short deadline. Deferred cleanup can almost never reuse the
	// request context, and the failure is silent if you don't think about it.
	defer func() {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := h.Queries.UpsertRoomRead(saveCtx, sqlc.UpsertRoomReadParams{
			RoomID:     roomID,
			UserID:     claims.UserID,
			LastReadAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		}); err != nil {
			log.Printf("read watermark (room %d, user %d): %v", roomID, claims.UserID, err)
		}
	}()

	// 4. Subscribe to the room's channel — anything published there (by anyone,
	//    on any gateway) should be delivered down THIS socket.
	sub := h.Redis.Subscribe(ctx, channel)
	defer sub.Close()

	// 5. Replay history. This happens AFTER subscribing on purpose: anything
	//    published while we're loading is buffered by the subscription rather
	//    than missed. The cost is a possible duplicate of the newest message;
	//    the alternative ordering would leave a permanent GAP. A duplicate is
	//    recoverable, a gap is not.
	history, err := h.Queries.ListRecentMessages(ctx, roomID)
	if err != nil {
		log.Printf("history (room %d): %v", roomID, err)
	}
	for _, m := range history {
		payload, err := json.Marshal(broker.ChatMessage{
			RoomID:   m.RoomID,
			UserID:   m.UserID,
			Username: m.Username,
			Body:     m.Body,
			SentAt:   m.CreatedAt.Time, // unwrap pgtype -> plain time.Time
		})
		if err != nil {
			continue
		}
		if err := writeFrame(ctx, conn, messageFrame(payload)); err != nil {
			return // client vanished mid-replay
		}
	}

	// 6. Pump A: Redis channel -> this socket, in its own goroutine.
	go func() {
		for msg := range sub.Channel() {
			if err := writeFrame(ctx, conn, messageFrame([]byte(msg.Payload))); err != nil {
				return
			}
		}
	}()

	// 7. Pump B: this socket -> Redis. Read what the user typed, tag it with
	//    their username, and publish it so every subscriber (incl. them) gets it.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return // client disconnected
		}

		// Slash commands are handled here and NOT published — they're a request to
		// the gateway, not a message to the room. Note this runs in a goroutine so
		// the user can keep chatting while a summary streams.
		if strings.EqualFold(strings.TrimSpace(string(data)), catchupCommand) {
			go h.streamCatchup(ctx, conn, roomID, claims.UserID)
			continue
		}

		payload, err := json.Marshal(broker.ChatMessage{
			RoomID:   roomID,
			UserID:   claims.UserID,
			Username: claims.Username,
			Body:     string(data),
			SentAt:   time.Now().UTC(), // stamped HERE — the true send time
		})
		if err != nil {
			continue
		}
		if err := h.Redis.Publish(ctx, channel, payload).Err(); err != nil {
			return
		}
		// Dual-write: also append to the durable Kafka log, keyed by room_id so a
		// room's messages stay ordered. Best-effort — a Kafka hiccup must not drop
		// a message that was already delivered live over Redis.
		if err := h.Producer.Publish(ctx, strconv.FormatInt(roomID, 10), payload); err != nil {
			log.Printf("kafka publish (room %d): %v", roomID, err)
		}
	}
}
