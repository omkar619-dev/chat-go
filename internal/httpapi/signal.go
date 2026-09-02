package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/coder/websocket"

	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/hub"
)

// handleClientFrame deals with a structured message the browser sent UP the
// socket. So far that means WebRTC signalling and nothing else.
func (h *Handlers) handleClientFrame(ctx context.Context, conn *websocket.Conn, roomID, fromUserID int64, raw []byte) {
	var cf clientFrame
	if err := json.Unmarshal(raw, &cf); err != nil {
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "could not read that request"})
		return
	}

	switch cf.Type {
	case frameSignal:
		h.relaySignal(ctx, conn, roomID, fromUserID, cf)
	default:
		// Ignored, not rejected — the same courtesy the browser extends to frame
		// types it doesn't recognise. That symmetry is what lets either side ship a
		// new kind of message before the other has learned about it.
	}
}

// relaySignal forwards one WebRTC note to whoever it is addressed to.
//
// Note what is NOT here: the note is never written to Kafka. Chat messages are
// the conversation and belong in the durable log; signalling is transient
// plumbing for one call. Replaying a six-week-old ICE candidate would be
// meaningless, and persisting it would be storing scaffolding as if it were the
// building.
func (h *Handlers) relaySignal(ctx context.Context, conn *websocket.Conn, roomID, fromUserID int64, cf clientFrame) {
	if cf.To == 0 || cf.To == fromUserID {
		// A note addressed to yourself is either a bug or somebody probing. Refuse
		// it rather than relay it — a browser negotiating a call with itself gets
		// into states that are very hard to reason about.
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "invalid call target"})
		return
	}

	payload, err := json.Marshal(broker.Signal{
		RoomID: roomID,
		From:   fromUserID,
		To:     cf.To,
		Kind:   cf.Kind,
		Data:   cf.Data,
	})
	if err != nil {
		return
	}

	// PUBLISHED, not written straight to the recipient's socket. They may well be
	// connected to the other gateway, and this process has no way to reach a socket
	// it doesn't hold. Redis is the only thing the gateways share — the same reason
	// presence lives there rather than in memory.
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := h.Redis.Publish(pubCtx, hub.SignalChannel(roomID), payload).Err(); err != nil {
		log.Printf("signal publish (room %d, %d->%d, %s): %v", roomID, fromUserID, cf.To, cf.Kind, err)
		_ = writeFrame(ctx, conn, frame{Type: frameError, Text: "could not reach the other side"})
		return
	}

	// ICE candidates arrive in bursts of dozens per call, so logging each one
	// would bury everything else in the gateway's output. The three notes that
	// describe the shape of a call are worth seeing; the candidates are not.
	if cf.Kind != "ice" {
		log.Printf("signal %s: user %d -> user %d in room %d", cf.Kind, fromUserID, cf.To, roomID)
	}
}
