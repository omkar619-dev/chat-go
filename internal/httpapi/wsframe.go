package httpapi

import (
	"context"
	"encoding/json"

	"github.com/coder/websocket"
)

// Frame types. Every object sent down the WebSocket carries one of these in its
// "type" field, so the client can tell kinds apart without guessing from which
// fields happen to be present.
const (
	frameMessage      = "message"       // a chat message (live or replayed history)
	frameSummaryChunk = "summary.chunk" // one fragment of a streaming summary
	frameSummaryDone  = "summary.done"  // the streaming summary finished
	frameError        = "error"         // something went wrong with a request
	// framePresence says "the online list changed, go and re-read it". It
	// deliberately carries no names: sending the list here as well would mean two
	// places producing the same answer, and two places that can disagree. One
	// source of truth — GET /rooms/{id}/presence — and this is only a nudge to
	// go and ask it.
	framePresence = "presence"
	// frameSignal carries one WebRTC note for THIS user. The server relays these
	// without understanding them — see broker.Signal.
	frameSignal = "signal"
)

// frame is one JSON object on the WebSocket.
//
// Until now the socket carried exactly one shape — a bare broker.ChatMessage —
// and the browser assumed it. That works right up until a second kind of thing
// needs sending, at which point the client has no way to tell them apart. The
// alternative to a type tag is sniffing which fields exist, which breaks quietly
// the moment two kinds share a field name.
//
// Note this is a SEPARATE format from broker.ChatMessage. That one is the Kafka
// wire format, shared with the persister and indexer; this one is the browser
// protocol. They were the same object while there was only one kind of message,
// but they answer to different consumers and evolve for different reasons, so
// adding a browser-only "type" field to the Kafka payload would be wrong.
//
// Go has no sum types, so the variants share one struct and `omitempty` keeps
// the unused fields off the wire.
type frame struct {
	Type    string          `json:"type"`
	Message json.RawMessage `json:"message,omitempty"` // set when Type == frameMessage
	Text    string          `json:"text,omitempty"`    // set for summary chunks / errors

	// Set only when Type == frameSignal.
	From   int64           `json:"from,omitempty"`   // who is calling / answering
	Kind   string          `json:"kind,omitempty"`   // offer | answer | ice | hangup
	Signal json.RawMessage `json:"signal,omitempty"` // opaque to us; the browser's SDP or candidate
}

// clientFrame is what the BROWSER sends UP the socket.
//
// Until now the inbound direction carried plain text: either a chat message or
// the "/catchup" slash command. That was enough while everything inbound was a
// string. WebRTC is not — a session description is a multi-line blob and an ICE
// candidate is a structured object — so inbound needs a real format too.
//
// Plain text still works, so nothing that already worked has changed: the read
// loop only parses JSON when the message starts with "{". Sniffing on a brace is
// a little crude, and it is written down here rather than left implicit.
type clientFrame struct {
	Type string `json:"type"` // "signal"
	To   int64  `json:"to"`   // which member of the room this is for

	// Kind and Data are passed straight through to the recipient. The server does
	// not look inside Data — see broker.Signal for why that is deliberate.
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data,omitempty"`
}

// writeFrame marshals a frame and sends it as one WebSocket text message.
func writeFrame(ctx context.Context, conn *websocket.Conn, f frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

// messageFrame wraps an already-encoded broker.ChatMessage.
//
// It takes bytes rather than a struct on purpose: messages arriving from Redis
// are already JSON, so this avoids decoding and re-encoding them just to put a
// label on the outside. json.RawMessage means "embed these bytes verbatim".
func messageFrame(payload []byte) frame {
	return frame{Type: frameMessage, Message: json.RawMessage(payload)}
}
