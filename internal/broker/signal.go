package broker

import "encoding/json"

// Signal is one WebRTC signalling note travelling between two people in a room.
//
// The server is a DUMB RELAY here, and deliberately so. Data holds the browser's
// session description or ICE candidate as opaque bytes — the gateway never parses
// SDP, never inspects a candidate, and has no idea what codecs were agreed. It
// only knows who sent it and who should get it.
//
// That matters for two reasons. WebRTC's negotiation format evolves, and a relay
// that doesn't parse it never needs updating when it does. And the media itself
// never touches the server at all: once these notes have been exchanged the two
// browsers talk directly, so anything the server understood would be pointless.
type Signal struct {
	RoomID int64 `json:"room_id"`
	From   int64 `json:"from"`
	To     int64 `json:"to"`

	// Kind is what sort of note this is: "offer", "answer", "ice" or "hangup".
	// The server passes it through without acting on it; only the browsers care.
	Kind string `json:"kind"`

	// Data is whatever the browser put in it. Opaque.
	Data json.RawMessage `json:"data,omitempty"`
}
