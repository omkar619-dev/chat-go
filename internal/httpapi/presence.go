package httpapi

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/omkar619-dev/chat-go/internal/hub"
	"github.com/omkar619-dev/chat-go/internal/presence"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// announcePresence tells every client in the room to re-read the online list.
//
// Fire and forget, and it logs rather than returning an error. A nudge that
// fails to send means somebody's list is a few seconds stale until their next
// periodic re-read picks it up — not something worth failing a connection over.
//
// Published to a channel of its own rather than added to the chat channel, so no
// consumer has to inspect a payload to work out what kind of thing it just got.
func (h *Handlers) announcePresence(ctx context.Context, roomID int64) {
	pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := h.Redis.Publish(pubCtx, hub.PresenceChannel(roomID), "changed").Err(); err != nil {
		log.Printf("presence notify (room %d): %v", roomID, err)
	}
}

// presenceResponse is the online list for one room.
type presenceResponse struct {
	Online []string `json:"online"`
}

// RoomPresence answers "who is online in this room right now?"
//
// The answer comes from Redis rather than from this process, on purpose. This
// gateway only knows about its own sockets; the other gateway's users are just
// as online. Redis is the shared place where every gateway writes its own list,
// so reading from there gives the complete picture from any instance.
func (h *Handlers) RoomPresence(w http.ResponseWriter, r *http.Request) {
	// 1. Which room?
	roomID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}

	// 2. Only members may look — the same gate as the WebSocket and /search.
	//    Without it, knowing who is in a private room would need no membership at
	//    all, which leaks the member list of every room to every account.
	userID, _ := UserIDFromContext(r.Context())
	member, err := h.Queries.IsRoomMember(r.Context(), sqlc.IsRoomMemberParams{
		RoomID: roomID,
		UserID: userID,
	})
	if err != nil || !member {
		writeError(w, http.StatusForbidden, "not a member of this room")
		return
	}

	// 3. Who has been seen recently? Redis down means we cannot answer, but chat
	//    itself is unaffected — so 503 ("temporarily unavailable"), not 500
	//    ("something is broken"). Same call as /search makes about Ollama.
	ids, err := presence.Online(r.Context(), h.Redis, roomID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "presence is unavailable")
		return
	}

	// An empty room is a normal answer, not an error. Return early so we don't
	// ask Postgres to look up nothing.
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, presenceResponse{Online: []string{}})
		return
	}

	// 4. Turn ids into names, in one query rather than one per person.
	names, err := h.Queries.ListUsernamesByIDs(r.Context(), ids)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read presence")
		return
	}

	// `[]string{}` and not a nil slice: a nil slice encodes as JSON `null`, and
	// the client would have to special-case it. An empty list is `[]`.
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, presenceResponse{Online: names})
}
