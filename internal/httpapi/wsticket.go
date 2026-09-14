package httpapi

import (
	"net/http"

	"github.com/omkar619-dev/chat-go/internal/wsticket"
)

// WSTicket mints a single-use ticket for opening a WebSocket.
//
// This route sits behind RequireAuth like every other protected endpoint, so
// the JWT arrives in an Authorization HEADER — which is the whole point. An
// ordinary HTTP request can carry one; a browser's WebSocket handshake cannot.
// So the client authenticates properly here, once, and receives something
// cheap it is allowed to put in a URL.
//
// It escalates nothing: the caller already holds a valid token for this
// identity, and the ticket is strictly weaker than the token it came from —
// one use, thirty seconds, and no authority beyond opening a socket.
func (h *Handlers) WSTicket(w http.ResponseWriter, r *http.Request) {
	userID, ok := UserIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	username, _ := UsernameFromContext(r.Context())

	ticket, err := wsticket.Mint(r.Context(), h.Redis, wsticket.Holder{
		UserID:   userID,
		Username: username,
	})
	if err != nil {
		// Redis is the only store for tickets, so if it is unreachable nobody
		// can open a socket at all. 503 rather than 500: this is a dependency
		// being down, not a bug here, and the client should retry rather than
		// conclude its credentials are wrong.
		writeError(w, http.StatusServiceUnavailable, "could not issue ticket")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"ticket": ticket})
}
