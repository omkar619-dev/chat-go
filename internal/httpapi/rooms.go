package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// roomResponse is the clean JSON shape we return, so we don't leak DB-internal
// types (like pgtype.Timestamptz) into the API.
type roomResponse struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func toRoomResponse(r sqlc.Room) roomResponse {
	return roomResponse{ID: r.ID, Name: r.Name}
}

// CreateRoom creates a room and auto-joins the creator as a member.
func (h *Handlers) CreateRoom(w http.ResponseWriter, r *http.Request) {
	userID, _ := UserIDFromContext(r.Context())

	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "room name is required")
		return
	}

	room, err := h.Queries.CreateRoom(r.Context(), sqlc.CreateRoomParams{
		Name:      body.Name,
		CreatedBy: userID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create room")
		return
	}

	// The creator is automatically a member of their own room.
	if err := h.Queries.AddRoomMember(r.Context(), sqlc.AddRoomMemberParams{
		RoomID: room.ID,
		UserID: userID,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "could not add creator to room")
		return
	}

	writeJSON(w, http.StatusCreated, toRoomResponse(room))
}

// ListRooms returns every room, so a user can see what to join.
func (h *Handlers) ListRooms(w http.ResponseWriter, r *http.Request) {
	rooms, err := h.Queries.ListRooms(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list rooms")
		return
	}
	// make(...,0,len) so an empty result marshals to [] not null.
	out := make([]roomResponse, 0, len(rooms))
	for _, room := range rooms {
		out = append(out, toRoomResponse(room))
	}
	writeJSON(w, http.StatusOK, out)
}

// JoinRoom adds the authenticated user to a room.
func (h *Handlers) JoinRoom(w http.ResponseWriter, r *http.Request) {
	userID, _ := UserIDFromContext(r.Context())

	roomID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}

	err = h.Queries.AddRoomMember(r.Context(), sqlc.AddRoomMemberParams{
		RoomID: roomID,
		UserID: userID,
	})
	if err != nil {
		// A foreign-key violation (23503) means that room id doesn't exist.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			writeError(w, http.StatusNotFound, "room not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "could not join room")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "joined"})
}
