package httpapi

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pgvector/pgvector-go"

	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// searchHit is one result. Deliberately does NOT expose the embedding — 384
// floats per row would be useless noise to a client.
type searchHit struct {
	Username   string    `json:"username"`
	Body       string    `json:"body"`
	SentAt     time.Time `json:"sent_at"`
	Similarity float64   `json:"similarity"` // 1.0 = identical meaning, 0 = unrelated
}

// searchResponse wraps the hits with the size of the index, so the UI can tell
// "nothing matched" apart from "nothing has been indexed yet" — two very
// different situations that would otherwise both look like an empty list.
type searchResponse struct {
	Query   string      `json:"query"`
	Indexed int64       `json:"indexed"`
	Results []searchHit `json:"results"`
}

// SearchRoom finds messages in a room by MEANING rather than by keyword.
//
// The trick is that both sides get turned into the same kind of thing: the
// indexer embedded every message, and here we embed the search query with the
// SAME model. Both are now points in the same 384-dimensional space, so
// "closest point" == "most similar meaning" — which is why this can match a
// message that shares no words at all with the query.
func (h *Handlers) SearchRoom(w http.ResponseWriter, r *http.Request) {
	// 1. Which room?
	roomID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}

	// 2. What are we looking for? Trim first, so "   " counts as empty.
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeError(w, http.StatusBadRequest, "missing search query")
		return
	}

	// 3. Only members may search a room — same gate as the WebSocket, because
	//    search would otherwise be a way to read a room you can't connect to.
	userID, _ := UserIDFromContext(r.Context())
	member, err := h.Queries.IsRoomMember(r.Context(), sqlc.IsRoomMemberParams{
		RoomID: roomID,
		UserID: userID,
	})
	if err != nil || !member {
		writeError(w, http.StatusForbidden, "not a member of this room")
		return
	}

	// 4. Turn the query text into a vector. This is the gateway's ONLY dependency
	//    on Ollama: if it's down, search returns 503 but live chat is unaffected.
	//    Shorter timeout than the indexer's — a human is waiting on this one.
	embedCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	vec, err := h.Embedder.Embed(embedCtx, query)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "search is unavailable (embedding service down)")
		return
	}

	// 5. Nearest-neighbour lookup, plus the index size for an honest empty state.
	indexed, err := h.Queries.CountMessageEmbeddings(r.Context(), roomID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search failed")
		return
	}
	rows, err := h.Queries.SearchMessages(r.Context(), sqlc.SearchMessagesParams{
		RoomID:         roomID,
		QueryEmbedding: pgvector.NewVector(vec),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search failed")
		return
	}

	// 6. Repackage DB rows into the API shape (drops pgtype wrappers and ids we
	//    don't want to leak). make(...,0,len) so an empty result marshals as []
	//    rather than null.
	hits := make([]searchHit, 0, len(rows))
	for _, row := range rows {
		hits = append(hits, searchHit{
			Username:   row.Username,
			Body:       row.Body,
			SentAt:     row.SentAt.Time,
			Similarity: row.Similarity,
		})
	}
	writeJSON(w, http.StatusOK, searchResponse{Query: query, Indexed: indexed, Results: hits})
}
