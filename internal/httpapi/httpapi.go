package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"

	"github.com/omkar619-dev/chat-go/internal/auth"
	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/embed"
	"github.com/omkar619-dev/chat-go/internal/hub"
	"github.com/omkar619-dev/chat-go/internal/llm"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// Handlers holds the dependencies every HTTP handler needs.
type Handlers struct {
	Queries   *sqlc.Queries
	JWTSecret string
	// Redis is still here for PUBLISH, which is an ordinary pooled command.
	// SUBSCRIBE goes through Hub instead — see internal/hub.
	Redis     *redis.Client
	Hub       *hub.Hub // one Redis subscription per room, fanned out in-process
	Producer  *broker.Producer
	Embedder  *embed.Client // embeds search queries; only /search needs it
	Generator *llm.Client   // streams "catch me up" summaries down the WebSocket
}

// credentials is the JSON body for both /register and /login.
type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Register creates a new user and returns a signed JWT.
func (h *Handlers) Register(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if c.Username == "" || c.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}

	hash, err := auth.HashPassword(c.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not hash password")
		return
	}

	user, err := h.Queries.CreateUser(r.Context(), sqlc.CreateUserParams{
		Username:     c.Username,
		PasswordHash: hash,
	})
	if err != nil {
		// A unique-constraint violation (code 23505) means the username is taken.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			writeError(w, http.StatusConflict, "username already taken")
			return
		}
		writeError(w, http.StatusInternalServerError, "could not create user")
		return
	}

	token, err := auth.GenerateToken(user.ID, user.Username, h.JWTSecret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create token")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"token": token})
}

// Login verifies credentials and returns a signed JWT.
func (h *Handlers) Login(w http.ResponseWriter, r *http.Request) {
	var c credentials
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	user, err := h.Queries.GetUserByUsername(r.Context(), c.Username)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Same generic message whether the user is missing OR the password is wrong,
			// so we never leak which usernames exist.
			writeError(w, http.StatusUnauthorized, "invalid username or password")
			return
		}
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	if err := auth.CheckPassword(user.PasswordHash, c.Password); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}

	token, err := auth.GenerateToken(user.ID, user.Username, h.JWTSecret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

// --- tiny JSON response helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
