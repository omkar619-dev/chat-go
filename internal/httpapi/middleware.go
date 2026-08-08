package httpapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/omkar619-dev/chat-go/internal/auth"
)

// ctxKey is an unexported type for context keys, so no other package can
// accidentally collide with the keys we store.
type ctxKey string

const (
	userIDKey   ctxKey = "userID"
	usernameKey ctxKey = "username"
)

// RequireAuth is middleware that rejects requests without a valid JWT, and on
// success stashes the user's id + name into the request context for handlers.
func (h *Handlers) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(header, "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}
		claims, err := auth.VerifyToken(token, h.JWTSecret)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}
		ctx := context.WithValue(r.Context(), userIDKey, claims.UserID)
		ctx = context.WithValue(ctx, usernameKey, claims.Username)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// UserIDFromContext reads the authenticated user's id back out of the context.
func UserIDFromContext(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(userIDKey).(int64)
	return id, ok
}

// UsernameFromContext reads the authenticated user's name from the context.
func UsernameFromContext(ctx context.Context) (string, bool) {
	name, ok := ctx.Value(usernameKey).(string)
	return name, ok
}

// Me is a protected handler that echoes the current user — proves auth works.
func (h *Handlers) Me(w http.ResponseWriter, r *http.Request) {
	id, _ := UserIDFromContext(r.Context())
	name, _ := UsernameFromContext(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"uid": id, "username": name})
}
