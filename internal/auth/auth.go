package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// HashPassword turns a plaintext password into a bcrypt hash safe to store.
func HashPassword(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// CheckPassword returns nil if the plaintext matches the stored bcrypt hash,
// or an error if it doesn't.
func CheckPassword(hash, plain string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain))
}

// Claims is the payload we embed inside the JWT: who the user is, plus the
// standard registered fields (expiry, issued-at).
type Claims struct {
	UserID   int64  `json:"uid"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// GenerateToken signs a JWT for a user, valid for 24 hours, using HS256.
func GenerateToken(userID int64, username, secret string) (string, error) {
	claims := Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// VerifyToken parses and validates a JWT string and returns its claims.
func VerifyToken(tokenStr, secret string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		// Guard against algorithm-confusion attacks: only accept HMAC-signed tokens.
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	return claims, nil
}
