package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "test-secret-not-used-anywhere-real"

func TestTokenRoundTrip(t *testing.T) {
	token, err := GenerateToken(42, "omkar", testSecret)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	claims, err := VerifyToken(token, testSecret)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if claims.UserID != 42 {
		t.Errorf("UserID = %d, want 42", claims.UserID)
	}
	if claims.Username != "omkar" {
		t.Errorf("Username = %q, want %q", claims.Username, "omkar")
	}
}

// TestVerifyTokenRejectsAlgNone is the reason the signing-method check exists.
//
// A JWT names its own algorithm in its header, and "none" is a legal value
// meaning "no signature". A verifier that trusts that header will happily
// accept a token the attacker wrote themselves, with any uid they like —
// authentication removed entirely by editing one field.
//
// VerifyToken defends against this by asserting the method is *SigningMethodHMAC
// BEFORE returning the key. This test forges exactly that token and requires it
// to be refused. If somebody ever "simplifies" that type assertion away, every
// other test here still passes and this one does not.
func TestVerifyTokenRejectsAlgNone(t *testing.T) {
	claims := Claims{
		UserID:   1,
		Username: "attacker",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	// jwt-go requires an explicit magic constant to sign with "none", precisely
	// because it is never what you want. That it takes effort to construct is
	// the point: an attacker only has to do it once.
	forged, err := jwt.NewWithClaims(jwt.SigningMethodNone, claims).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("could not forge an alg=none token: %v", err)
	}

	if _, err := VerifyToken(forged, testSecret); err == nil {
		t.Fatal("VerifyToken ACCEPTED an alg=none token — the signing-method guard is gone")
	}
}

func TestVerifyTokenRejects(t *testing.T) {
	valid, err := GenerateToken(7, "bob", testSecret)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	// An expired token cannot come from GenerateToken, which hardcodes 24 hours,
	// so it is signed by hand — with the CORRECT secret, so that expiry is the
	// only thing wrong with it.
	expired, err := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		UserID: 7, Username: "bob",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		},
	}).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("signing expired token: %v", err)
	}

	tests := []struct {
		name  string
		token string
		// what the case is actually asserting, so a failure message says why it
		// mattered rather than just which line broke
		why string
	}{
		{"empty", "", "no token at all must not authenticate anyone"},
		{"garbage", "not-a-jwt", "malformed input must not panic or pass"},
		{"wrong secret", mustSign(t, 7, "bob", "a-different-secret"),
			"a token signed by another instance's key must not be honoured"},
		{"tampered payload", tamper(valid),
			"editing the claims must invalidate the signature"},
		{"expired", expired, "past ExpiresAt must be refused even with a valid signature"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := VerifyToken(tc.token, testSecret); err == nil {
				t.Errorf("VerifyToken accepted %s — %s", tc.name, tc.why)
			}
		})
	}
}

func TestPasswordHashing(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if hash == "correct horse battery staple" {
		t.Fatal("HashPassword returned the plaintext")
	}
	if err := CheckPassword(hash, "correct horse battery staple"); err != nil {
		t.Errorf("CheckPassword rejected the right password: %v", err)
	}
	if err := CheckPassword(hash, "wrong password"); err == nil {
		t.Error("CheckPassword accepted the wrong password")
	}
}

// TestPasswordHashesAreSalted guards a property people assume rather than check.
//
// bcrypt generates a fresh random salt per call, so the same password hashed
// twice gives two different strings. That is what stops a stolen table being
// scanned for users who share a password, and what makes rainbow tables useless.
func TestPasswordHashesAreSalted(t *testing.T) {
	a, err := HashPassword("same password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	b, err := HashPassword("same password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if a == b {
		t.Fatal("two hashes of the same password are identical — no salt")
	}
}

func mustSign(t *testing.T, uid int64, name, secret string) string {
	t.Helper()
	token, err := GenerateToken(uid, name, secret)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	return token
}

// tamper flips a character in the PAYLOAD segment, leaving the signature as it
// was — which is exactly what an attacker editing their own uid would produce.
func tamper(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(parts[1]) == 0 {
		return token + "x"
	}
	b := []byte(parts[1])
	if b[0] == 'a' {
		b[0] = 'b'
	} else {
		b[0] = 'a'
	}
	return parts[0] + "." + string(b) + "." + parts[2]
}
