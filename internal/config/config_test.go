package config

import "testing"

// TestRequireJWTSecret pins the fail-closed behaviour that blocker B3 was about.
//
// The old default was a working placeholder, and the danger was that nothing
// misbehaved: every instance fell back to the SAME string, so logins succeeded
// and tokens minted on one gateway validated on another, while a value
// published in a public repository signed every token in the system. A
// misconfiguration that behaves perfectly is worse than one that crashes,
// because nothing ever prompts you to go and look.
//
// The chart relies on this too — helm lint runs once WITHOUT jwtSecret in CI
// specifically so the template's own guard is exercised. This test covers the
// other half, in the process itself.
func TestRequireJWTSecret(t *testing.T) {
	tests := []struct {
		name    string
		secret  string
		wantErr bool
	}{
		{"unset", "", true},
		{"the placeholder itself", devJWTSecret, true},
		{"a real value", "6f1c6a1f0e6b4c0a9d2e5f8a3b7c1d4e", false},
		// Not rejected, and deliberately so: refusing short or low-entropy keys
		// would mean inventing a strength rule, and the failure this guard
		// exists for is a shared DEFAULT, not a weak choice.
		{"short but chosen", "x", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Config{JWTSecret: tc.secret}.RequireJWTSecret()
			if tc.wantErr && err == nil {
				t.Errorf("RequireJWTSecret(%q) = nil, want an error", tc.secret)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("RequireJWTSecret(%q) = %v, want nil", tc.secret, err)
			}
		})
	}
}

// TestGetenvListIsNilWhenUnset matters more than it looks.
//
// nil means "same-origin only" to websocket.Accept. A slice holding one empty
// string would be a pattern matching nothing — which behaves the same by luck,
// looks identical in a config dump, and would quietly diverge if the library
// ever treated a present-but-empty list as "allow all".
func TestGetenvListIsNilWhenUnset(t *testing.T) {
	if got := getenvList("CHAT_GO_DEFINITELY_UNSET_VARIABLE"); got != nil {
		t.Errorf("getenvList on an unset variable = %#v, want nil", got)
	}

	t.Setenv("CHAT_GO_TEST_ORIGINS", " a.example.com , ,b.example.com ")
	got := getenvList("CHAT_GO_TEST_ORIGINS")
	want := []string{"a.example.com", "b.example.com"}
	if len(got) != len(want) {
		t.Fatalf("getenvList = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %q, want %q — blanks must be dropped and spaces trimmed",
				i, got[i], want[i])
		}
	}
}
