package httpapi

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"
)

// iceServer mirrors the browser's RTCIceServer shape exactly, so the response
// can be handed straight to RTCPeerConnection without translation.
type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type iceConfigResponse struct {
	ICEServers []iceServer `json:"iceServers"`
}

// ICEConfig tells the browser where to find STUN and TURN.
//
// This used to be a constant in index.html. It moved here because the TURN
// server runs ON DEMAND on a cheap cloud instance with no fixed address — it
// gets a new public IP every time it starts. Hardcoded, that meant editing the
// page and rebuilding the binary after every start. As config, it is a restart.
//
// Which is where a server address belonged anyway: the browser should be told
// what the deployment looks like, not ship with an assumption about it baked in.
func (h *Handlers) ICEConfig(w http.ResponseWriter, r *http.Request) {
	userID, _ := UserIDFromContext(r.Context())

	// STUN is always available and needs no credentials — it hands back "this is
	// what your address looks like from out here" and relays nothing, which is
	// why public servers are free to use.
	servers := []iceServer{{URLs: []string{h.StunURL}}}

	// TURN only if a relay is actually configured. With it unset the browser
	// still gets a usable config and simply cannot fall back to a relay — which
	// is the honest state of things rather than a broken-looking error.
	if h.TurnURL != "" && h.TurnSecret != "" {
		username, credential := turnCredentials(h.TurnSecret, userID, h.TurnTTL)
		servers = append(servers, iceServer{
			// Both transports offered. UDP is what you want; TCP is the fallback
			// for networks that block UDP outright, which is common on corporate
			// and hotel WiFi — exactly the networks a relay exists for.
			URLs: []string{
				h.TurnURL + "?transport=udp",
				h.TurnURL + "?transport=tcp",
			},
			Username:   username,
			Credential: credential,
		})
	}

	writeJSON(w, http.StatusOK, iceConfigResponse{ICEServers: servers})
}

// turnCredentials mints a short-lived username and password for coturn.
//
// The relay has no user database and never talks to ours. Instead both sides
// hold one shared secret, and the "password" is just an HMAC of the username,
// which coturn recomputes to check. That is coturn's use-auth-secret mode, and
// it means the relay needs to know nothing about who our users are.
//
// The username is "<expiry unix time>:<user id>", and coturn refuses it once
// that time has passed. So a credential that leaks is worthless within hours,
// and nobody keeps relay access forever just because they once had an account.
// The user id is in there only so relay usage can be attributed in coturn's
// logs; coturn itself does not interpret that half.
//
// SHA-1 is not a choice — the TURN REST convention specifies it, and coturn
// computes the same thing. It is a shared-secret MAC over a short public string,
// not a password hash, so SHA-1's collision weaknesses do not apply here.
//
// The secret itself NEVER goes to the browser. Only the derived, expiring pair.
func turnCredentials(secret string, userID int64, ttl time.Duration) (string, string) {
	username := fmt.Sprintf("%d:%d", time.Now().Add(ttl).Unix(), userID)

	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(username))
	return username, base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
