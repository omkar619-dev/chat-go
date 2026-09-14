// Package wsticket issues and redeems the short-lived, single-use tickets that
// let a browser open a WebSocket without putting a JWT in the URL.
//
// The problem it exists for: a browser's WebSocket constructor CANNOT set
// request headers. There is no way to send `Authorization: Bearer <jwt>` on the
// handshake, so the token was smuggled into the query string instead — and
// query strings are written to every access log on the path. chi logged one.
// Traefik logs another. Any log shipper or APM agent would hold a working
// 24-hour credential for every user who ever connected.
//
// The fix is not to hide the token better but to send something that is not
// worth stealing. A ticket is opaque, random, valid for 30 seconds, and dies
// the first time it is used — so by the time it reaches a log file it has
// already been spent by the legitimate client.
package wsticket

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TTL is how long a minted ticket stays redeemable.
//
// This is the window between "the page asked for a ticket" and "the browser
// opened the socket", which is one round trip. Thirty seconds is generous for
// that and still short enough that a ticket in a log is stale before anyone
// reads the log. It is not a session lifetime — the JWT remains the session,
// and this is only the handoff.
const TTL = 30 * time.Second

// ErrInvalid covers every failure that should look identical from outside:
// never existed, expired, already redeemed, or simply wrong. Distinguishing
// them in the response would tell an attacker which guesses were close.
var ErrInvalid = errors.New("ticket is invalid, expired, or already used")

// Holder is what redeeming a ticket yields — the identity the JWT carried,
// handed across without the JWT itself travelling in a URL.
type Holder struct {
	UserID   int64  `json:"uid"`
	Username string `json:"username"`
}

func key(ticket string) string { return "ws:ticket:" + ticket }

// Mint creates a ticket for a user and stores it against their identity.
//
// The caller must already have authenticated them normally — this converts an
// existing, header-borne credential into a URL-safe one. It grants nothing new.
func Mint(ctx context.Context, rdb *redis.Client, h Holder) (string, error) {
	// 32 bytes from crypto/rand, not math/rand. This value is a bearer
	// credential for its 30 seconds, and a predictable one would let an
	// attacker guess a ticket that has been minted but not yet used.
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate ticket: %w", err)
	}
	// RawURLEncoding: no padding, and no characters that need escaping in a
	// query string. A ticket that has to be percent-encoded is a ticket
	// somebody will eventually compare before decoding.
	ticket := base64.RawURLEncoding.EncodeToString(raw)

	payload, err := json.Marshal(h)
	if err != nil {
		return "", fmt.Errorf("encode ticket: %w", err)
	}

	// The TTL is set with the value, in one command. Storing first and
	// expiring second would leave a window where a crash between the two
	// leaves an immortal ticket in Redis.
	if err := rdb.Set(ctx, key(ticket), payload, TTL).Err(); err != nil {
		return "", fmt.Errorf("store ticket: %w", err)
	}
	return ticket, nil
}

// Redeem exchanges a ticket for the identity it was minted against, and
// destroys it in the same operation.
//
// GETDEL is the point of the whole design and it must stay atomic. With a
// separate GET then DEL, two handshakes arriving together could both read the
// value before either deleted it, and a captured ticket would be replayable for
// as long as the race window — which is exactly the property being removed.
// Redis executes commands one at a time, so GETDEL cannot interleave.
func Redeem(ctx context.Context, rdb *redis.Client, ticket string) (Holder, error) {
	if ticket == "" {
		return Holder{}, ErrInvalid
	}

	payload, err := rdb.GetDel(ctx, key(ticket)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Holder{}, ErrInvalid
	}
	if err != nil {
		// A Redis failure FAILS CLOSED: no ticket can be redeemed, so no socket
		// opens. That is the opposite of the bot's rate limiter, which fails
		// open when Redis is down — and deliberately so. That limiter guards
		// COST; this guards ACCESS. When the thing you are protecting is
		// spending, erring towards letting people through is kind. When it is
		// authorisation, it is a hole.
		return Holder{}, fmt.Errorf("redeem ticket: %w", err)
	}

	var h Holder
	if err := json.Unmarshal(payload, &h); err != nil {
		return Holder{}, fmt.Errorf("decode ticket: %w", err)
	}
	return h, nil
}
