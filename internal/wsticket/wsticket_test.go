package wsticket

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis gives each test its own in-memory Redis.
//
// miniredis rather than a real server or a mock. A mock would only assert that
// we called GETDEL, which proves nothing — the property under test is what
// GETDEL DOES, and a mock would happily agree with a broken implementation.
// miniredis actually stores and expires things, and it also lets a test move
// the clock forward, which is the only sane way to test a TTL.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t) // RunT closes it when the test finishes
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestMintThenRedeem(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)

	want := Holder{UserID: 42, Username: "omkar"}
	ticket, err := Mint(ctx, rdb, want)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if ticket == "" {
		t.Fatal("Mint returned an empty ticket")
	}

	got, err := Redeem(ctx, rdb, ticket)
	if err != nil {
		t.Fatalf("Redeem: %v", err)
	}
	if got != want {
		t.Errorf("Redeem = %+v, want %+v", got, want)
	}
}

// TestTicketIsSingleUse is the whole reason this package exists.
//
// The ticket travels in a URL and therefore lands in access logs — that was
// never avoidable. What makes it safe is that the value in the log has already
// been spent. If redeeming ever stopped destroying the ticket, everything else
// would keep working perfectly and B1 would be quietly reopened: a logged
// ticket would be replayable for its full 30 seconds.
//
// This is also the test that guards the ATOMICITY of GETDEL. Someone splitting
// it into a GET followed by a DEL "for clarity" would still pass this test
// sequentially — but see TestConcurrentRedeemYieldsExactlyOneWinner, which
// would not.
func TestTicketIsSingleUse(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)

	ticket, err := Mint(ctx, rdb, Holder{UserID: 1, Username: "omkar"})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	if _, err := Redeem(ctx, rdb, ticket); err != nil {
		t.Fatalf("first Redeem should succeed: %v", err)
	}

	_, err = Redeem(ctx, rdb, ticket)
	if err == nil {
		t.Fatal("second Redeem SUCCEEDED — the ticket is replayable, B1 is reopened")
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("second Redeem error = %v, want ErrInvalid", err)
	}
}

// TestConcurrentRedeemYieldsExactlyOneWinner is the test a non-atomic
// implementation fails.
//
// Two handshakes arriving together is not hypothetical — it is what a replay
// attack looks like, and what a client retrying a flaky connection does by
// accident. With GET then DEL as separate commands, both goroutines can read
// the value before either deletes it, and both are admitted.
func TestConcurrentRedeemYieldsExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)

	ticket, err := Mint(ctx, rdb, Holder{UserID: 1, Username: "omkar"})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	const attempts = 8
	results := make(chan error, attempts)
	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		go func() {
			<-start // release them all at once, to maximise the overlap
			_, err := Redeem(ctx, rdb, ticket)
			results <- err
		}()
	}
	close(start)

	winners := 0
	for i := 0; i < attempts; i++ {
		if err := <-results; err == nil {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d of %d concurrent redemptions succeeded, want exactly 1", winners, attempts)
	}
}

func TestRedeemRejects(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)

	for _, tc := range []struct{ name, ticket string }{
		{"empty", ""},
		{"never minted", "Zm9vYmFyYmF6cXV4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Redeem(ctx, rdb, tc.ticket)
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Redeem(%q) = %v, want ErrInvalid", tc.ticket, err)
			}
		})
	}
}

// TestTicketExpires checks the second half of "worth nothing by the time it is
// logged" — a ticket that was minted and never used must not sit in Redis
// indefinitely waiting for someone to find it in a log file.
func TestTicketExpires(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	ticket, err := Mint(ctx, rdb, Holder{UserID: 1, Username: "omkar"})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Moving miniredis's clock rather than sleeping: a test that actually waits
	// 30 seconds is a test people delete.
	mr.FastForward(TTL + time.Second)

	if _, err := Redeem(ctx, rdb, ticket); !errors.Is(err, ErrInvalid) {
		t.Errorf("Redeem after TTL = %v, want ErrInvalid — the ticket never expired", err)
	}
}

// TestTicketsAreUnique is cheap insurance on the randomness.
//
// It cannot prove crypto/rand is being used — a broken generator could still
// produce 64 distinct values — but it does catch the failure that matters in
// practice: a constant, a counter, or a value derived from the user id, any of
// which would make one user's ticket guessable from another's.
func TestTicketsAreUnique(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)

	seen := make(map[string]bool, 64)
	for i := 0; i < 64; i++ {
		ticket, err := Mint(ctx, rdb, Holder{UserID: 1, Username: "omkar"})
		if err != nil {
			t.Fatalf("Mint: %v", err)
		}
		if seen[ticket] {
			t.Fatalf("Mint produced a duplicate ticket after %d calls", i)
		}
		seen[ticket] = true
	}
}
