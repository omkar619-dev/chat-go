// Command loadtest opens N concurrent WebSocket connections to a chat-go gateway
// and holds them open, reporting how many survived.
//
// Step 1 of the Phase 6.2 load test. It deliberately measures nothing else.
// Before asking "how does Redis behave at N connections" you have to establish
// that N connections can exist at all — and, more importantly, that a failure at
// N is the GATEWAY's ceiling rather than the client's own file-descriptor or
// ephemeral-port ceiling. Those two look identical from the outside and mean
// opposite things.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
)

func main() {
	base := flag.String("base", "http://localhost:8090", "gateway base URL (through nginx: http://localhost:8088)")
	conns := flag.Int("conns", 100, "number of concurrent WebSocket connections")
	user := flag.String("user", "omkar", "user to log in as")
	pass := flag.String("pass", "secret123", "password")
	room := flag.Int64("room", 1, "room to join")
	ramp := flag.Duration("ramp", 5*time.Millisecond, "pause between dials")
	hold := flag.Duration("hold", 30*time.Second, "how long to hold the connections open")
	publish := flag.Int("publish", 0, "messages to publish once connections settle (0 = skip the fan-out measurement)")
	rate := flag.Int("rate", 10, "messages per second while publishing")
	redisAddr := flag.String("redis", "127.0.0.1:6381", "redis address, read-only, for INFO sampling")
	flag.Parse()

	// Ctrl+C cancels everything at once: the dial loop, every read loop, the hold.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// ONE login, one token, reused by every connection.
	//
	// bcrypt is deliberately slow — that is its whole purpose — so N logins would
	// cost N × ~100ms and we would be measuring password hashing rather than the
	// socket layer. Reusing one token is also faithful to what the WS handler
	// actually does: verify a signature and check room membership, neither of
	// which varies per connection.
	//
	// The cost is that this does NOT exercise per-user work — distinct read
	// watermarks, distinct history. That is a stated limitation, not an oversight.
	token, err := login(ctx, *base, *user, *pass)
	if err != nil {
		log.Fatalf("login: %v", err)
	}
	log.Printf("logged in as %q — opening %d connections to %s", *user, *conns, *base)

	// http -> ws, https -> wss. Replacing only the FIRST occurrence leaves any
	// "http" inside a path or query string alone.
	wsURL := strings.Replace(*base, "http", "ws", 1) +
		fmt.Sprintf("/ws?token=%s&room=%d", token, *room)

	// The hold is a context rather than a timer so that it reaches every blocked
	// Read simultaneously. conn.Read on a silent room blocks indefinitely, so a
	// timer the read loop only checks between messages would never fire.
	holdCtx, cancelHold := context.WithTimeout(ctx, *hold)
	defer cancelHold()

	var (
		open   atomic.Int64 // connections currently established
		failed atomic.Int64 // dials that never succeeded
		frames atomic.Int64 // frames received across all connections
		wg     sync.WaitGroup
	)

	go progress(holdCtx, &open, &failed, &frames)

	started := time.Now()
	for i := 0; i < *conns; i++ {
		if holdCtx.Err() != nil {
			break // Ctrl+C, or the hold expired while we were still ramping
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			c, _, err := websocket.Dial(holdCtx, wsURL, nil)
			if err != nil {
				// Only the first few. Otherwise 1000 failures print 1000 identical
				// lines and bury whatever else the run was trying to tell you.
				if n := failed.Add(1); n <= 5 {
					log.Printf("conn %d: dial failed: %v", id, err)
				}
				return
			}
			defer c.CloseNow()

			open.Add(1)
			defer open.Add(-1)

			// Drain and count. Every connection receives the room's history replay
			// on connect and then whatever is published afterwards. We never decode
			// — parsing N×M frames would make the CLIENT the bottleneck, which is
			// the one thing this test must not measure.
			for {
				if _, _, err := c.Read(holdCtx); err != nil {
					return
				}
				frames.Add(1)
			}
		}(i)

		// Ramp rather than a thundering herd. A thousand simultaneous dials fail
		// for reasons that have nothing to do with capacity — SYN backlog, accept
		// queue overflow — and you would wrongly blame the gateway.
		time.Sleep(*ramp)
	}

	rampTook := time.Since(started)

	// The fan-out measurement. Runs while every subscriber is still connected,
	// then releases the hold so the read loops exit and wg.Wait() returns.
	if *publish > 0 {
		publishPhase(holdCtx, wsURL, *redisAddr, *publish, *rate, int(int64(*conns)-failed.Load()), &frames)
		cancelHold()
	}

	wg.Wait()

	established := int64(*conns) - failed.Load()
	log.Printf("─────────────────────────")
	log.Printf("requested:    %d", *conns)
	log.Printf("established:  %d", established)
	log.Printf("dial failed:  %d", failed.Load())
	log.Printf("ramp took:    %s", rampTook.Round(time.Millisecond))
	log.Printf("frames recvd: %d  (~%d per connection)", frames.Load(), safeDiv(frames.Load(), established))
}

// publishPhase measures what a single PUBLISH actually costs Redis.
//
// The subscribers are already connected and idle. We snapshot Redis's own byte
// counters, send `count` messages down ONE extra connection, wait for delivery
// to stop, and snapshot again.
//
// The number that matters is bytes-out per published message:
//
//	one subscription per GATEWAY    -> roughly one message, flat as subscribers grow
//	one subscription per CONNECTION -> one message × subscribers, linear
//
// Reading Redis's counters rather than timing anything is deliberate. A ratio is
// immune to how loaded the machine is — a contended CPU slows everything
// uniformly, it does not turn 500 deliveries into 250 — which is what makes this
// measurement trustworthy on a laptop that is also running the client.
func publishPhase(ctx context.Context, wsURL, redisAddr string, count, rate, subs int, frames *atomic.Int64) {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// Let history replay finish first. Every connection is handed ~50 messages
	// from Postgres on connect, and attributing those to the publish would
	// inflate the result by a factor of fifty.
	waitQuiet(ctx, frames, 3*time.Second)

	before, err := redisCounters(ctx, rdb)
	if err != nil {
		log.Printf("redis INFO: %v", err)
		return
	}
	baseFrames := frames.Load()

	pub, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		log.Printf("publisher dial: %v", err)
		return
	}
	defer pub.CloseNow()

	// The publisher is itself a member of the room, so the server pushes every
	// message back to it. Nothing reads that socket otherwise, and an unread
	// socket eventually stalls the writer — so drain and discard.
	go func() {
		for {
			if _, _, err := pub.Read(ctx); err != nil {
				return
			}
		}
	}()

	// Fixed-size body so bytes-per-message is comparable across runs.
	body := strings.Repeat("x", 40)
	interval := time.Second / time.Duration(rate)
	log.Printf("publishing %d messages at %d/s to %d subscribers", count, rate, subs)

	sent := 0
	for i := 0; i < count; i++ {
		if err := pub.Write(ctx, websocket.MessageText, []byte(body)); err != nil {
			log.Printf("publish %d: %v", i, err)
			break
		}
		sent++
		time.Sleep(interval)
	}
	if sent == 0 {
		return
	}

	waitQuiet(ctx, frames, 3*time.Second)

	after, err := redisCounters(ctx, rdb)
	if err != nil {
		log.Printf("redis INFO: %v", err)
		return
	}

	outDelta := after.outBytes - before.outBytes
	cmdDelta := after.commands - before.commands
	delivered := frames.Load() - baseFrames

	log.Printf("─── fan-out ─────────────")
	log.Printf("subscribers:          %d", subs)
	log.Printf("messages published:   %d", sent)
	log.Printf("frames delivered:     %d  (expected %d)", delivered, int64(sent)*int64(subs))
	log.Printf("redis commands:       %d", cmdDelta)
	log.Printf("redis bytes out:      %d", outDelta)
	log.Printf("bytes per message:    %d", outDelta/int64(sent))
	log.Printf("  ... per subscriber: %d   <-- roughly CONSTANT across N means bytes scale WITH N",
		outDelta/int64(sent)/int64(max(subs, 1)))
}

// waitQuiet blocks until the frame counter stops moving for `quiet`. Without it
// one phase drains into the next and the measurement includes work it did not do.
func waitQuiet(ctx context.Context, frames *atomic.Int64, quiet time.Duration) {
	last := frames.Load()
	stable := time.Now()
	for ctx.Err() == nil {
		time.Sleep(250 * time.Millisecond)
		if n := frames.Load(); n != last {
			last, stable = n, time.Now()
			continue
		}
		if time.Since(stable) >= quiet {
			return
		}
	}
}

type counters struct {
	outBytes int64
	commands int64
}

// redisCounters reads the two cumulative counters we diff around the publish.
func redisCounters(ctx context.Context, rdb *redis.Client) (counters, error) {
	info, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return counters{}, err
	}
	return counters{
		outBytes: infoInt(info, "total_net_output_bytes"),
		commands: infoInt(info, "total_commands_processed"),
	}, nil
}

// infoInt pulls one "key:value" line out of an INFO section.
func infoInt(info, key string) int64 {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), key+":"); ok {
			n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			return n
		}
	}
	return 0
}

// progress prints a live count every two seconds. Without it a long run is
// indistinguishable from a hang, and you cannot see WHERE connections start
// failing — which is the most informative part of the whole exercise.
func progress(ctx context.Context, open, failed, frames *atomic.Int64) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Printf("open=%d failed=%d frames=%d", open.Load(), failed.Load(), frames.Load())
		}
	}
}

// login exchanges credentials for a JWT. POST /login returns {"token": "..."}.
func login(ctx context.Context, base, user, pass string) (string, error) {
	body, err := json.Marshal(map[string]string{"username": user, "password": pass})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %s", resp.Status)
	}

	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("no token in login response")
	}
	return out.Token, nil
}

func safeDiv(a, b int64) int64 {
	if b == 0 {
		return 0
	}
	return a / b
}
