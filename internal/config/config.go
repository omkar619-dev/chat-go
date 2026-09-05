package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// devJWTSecret is the placeholder that used to be a silent default. It stays
// named here so the gateway can refuse it BY NAME: an unset secret and an
// explicitly configured placeholder are the same mistake and must fail alike.
const devJWTSecret = "dev-change-me"

// Config holds everything the gateway needs to start, read from the environment.
type Config struct {
	HTTPAddr    string // address the HTTP + WebSocket server listens on
	DatabaseURL string // Postgres connection string
	RedisAddr   string // Redis host:port
	JWTSecret   string // secret used to sign & verify JWTs (override in prod!)
	KafkaBroker string // Kafka broker host:port
	KafkaTopic  string // Kafka topic that chat messages are written to
	OllamaURL   string // Ollama base URL (serves both embeddings and generation)
	EmbedModel  string // embedding model name (must match the schema's vector size)
	ChatModel   string // generation model name, used by the RAG bot
	HubBuffer   int    // messages that may queue for one socket before the hub drops it

	// Bot load shedding. Env-tunable rather than constants because these are
	// exactly the values you want to make extreme in order to PROVE the shedding
	// paths run — and editing constants to test them means remembering to change
	// them back, which is how a test value reaches production.
	BotWorkers    int           // concurrent answers; more does not mean faster, Ollama serialises
	BotQueue      int           // questions that may wait before new ones are shed
	BotMaxAge     time.Duration // a question older than this when picked up is binned
	BotRatePerMin int           // per-user mention budget

	// WebRTC ICE. Served to the browser at /ice-config rather than baked into
	// the page, because the TURN server runs ON DEMAND on a cheap cloud instance
	// and gets a new public address every time it starts. A server address in
	// the HTML would mean a rebuild after every start; here it is a restart.
	StunURL    string        // always set; STUN only reports your public address
	TurnURL    string        // e.g. turn:13.1.2.3:3478 — EMPTY means no relay configured
	TurnSecret string        // shared with coturn's static-auth-secret; never sent to the browser
	TurnTTL    time.Duration // how long a minted TURN credential stays valid
}

// Load reads config from environment variables, falling back to local-dev
// defaults that match our docker-compose ports (5433 / 6381). This means
// `go run` Just Works locally with no .env file needed.
func Load() Config {
	return Config{
		HTTPAddr: getenv("HTTP_ADDR", ":8090"), // :8080 is taken by the local Atlassian app
		// 127.0.0.1, NOT localhost, and deliberately so.
		//
		// Go resolves "localhost" to ::1 (IPv6) first. On Windows with Docker's WSL2
		// backend, com.docker.backend binds the wildcard :: while wslrelay.exe binds
		// the specific ::1 — and Windows routes to the most specific listener. So
		// "localhost" reaches wslrelay, which ACCEPTS the TCP connection and then
		// never forwards it. Every reachability check passes; every real connection
		// hangs until its deadline. Cost half an evening to find, because a listener
		// that accepts and does nothing is invisible to anything short of speaking
		// the actual protocol. Pinning IPv4 sidesteps the whole race.
		DatabaseURL: getenv("DATABASE_URL", "postgres://chat:chat_dev@127.0.0.1:5433/chat?sslmode=disable"),
		RedisAddr:   getenv("REDIS_ADDR", "127.0.0.1:6381"),
		JWTSecret:   getenv("JWT_SECRET", devJWTSecret),
		KafkaBroker: getenv("KAFKA_BROKER", "127.0.0.1:9094"), // IPv4 for the same reason as above
		KafkaTopic:  getenv("KAFKA_TOPIC", "chat.messages"),
		// Ollama runs on the homelab Old PC. This address is a DHCP lease that moved
		// three times (192.168.1.105 → 192.168.0.232 → 192.168.29.123 → this one),
		// which is why it's now RESERVED in the router against the NIC's MAC — so
		// it no longer drifts. Briefly used the tailnet address 100.72.182.75
		// instead, which never changes but adds a dependency on Tailscale being up
		// at both ends; that dependency promptly timed out mid-request, so the
		// pinned LAN address wins: fewer moving parts, lower latency.
		// Override with OLLAMA_URL — use the tailnet address from off-LAN, or
		// localhost if you run Ollama on this machine.
		OllamaURL: getenv("OLLAMA_URL", "http://192.168.29.30:11434"),
		// all-minilm outputs 384 numbers, matching vector(384) in schema.sql.
		// Changing the model means changing that column AND re-indexing everything.
		EmbedModel: getenv("EMBED_MODEL", "all-minilm"),
		// Generation model for the RAG bot. Unlike EMBED_MODEL, changing this is
		// cheap — nothing stored depends on it (no vectors to re-compute), so it's
		// safe to swap while hunting for one that's fast enough on the host.
		ChatModel: getenv("CHAT_MODEL", "qwen2.5:1.5b"),
		// How far one slow socket may fall behind before the hub disconnects it.
		// Env-tunable on purpose: this is the first number in the system that
		// should be chosen by MEASUREMENT rather than taste, and cmd/loadtest can
		// now sweep it. Too small and ordinary network hiccups evict people; too
		// large and a stalled socket holds memory for messages it will never read.
		HubBuffer: getenvInt("HUB_BUFFER", 64),

		BotWorkers:    getenvInt("BOT_WORKERS", 2),
		BotQueue:      getenvInt("BOT_QUEUE", 8),
		BotMaxAge:     getenvDuration("BOT_MAX_AGE", 90*time.Second),
		BotRatePerMin: getenvInt("BOT_RATE_PER_MIN", 3),

		// Google's public STUN. Safe to use free because STUN relays nothing — it
		// only tells a browser what its own address looks like from outside.
		StunURL:    getenv("STUN_URL", "stun:stun.l.google.com:19302"),
		TurnURL:    getenv("TURN_URL", ""), // unset until the relay is running
		TurnSecret: getenv("TURN_SECRET", ""),
		TurnTTL:    getenvDuration("TURN_TTL", 12*time.Hour),
	}
}

// getenvDuration parses values like "90s", "500ms" or "2m". Falls back on
// anything unparseable rather than refusing to boot over a typo in a knob.
func getenvDuration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d > 0 {
		return d
	}
	return fallback
}

// getenvInt is getenv for integers, falling back on anything unparseable rather
// than failing to boot over a typo in a tuning knob.
func getenvInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return fallback
}

// RequireJWTSecret refuses to accept a missing or placeholder signing key.
//
// This is the one setting the process will not guess at, because the old default
// failed OPEN and did so invisibly. With JWT_SECRET unset, every instance fell
// back to the SAME placeholder — so logins succeeded, tokens minted on one
// gateway validated on another, and nothing anywhere errored, while a string
// published in a public repository signed every token in the system. A
// misconfiguration that behaves perfectly is more dangerous than one that
// crashes, because nothing ever prompts you to go and look.
//
// Only the gateway signs and verifies tokens. The persister, indexer and bot
// never touch a JWT, which is why this is a separate call rather than part of
// Load() — requiring it everywhere would be security theatre with a real cost.
func (c Config) RequireJWTSecret() error {
	switch c.JWTSecret {
	case "", devJWTSecret:
		return fmt.Errorf("JWT_SECRET is unset or still the placeholder %q: set it to a "+
			"random value, and to the SAME value on every gateway — tokens signed by one "+
			"instance are verified by recomputing the signature on whichever instance the "+
			"load balancer picks next", devJWTSecret)
	}
	return nil
}

// getenv returns the environment variable if it's set and non-empty,
// otherwise the fallback default.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
