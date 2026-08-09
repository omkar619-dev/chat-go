package config

import (
	"fmt"
	"os"
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
	}
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
