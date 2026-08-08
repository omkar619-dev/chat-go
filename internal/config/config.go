package config

import "os"

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
		HTTPAddr:    getenv("HTTP_ADDR", ":8090"), // :8080 is taken by the local Atlassian app
		DatabaseURL: getenv("DATABASE_URL", "postgres://chat:chat_dev@localhost:5433/chat?sslmode=disable"),
		RedisAddr:   getenv("REDIS_ADDR", "localhost:6381"),
		JWTSecret:   getenv("JWT_SECRET", "dev-change-me"),
		KafkaBroker: getenv("KAFKA_BROKER", "localhost:9094"),
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

// getenv returns the environment variable if it's set and non-empty,
// otherwise the fallback default.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
