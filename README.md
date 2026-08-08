# chat-go

Real-time chat in Go — WebSockets for live text, Kafka as the durable event log,
Redis for fan-out and presence, Postgres/pgvector for history and semantic search,
and local-LLM (Ollama) RAG features. Web UI via HTMX (no SPA). Sibling project to
[news-feed-go](../news-feed-go): "one repo, many roles."

## Architecture — two planes

- **Hot path (real-time, <100ms):** client `-> WebSocket -> gateway -> Redis pub/sub -> gateway -> WebSocket ->` other clients. No broker on this path.
- **Durable path:** every message is also an event -> **Kafka log** (`chat.messages`, keyed by `room_id` for per-room ordering), consumed independently by persistence, search-indexing, and AI.

Delivery avoids the dual-write problem: the gateway produces to Kafka (source of
truth), a fan-out consumer republishes to Redis, and every gateway with a
subscriber in that room pushes down the socket.

## Components (processes — same shape as news-feed-go)

| Process     | Role                                                                        |
|-------------|-----------------------------------------------------------------------------|
| `gateway`   | HTTP + WebSocket server: JWT auth, HTMX UI, WS connections, WebRTC signaling |
| `persister` | Kafka -> Postgres message history                                            |
| `indexer`   | Kafka -> Ollama embed -> pgvector                                            |
| `ai-worker` | RAG assistant + "catch me up" summaries                                      |
| `notifier`  | offline push                                                                 |

Backing services: **Postgres + pgvector**, **Redis** (pub/sub + presence), **Kafka** (durable log).

## Build phases

0. **Scaffold** — repo, schema, docker-compose (Postgres + Redis).  <- *you are here*
1. **Text chat MVP** — WS gateway + auth + send/receive via Redis pub/sub + HTMX UI (no Kafka yet).
2. **Durability + Kafka** — produce to Kafka, `persister` -> Postgres, load history.
3. **Semantic search** — `indexer` (embed -> pgvector) + search.
4. **RAG assistant** — `@bot` retrieve + LLM + stream.
5. **"Catch me up"** summarization.
6. **Presence + multi-gateway scaling** (Redis TTL heartbeats).
7. **WebRTC** voice/video (signaling + STUN/TURN).
8. **Deploy** on k3s (Helm) + tests + CI.

## Local dev

```bash
cp .env.example .env
docker compose up -d                       # Postgres :5433, Redis :6381

# apply the schema:
docker compose exec -T postgres psql -U chat -d chat < internal/repository/postgres/schema.sql

# generate type-safe Go from SQL:
sqlc generate                              # -> internal/repository/postgres/sqlc
```
