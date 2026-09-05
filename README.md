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

0. ✅ **Scaffold** — repo, schema, docker-compose (Postgres + Redis).
1. ✅ **Text chat MVP** — WS gateway + auth + send/receive via Redis pub/sub + HTMX UI (no Kafka yet).
2. ✅ **Durability + Kafka** — produce to Kafka, `persister` -> Postgres, load history.
3. ✅ **Semantic search** — `indexer` (embed -> pgvector) + search.
4. ✅ **RAG assistant** — `@bot` retrieve + LLM + stream.
5. ✅ **"Catch me up"** summarization.
6. ✅ **Presence + multi-gateway scaling** — Redis heartbeats, two gateways behind nginx, load shedding.
7. ✅ **WebRTC** voice/video — signalling over the existing socket, STUN + coturn TURN.
8. **Deploy** on k3s (Helm) + tests + CI.  <- *you are here*

Known gaps are tracked in [docs/HARDENING.md](docs/HARDENING.md) rather than left
implied — four blockers remain open, and they gate public exposure, not the build.

## Measured, not assumed

**Redis fan-out.** The gateway originally opened one Redis subscription per
WebSocket connection, so a room with 500 members meant 500 subscriptions all
receiving identical bytes. It is now one subscription per room per gateway
process, fanned out in memory. Measured with `PUBSUB NUMSUB` and
`total_net_output_bytes` across a 100/500/1000/2000-connection ladder
(`cmd/loadtest`), before and after.

**Slow readers.** A socket that stops reading gets a bounded queue, then a
disconnect — not an unbounded buffer. Verified by stalling one client
deliberately and watching it get evicted while the others kept up.

**WebRTC paths**, from `RTCPeerConnection.getStats()`:

| Path | Result |
|---|---|
| Direct, two real networks (laptop on WiFi + phone on mobile data) | `srflx <-> srflx`, RTT 63ms (min 35 / max 841, n=25) |
| Relayed via coturn in `ap-south-1`, forced with `iceTransportPolicy: 'relay'` | `relay <-> relay`, RTT 92ms (min 20, n=2) |

These two numbers are **not yet a fair comparison** — the direct figure is two
devices on two networks, the relayed one is two browsers on a single host. The
honest version needs the same pair measured twice with only the relay toggled,
and is still to do.

## Local dev

```bash
cp .env.example .env
docker compose up -d                       # Postgres :5433, Redis :6381

# apply the schema:
docker compose exec -T postgres psql -U chat -d chat < internal/repository/postgres/schema.sql

# generate type-safe Go from SQL:
sqlc generate                              # -> internal/repository/postgres/sqlc
```
