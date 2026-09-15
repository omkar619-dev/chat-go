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
8. ✅ **Deploy** on k3s — Helm chart, sealed secrets, CI to ghcr, TLS, consumer-lag alerting.

Known gaps are tracked in [docs/HARDENING.md](docs/HARDENING.md) rather than left
implied. **All four pre-exposure blockers are now closed** — TLS, secrets out of
git, WebSocket origin verification, and no credential in a URL. What remains
there is graded Important and Hygiene, and is written down rather than quietly
carried.

## Deployment

It runs on a single-node **k3s** cluster on a machine in my flat, reachable over
Tailscale at a stable HTTPS address. One command brings the whole thing up on an
empty cluster — seven components, its own schema, and credentials it reads but
this repository does not contain.

```bash
helm upgrade --install chat-go deploy/helm/chat-go -f deploy/helm/chat-go/homelab.values.yaml
```

**Images come from CI.** `.github/workflows/ci.yml` builds and pushes to
`ghcr.io/omkar619-dev/chat-go` on every push to `main`, tagged `sha-<commit>`.
The deployed tag is pinned in `homelab.values.yaml`, so what is running is
recorded in git and a rollback is a revert. The mutable `main` tag is published
too, and deliberately not used for deployment — two nodes pulling it an hour
apart can run different code while reporting the same version.

**Credentials never enter the repository.** The chart is installed with
`secrets.create=false`, so it consumes a Secret it does not create. That Secret
is produced by a **SealedSecret** (`deploy/sealed/homelab.yaml`) — encrypted with
the target cluster's public key, which makes the ciphertext safe to commit and
readable by exactly one cluster. Postgres takes its password from the same
Secret, so there is one copy rather than two that can disagree.

**TLS is a real certificate, without owning a domain.** Traefik terminates it at
the Ingress; the certificate is issued by Tailscale for the node's `*.ts.net`
name, which works because Tailscale controls that DNS zone and can answer the
challenge itself. `curl` validates it against the system trust store with no
`-k`. That also removes a recurring nuisance: browsers refuse camera access on a
non-HTTPS origin, so WebRTC testing previously needed a throwaway tunnel with a
new URL every session.

**What deploying found.** The chart was correct on a local k3d cluster and still
shipped a bug that made a fresh install unusable: the front end hardcoded a join
to room 1, and a new database has no rooms at all. It had worked on every
database it had ever run against, because room 1 had existed since phase 1 — so
that path had never executed. Running it somewhere genuinely new is the only
thing that finds those.

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

**Consumer lag**, which caught a real failure on the day it shipped. A separate
process (`cmd/lagexporter`) publishes how far each consumer group is behind, and
Prometheus alerts when a group stays behind. Two things about it are less obvious
than they look:

*It cannot live inside the consumers.* The first attempt had each one report its
own lag, and `kafka-go` refuses — `ReadLag` is "unavailable when GroupID is set".
That is the right answer to the wrong question: **a consumer that has died cannot
report its own lag.** Anything measured in-process freezes at a healthy-looking
value at exactly the moment the failure happens. So the exporter reads Kafka's own
bookkeeping instead — newest offset per partition, committed offset per group —
neither of which needs the consumer alive.

*The obvious alert would have missed the real bug.* "Lag is growing" is what you
reach for first. The failure it found was the indexer stuck at exactly 10, because
Ollama was unreachable and the room was quiet — so the lag was perfectly flat and
a growth-based rule would have said nothing. What separates *stuck* from *busy* is
**time spent behind**: a working consumer returns to zero in seconds, a stopped one
never does. The pod was `1/1 Running` with zero restarts the whole time, which is
precisely why a liveness probe was never going to catch it.

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
