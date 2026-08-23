# Pre-public hardening checklist

chat-go currently runs **local-only, on purpose**. Several deliberate shortcuts make
local development fast and are unacceptable on a public network. This document is the
gate: **nothing here is exposed to the internet until every "Blocker" is closed.**

Each item states the shortcut, why it is dangerous, and the intended fix. Items were
found while building, not bolted on afterwards — the decision each time was to note it
and keep feature work moving, rather than half-fix it under time pressure.

Status: **1 of 5 blockers closed** (B3, 2026-08-09). The rest run before Phase 8 (deploy).

---

## Blockers — must be fixed before any public exposure

### B1. The JWT travels in the WebSocket URL query string
`cmd/gateway/index.html` connects with `ws://host/ws?token=<JWT>&room=1`, and
`internal/httpapi/ws.go` reads it from `r.URL.Query()`.

**Why it's dangerous:** query strings are logged. The gateway's own access log already
contains complete, valid, 24-hour tokens in plaintext:

```
GET /ws?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...&room=1
```

Anyone with log access — or a log shipper, an APM vendor, a `Referer` header, or browser
history — holds a working credential. This exists only because browsers cannot set custom
headers on a WebSocket handshake, so the token was smuggled into the URL.

**Fix:** short-lived single-use ticket.
1. `POST /ws-ticket` authenticated normally (JWT in the `Authorization` header).
2. Server generates a random opaque string, stores it in Redis with a ~30s TTL bound to the user id.
3. Client connects with `?ticket=<random>`.
4. Server redeems it: look up, **delete immediately** (single use), then authorize.

A leaked 30-second one-time ticket is worthless; a leaked 24-hour JWT is a skeleton key.

### B2. WebSocket origin verification is disabled
`internal/httpapi/ws.go` calls `websocket.Accept` with `InsecureSkipVerify: true`.

**Why it's dangerous:** this permits **Cross-Site WebSocket Hijacking**. Any website the
user visits can open a socket to our gateway. Today the blast radius is limited because the
attacker also needs the token, but the moment auth moves to anything the browser attaches
automatically (a cookie), this becomes a full account takeover.

**Fix:** delete the option. `coder/websocket` defaults to requiring the `Origin` host to
match the `Host` header, which is exactly right for a same-origin deployment. If the front
end is ever served from a different host, use `OriginPatterns` with an explicit allowlist —
never a wildcard.

### ~~B3. The JWT signing secret has a working default~~ — ✅ FIXED 2026-08-09
`internal/config/config.go` fell back to `JWTSecret: "dev-change-me"`.

**Why it was dangerous:** it failed *open*, and did so **invisibly**. With `JWT_SECRET`
unset, every instance fell back to the *same* placeholder — so logins succeeded, tokens
minted on one gateway validated on another, and nothing anywhere errored, while a string
published in this repo signed every token in the system. That is worse than a mismatch: a
mismatch produces random 401s and gets investigated in minutes, whereas a consistent wrong
default produces a service that behaves perfectly and is wide open.

**Fixed by** `config.RequireJWTSecret()`, called from `cmd/gateway/main.go` before any
connection is dialled. The placeholder is a named constant so it can be refused *by name*:
unset and explicitly-set-to-the-placeholder are the same mistake and fail the same way.

Deliberately stricter than the fix originally proposed here. The plan was to gate on an
`APP_ENV=production` marker and keep the default for local dev; the implementation has **no
escape hatch at all**, because an escape hatch is exactly the thing that ends up enabled in
production. Local dev sets a real random secret once, in the user environment.

Only the gateway signs or verifies tokens, so the check lives there rather than in `Load()`.
The persister, indexer and bot need no secret and are unaffected — requiring it everywhere
would be theatre with a real cost.

Surfaced by Phase 6.1: with two gateways behind nginx, a token is minted on whichever
instance served `POST /login` and verified on whichever served `GET /ws`. That works only
while every instance holds the same key — the property a Kubernetes Secret must guarantee
across replicas (see B5), and precisely the one the old default hid.

### B4. No TLS
Everything is `http://` and `ws://`.

**Why it's dangerous:** credentials, tokens and message bodies cross the network in
plaintext. On any shared network this is a trivial capture.

**Fix:** terminate TLS at the ingress (k3s + cert-manager, matching the existing homelab
setup), serve `https://`, and have the client derive the socket scheme rather than
hardcoding it: `location.protocol === 'https:' ? 'wss:' : 'ws:'`.

### B5. Infrastructure credentials are hardcoded
`docker-compose.yml` contains `chat:chat_dev` for Postgres; the connection string with
those credentials is a default in `config.go`.

**Why it's dangerous:** dev credentials tend to survive into the first deploy.

**Fix:** real generated secrets, delivered via **Sealed Secrets** (already in use for
newsfeed on the k3s cluster). No credential in the repo, no credential in a Helm values file.

---

## Important — fix before inviting anyone else in

### I1. The token is stored in `localStorage`
`cmd/gateway/index.html` persists the JWT to `localStorage` so a refresh doesn't log the
user out.

**Why it's a risk:** `localStorage` is readable by *any* JavaScript running on the page. A
single XSS hole yields a valid 24-hour token. (The DOM code deliberately uses `textContent`
rather than `innerHTML`, so no injection path is known today — but this makes any future
XSS far more costly.)

**Fix:** the standard split — a short-lived access token held in memory, plus a refresh
token in an **`httpOnly; Secure; SameSite=Lax`** cookie that JavaScript cannot read. Pairs
naturally with B1, since a cookie is sent automatically on a same-origin WS handshake.
Requires CSRF consideration once cookies carry auth.

### I2. No logout, and no way to revoke a token
JWTs are stateless: once signed, a token is valid until it expires. There is no logout
endpoint, and clearing `localStorage` only forgets the token locally — a copied token keeps
working for the rest of its 24 hours.

**Fix:** shorten the access-token lifetime to minutes, add a refresh endpoint, and keep a
Redis denylist of revoked token ids (`jti`) for the remainder of their validity.

**Chosen as the priority for auth work (2026-08-23), in preference to adding social login.**
"Sign in with Google" was considered and deliberately skipped: it is a well-documented
integration that would add an external dependency to a project whose value is elsewhere,
and it demonstrates nothing about design. This does. Revoking a *stateless* token across
several gateways without a database read on every request is a real problem with real
trade-offs — how short is short enough, where the denylist lives, what happens when that
store is unreachable, and whether you fail open or closed. The sign-out button shipped
before this is honest about being a convenience only: it forgets the token locally and the
token itself stays valid until it expires.

### I3. No rate limiting
`/login` accepts unlimited attempts (brute force), and an open socket can publish messages
as fast as it can write (spam, and unbounded Kafka/Postgres growth).

**Fix:** per-IP limit on `/login` and per-user message rate limiting, both backed by Redis —
the same pattern already used in StudentSystemGo.

### I4. Any authenticated user can join any room
`POST /rooms/{id}/join` performs no invitation or visibility check. Every room is
effectively public to anyone with an account.

**Fix:** a room visibility column (public/private) plus an invitation or membership-approval
path before private rooms are meaningful.

### I5. Input is barely validated
- Message bodies: no length cap, no rejection of empty/whitespace-only messages.
  `coder/websocket` applies a default read limit (~32 KiB) — confirm it and set it
  explicitly with `conn.SetReadLimit()` rather than relying on a library default.
- Passwords: no minimum length or complexity requirement.
- Usernames: no length or character constraints.

### I6. The RAG bot has no spam protection, and blocks head-of-line
`cmd/bot` processes messages **strictly one at a time**, and each `@bot` question
costs an embedding call plus a generation that takes 3-25 seconds on the homelab box.

**Why it's a risk:** ten `@bot` mentions in a row become a serial queue tens of seconds
deep, and **every other room waits behind them** — one user can stall the bot for
everyone. Same head-of-line blocking shape as the persister retry loop, but trivially
triggerable by any authenticated user. Generation is also the most expensive operation
in the system, which makes it the natural target for abuse.

**Fix:** a per-user rate limit on mentions (Redis, same pattern as elsewhere), plus a
bounded worker pool so one slow generation doesn't block unrelated rooms. Consider
dropping rather than queueing when the backlog is deep — a stale answer to a question
asked two minutes ago is worse than no answer.

### I7. Kafka auto-creates topics
`internal/broker/producer.go` sets `AllowAutoTopicCreation: true`, which is convenient
locally but means a typo in a topic name silently creates a new topic in production, with
default partition and replication settings.

**Fix:** create topics explicitly with deliberate partition counts and replication factor;
disable auto-creation.

---

## Hygiene — before calling it finished

### H1. No tests
No unit or integration tests exist. The highest-value targets, in order: the `auth` package
(token generation/verification, including the algorithm-confusion guard), the persister's
at-least-once commit ordering, and an end-to-end WebSocket round trip.

### H2. Access logs record full URLs
`middleware.Logger` logs the complete request URI, which is how B1's tokens ended up in the
log. Even after B1 is fixed, request logging should scrub query strings on auth-bearing routes.

### H3. Bot replies can duplicate, and `/search` is inconsistent with the bot
The bot commits its Kafka offset *after* handling a message, so a crash mid-generation
means the message is re-read and answered twice on restart. At-least-once again —
acceptable, but it should be a stated property rather than a surprise.

Separately, `cmd/bot` excludes its own replies from retrieved context (to avoid
grounding answers in its own output), but the `/search` API does **not** filter them.
That may well be correct — a user might want to find something the bot said — but it
should be a deliberate decision, not an accident of where the filter happens to live.

### H5. Nothing notices when a consumer stops
Found the hard way on 2026-08-10. The persister was not running for several hours. Nothing
reported it — not the gateway, not the browser, no error frame anywhere.

**Why it was invisible:** the gateway's promise is that a message reached **Kafka**, and it
kept that promise perfectly. Whether anything *consumed* that message is a separate
guarantee, and nothing in the system was watching it. "Durably recorded" and "processed into
a queryable table" are two different claims, and a dead consumer breaks the second while the
first stays true.

**How it eventually surfaced:** as an unrelated-looking symptom, hours later — "my messages
disappear when I switch rooms". History replay reads Postgres, Postgres is filled by the
persister, so an idle persister looks exactly like lost messages from the UI. The data was
never lost; it was still sitting in Kafka and drained the moment the persister restarted,
which is the at-least-once design working as intended.

**Fix:** export **consumer lag** — the gap between the newest offset in a partition and the
last offset a group has committed — for all three groups (`persister`, `indexer`, `bot`), and
alert on it growing. Lag is the right signal precisely because it catches *both* failure
modes with one number: a consumer that has died, and one that is merely too slow. Natural
fit alongside Phase 8, since the homelab already runs Prometheus.

Related: the persister deliberately retries in place and never advances past a failed message
(see the offset-commit note in the Phase 2 history), which means a poison message stops that
partition rather than skipping it. That is the correct trade for not losing data, but it makes
lag monitoring more important, not less — a stuck consumer and a dead one look identical from
the outside.

### H4. Single points of failure
Single-node Kafka (`replication factor 1`), single Postgres, single Redis. Acceptable for a
homelab portfolio deployment, but should be stated explicitly rather than implied — the
persister's at-least-once guarantee protects against *consumer* failure, not against loss of
the broker's only copy.
