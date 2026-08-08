-- chat-go schema.
--
-- This single file is the source of truth sqlc reads to generate Go types,
-- and the same file we apply to the database (idempotent: IF NOT EXISTS).

-- pgvector — enables the `vector` column type used by semantic search (Phase 3).
-- Must come before any table that declares a vector column.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS users (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username      text        NOT NULL UNIQUE,
    password_hash text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rooms (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        NOT NULL,
    created_by bigint      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Many-to-many: which users are in which rooms. Composite PK = a user
-- appears in a given room at most once.
CREATE TABLE IF NOT EXISTS room_members (
    room_id   bigint      NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id   bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    joined_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    room_id    bigint      NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id    bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Hot query: "latest N messages in room X, newest first" -> this index serves it directly.
CREATE INDEX IF NOT EXISTS idx_messages_room_id_id ON messages (room_id, id DESC);

-- Serves the unread query: "messages in room X sent after time T" (Phase 5.4).
-- The id index above can't do this — it's ordered by id, not by time.
CREATE INDEX IF NOT EXISTS idx_messages_room_id_created_at ON messages (room_id, created_at);

-- --- Read state (Phase 5.4) --------------------------------------------------
-- How far each user has read in each room, so "catch me up" can summarise the
-- gap instead of the last 50 messages.
--
-- The watermark is a TIMESTAMP, not a message id, for a concrete reason: ids are
-- assigned by the persister when it writes to Postgres, so a message delivered
-- live over Redis has no id yet. sent_at is stamped by the gateway and is already
-- carried on the wire. Within a room the two orders agree anyway, because Kafka
-- partitions by room_id and the persister never skips ahead.
CREATE TABLE IF NOT EXISTS room_reads (
    room_id      bigint      NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id      bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_read_at timestamptz NOT NULL,
    -- One row per user per room, same shape as room_members.
    PRIMARY KEY (room_id, user_id)
);

-- --- Semantic search (Phase 3) ----------------------------------------------
-- This is a SEARCH PROJECTION, not a copy of `messages`. It is owned entirely by
-- the `indexer` consumer group, which builds it by replaying the Kafka log. The
-- persister neither reads nor writes it — the two consumers are independent, and
-- either one can be wiped and rebuilt from the log without touching the other.
-- It carries its own copy of `body` for exactly that reason: no join to
-- `messages`, so no dependency on the persister having caught up first.
CREATE TABLE IF NOT EXISTS message_embeddings (
    id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    room_id   bigint      NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id   bigint      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body      text        NOT NULL,
    sent_at   timestamptz NOT NULL,
    embedding vector(384) NOT NULL,   -- all-minilm produces 384 numbers
    -- Kafka gives at-least-once delivery, so the indexer CAN see the same message
    -- twice. This natural key turns a re-delivery into a no-op (ON CONFLICT DO
    -- NOTHING) instead of a duplicate search result. Microsecond-precision
    -- sent_at makes (room, user, time) effectively unique per message.
    UNIQUE (room_id, user_id, sent_at)
);

-- HNSW = the graph index pgvector uses for approximate nearest-neighbour search.
-- vector_cosine_ops matches the `<=>` (cosine distance) operator we query with;
-- the index is only used if the operator and the ops class agree.
CREATE INDEX IF NOT EXISTS idx_message_embeddings_hnsw
    ON message_embeddings USING hnsw (embedding vector_cosine_ops);
