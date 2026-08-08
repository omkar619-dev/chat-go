-- The `::vector` cast is how sqlc learns the parameter's type: without it sqlc
-- has no idea what a vector is, with it the generated Go param is a
-- pgvector.Vector. Same trick news-feed uses.
--
-- ON CONFLICT DO NOTHING makes the indexer IDEMPOTENT: Kafka can redeliver a
-- message, and re-indexing it must not create a second search hit.
-- name: UpsertMessageEmbedding :exec
INSERT INTO message_embeddings (room_id, user_id, body, sent_at, embedding)
VALUES (
    sqlc.arg(room_id),
    sqlc.arg(user_id),
    sqlc.arg(body),
    sqlc.arg(sent_at),
    sqlc.arg(embedding)::vector
)
ON CONFLICT (room_id, user_id, sent_at) DO NOTHING;

-- Semantic search within one room, nearest-meaning first.
--   <=>  is pgvector's cosine-DISTANCE operator: 0 = identical meaning, 2 = opposite.
--   Ordering ASC by distance therefore puts the best matches first, and this is
--   the operator the HNSW index was built for, so it doesn't scan the table.
--   We return (1 - distance) as `similarity` because a number where HIGHER means
--   BETTER is far easier to reason about in the UI.
-- name: SearchMessages :many
SELECT
    me.room_id,
    me.user_id,
    u.username,
    me.body,
    me.sent_at,
    (1 - (me.embedding <=> sqlc.arg(query_embedding)::vector))::float8 AS similarity
FROM message_embeddings me
JOIN users u ON u.id = me.user_id
WHERE me.room_id = sqlc.arg(room_id)
ORDER BY me.embedding <=> sqlc.arg(query_embedding)::vector
LIMIT 10;

-- How much of the log the indexer has covered — used by the /search handler to
-- give an honest "index is still warming up" answer instead of empty results.
-- name: CountMessageEmbeddings :one
SELECT count(*) FROM message_embeddings WHERE room_id = $1;
