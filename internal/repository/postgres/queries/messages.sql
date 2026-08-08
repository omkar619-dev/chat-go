-- created_at is passed in (not left to DEFAULT now()) so the stored time is when
-- the user actually sent the message, not when the persister happened to insert it.
-- name: CreateMessage :one
INSERT INTO messages (room_id, user_id, body, created_at)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- The last 50 messages of a room, oldest-first (chat reading order).
-- The inner query walks idx_messages_room_id_id backwards to grab the NEWEST 50
-- without scanning the room's whole history; the outer query then flips those
-- 50 rows back into chronological order.
-- name: ListRecentMessages :many
SELECT recent.room_id, recent.user_id, recent.username, recent.body, recent.created_at
FROM (
    SELECT m.id, m.room_id, m.user_id, u.username, m.body, m.created_at
    FROM messages m
    JOIN users u ON u.id = m.user_id
    WHERE m.room_id = $1
    ORDER BY m.id DESC
    LIMIT 50
) AS recent
ORDER BY recent.id;
