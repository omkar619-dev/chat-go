-- Read-state queries (Phase 5.4) — how far a user has read in a room, and what
-- they missed. Used only by "/catchup".

-- name: GetRoomRead :one
SELECT last_read_at FROM room_reads
WHERE room_id = $1 AND user_id = $2;

-- Move a user's read watermark forward.
--
-- GREATEST() is the important part: a watermark must only ever move FORWARD.
-- Without it, two tabs open on the same room would fight — the one that
-- disconnects second could be *behind*, and would drag the watermark backwards,
-- silently resurrecting messages the user already saw. Same lesson as the Kafka
-- offset: a watermark records how far you got, so it can only go up.
-- name: UpsertRoomRead :exec
INSERT INTO room_reads (room_id, user_id, last_read_at)
VALUES ($1, $2, $3)
ON CONFLICT (room_id, user_id)
DO UPDATE SET last_read_at = GREATEST(room_reads.last_read_at, EXCLUDED.last_read_at);

-- The messages a user missed, oldest-first, capped.
--
-- "Missed" is three conditions, not two. The timestamp alone says "arrived after
-- you left", which is also true of messages YOU sent on returning — they land on
-- the far side of your own watermark. You have read what you wrote, so $3
-- excludes the asker. Without it /catchup answers "what happened here?" when the
-- question is "what did I miss?".
--
-- Note we do NOT advance the watermark when a user sends. Talking is not reading:
-- you can reconnect, fire off a message without scrolling up, and still be owed a
-- summary of everything above it.
--
-- The cap is not cosmetic. Prompt tokens are the expensive half of generation on
-- the homelab box (~71 tok/s prefill vs ~4.7 tok/s decode), and the model's
-- context is 2048 tokens. Someone away for a month must not push 5,000 messages
-- into it. We take the NEWEST 200 unread and flip them back to chronological —
-- the same inner/outer trick ListRecentMessages uses.
-- name: ListUnreadMessages :many
SELECT unread.room_id, unread.user_id, unread.username, unread.body, unread.created_at
FROM (
    SELECT m.id, m.room_id, m.user_id, u.username, m.body, m.created_at
    FROM messages m
    JOIN users u ON u.id = m.user_id
    WHERE m.room_id = $1 AND m.created_at > $2 AND m.user_id <> $3
    ORDER BY m.id DESC
    LIMIT 200
) AS unread
ORDER BY unread.id;

-- Total unread, so the UI can say "summarising the latest 200 of 4,312" honestly
-- rather than silently truncating. Must apply the SAME three conditions as
-- ListUnreadMessages — if the count and the list disagree about what "unread"
-- means, the "(N unread — summarising the most recent M)" notice lies.
-- name: CountUnreadMessages :one
SELECT count(*) FROM messages
WHERE room_id = $1 AND created_at > $2 AND user_id <> $3;
