-- name: CreateRoom :one
INSERT INTO rooms (name, created_by)
VALUES ($1, $2)
RETURNING *;

-- name: ListRooms :many
SELECT * FROM rooms
ORDER BY id;

-- Every room, plus whether THIS user is already a member of it.
--
-- One query rather than "list the rooms" followed by a membership check per
-- room: that version costs one database round trip per room, so the list gets
-- slower the more rooms exist.
--
-- LEFT JOIN, not JOIN. A LEFT JOIN keeps rooms that have no matching membership
-- row, filling the joined columns with NULL — which is exactly how a room you
-- are NOT in still appears in the list as something you could join. A plain JOIN
-- would silently drop every room you haven't joined, which is most of them.
-- name: ListRoomsForUser :many
-- The ::boolean cast is not decoration. sqlc infers Go types from what Postgres
-- reports, and it cannot work out the type of a bare IS NOT NULL expression, so
-- it falls back to interface{} and the Go code then won't compile. The cast
-- tells it. Same trick as ::vector in search.sql.
SELECT r.id, r.name, (rm.user_id IS NOT NULL)::boolean AS is_member
FROM rooms r
LEFT JOIN room_members rm ON rm.room_id = r.id AND rm.user_id = $1
ORDER BY r.name;

-- name: AddRoomMember :exec
INSERT INTO room_members (room_id, user_id)
VALUES ($1, $2)
ON CONFLICT (room_id, user_id) DO NOTHING;

-- name: IsRoomMember :one
SELECT EXISTS (
    SELECT 1 FROM room_members
    WHERE room_id = $1 AND user_id = $2
) AS is_member;
