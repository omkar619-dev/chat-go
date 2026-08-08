-- name: CreateRoom :one
INSERT INTO rooms (name, created_by)
VALUES ($1, $2)
RETURNING *;

-- name: ListRooms :many
SELECT * FROM rooms
ORDER BY id;

-- name: AddRoomMember :exec
INSERT INTO room_members (room_id, user_id)
VALUES ($1, $2)
ON CONFLICT (room_id, user_id) DO NOTHING;

-- name: IsRoomMember :one
SELECT EXISTS (
    SELECT 1 FROM room_members
    WHERE room_id = $1 AND user_id = $2
) AS is_member;
