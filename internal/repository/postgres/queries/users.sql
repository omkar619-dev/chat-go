-- name: CreateUser :one
INSERT INTO users (username, password_hash)
VALUES ($1, $2)
RETURNING *;

-- name: GetUserByUsername :one
SELECT * FROM users
WHERE username = $1;

-- Names for a set of user ids. Presence stores ids in Redis; the UI shows names.
--
-- ANY() takes the whole set in ONE query. The obvious alternative — a lookup per
-- user — would mean one database round trip per person in the room, so a busy
-- room would cost more trips the more popular it got.
--
-- ORDER BY username so the online list doesn't shuffle between refreshes, which
-- would look like people joining and leaving when nothing changed.
-- Returns the id as well as the name, because the online list is now clickable:
-- calling somebody needs their user id, and names are not identifiers.
-- name: ListUsersByIDs :many
SELECT id, username FROM users
WHERE id = ANY($1::bigint[])
ORDER BY username;
