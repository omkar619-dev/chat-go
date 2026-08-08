-- name: CreateUser :one
INSERT INTO users (username, password_hash)
VALUES ($1, $2)
RETURNING *;

-- name: GetUserByUsername :one
SELECT * FROM users
WHERE username = $1;
