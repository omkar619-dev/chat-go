package redisclient

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Connect creates a Redis client and pings it to verify connectivity at startup.
func Connect(ctx context.Context, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}
