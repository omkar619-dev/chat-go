package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/config"
	"github.com/omkar619-dev/chat-go/internal/db"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// The persister is a SEPARATE process from the gateway. It does exactly one job:
// read the durable Kafka log and copy every chat message into Postgres. Because
// it's decoupled, the gateway keeps serving even when the persister is down —
// Kafka holds the messages until the persister comes back and catches up.
func main() {
	cfg := config.Load()

	// Cancel cleanly on Ctrl+C / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Postgres — the permanent home for messages.
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	queries := sqlc.New(pool)
	log.Println("connected to postgres")

	// Kafka consumer in the "persister" group, so our progress is tracked.
	consumer := broker.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, "persister")
	defer consumer.Close()
	log.Printf("persister reading topic %q", cfg.KafkaTopic)

	for {
		// 1. Block for the next message (returns an error when ctx cancels on shutdown).
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break // clean shutdown
			}
			log.Printf("fetch: %v", err)
			continue
		}

		// 2. Decode the wire format.
		var m broker.ChatMessage
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			log.Printf("skipping unreadable message at offset %d: %v", msg.Offset, err)
			_ = consumer.Commit(ctx, msg) // poison message: skip so it can't block the pipeline
			continue
		}

		// 3. Guard: messages produced before we added room_id/user_id (or any
		//    malformed one) have zero IDs, which would violate the foreign keys.
		//    IDENTITY columns start at 1, so 0 reliably means "missing".
		if m.RoomID == 0 || m.UserID == 0 {
			log.Printf("skipping legacy/invalid message at offset %d", msg.Offset)
			_ = consumer.Commit(ctx, msg)
			continue
		}

		// 4. Durably store it, preserving the gateway's send-time. If we fell back
		//    to the DB's now(), a backlog drained after an outage would stamp every
		//    message with the restart instant instead of when it was really sent.
		sentAt := m.SentAt
		if sentAt.IsZero() {
			sentAt = time.Now().UTC() // defensive: message from before SentAt existed
		}

		// Retry until the insert succeeds. We must NOT move on to the next message
		// after a failure: offsets record how FAR we got, so committing a later
		// message would permanently step over this one (see broker.Backoff).
		stored := false
		for attempt := 1; !stored; attempt++ {
			_, err := queries.CreateMessage(ctx, sqlc.CreateMessageParams{
				RoomID:    m.RoomID,
				UserID:    m.UserID,
				Body:      m.Body,
				CreatedAt: pgtype.Timestamptz{Time: sentAt, Valid: true},
			})
			if err == nil {
				stored = true
				break
			}
			wait := broker.Backoff(attempt)
			log.Printf("insert (room %d) attempt %d: %v — retrying in %s", m.RoomID, attempt, err, wait)
			select {
			case <-ctx.Done():
				log.Println("persister shutting down mid-retry (message stays uncommitted)")
				return
			case <-time.After(wait):
			}
		}

		// 5. Only NOW mark it done. Committing AFTER the successful insert is what
		//    makes this at-least-once: a crash between fetch and commit means we
		//    re-process (a possible duplicate), never lose a message.
		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit at offset %d: %v", msg.Offset, err)
		}
		log.Printf("persisted a message from user %d in room %d", m.UserID, m.RoomID)
	}

	log.Println("persister shutting down")
}
