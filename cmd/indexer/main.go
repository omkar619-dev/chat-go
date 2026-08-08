package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgvector/pgvector-go"

	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/config"
	"github.com/omkar619-dev/chat-go/internal/db"
	"github.com/omkar619-dev/chat-go/internal/embed"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// The indexer is the SECOND independent consumer of chat.messages. It reads the
// same log the persister reads — from its own group, at its own pace — turns each
// message into an embedding via Ollama, and stores it for semantic search.
//
// This is the property that made Kafka the right choice over a queue: with a
// queue, a message consumed by the persister would be GONE. With a log, every
// consumer group has its own independent cursor, so adding this whole feature
// required zero changes to the gateway or the persister. Because it starts as a
// brand-new group, it also replays the entire history and back-fills the index.
func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	queries := sqlc.New(pool)
	log.Println("connected to postgres")

	embedder := embed.New(cfg.OllamaURL, cfg.EmbedModel)
	log.Printf("embedding via %s (model %q)", cfg.OllamaURL, cfg.EmbedModel)

	// A DIFFERENT group id from the persister — that's what makes the two cursors
	// independent. Same group id would mean they SPLIT the messages between them.
	consumer := broker.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, "indexer")
	defer consumer.Close()
	log.Printf("indexer reading topic %q from the beginning", cfg.KafkaTopic)

	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Printf("fetch: %v", err)
			continue
		}

		var m broker.ChatMessage
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			log.Printf("skipping unreadable message at offset %d: %v", msg.Offset, err)
			_ = consumer.Commit(ctx, msg)
			continue
		}

		// Nothing worth embedding: legacy messages with no ids, or empty text.
		if m.RoomID == 0 || m.UserID == 0 || strings.TrimSpace(m.Body) == "" {
			log.Printf("skipping unindexable message at offset %d", msg.Offset)
			_ = consumer.Commit(ctx, msg)
			continue
		}

		sentAt := m.SentAt
		if sentAt.IsZero() {
			sentAt = time.Now().UTC()
		}

		// Embed + store, retrying until it works. Ollama being down or slow must
		// not cause us to skip ahead — see broker.Backoff for why skipping loses data.
		if !indexMessage(ctx, embedder, queries, m, sentAt) {
			log.Println("indexer shutting down mid-retry (message stays uncommitted)")
			return
		}

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit at offset %d: %v", msg.Offset, err)
		}
	}

	log.Println("indexer shutting down")
}

// indexMessage embeds one message and upserts it, retrying with backoff until it
// succeeds. Returns false only if the process is shutting down.
func indexMessage(
	ctx context.Context,
	embedder *embed.Client,
	queries *sqlc.Queries,
	m broker.ChatMessage,
	sentAt time.Time,
) bool {
	for attempt := 1; ; attempt++ {
		err := func() error {
			// Its own timeout: a hung Ollama shouldn't block shutdown forever.
			embedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			vec, err := embedder.Embed(embedCtx, m.Body)
			if err != nil {
				return err
			}
			return queries.UpsertMessageEmbedding(ctx, sqlc.UpsertMessageEmbeddingParams{
				RoomID:    m.RoomID,
				UserID:    m.UserID,
				Body:      m.Body,
				SentAt:    pgtype.Timestamptz{Time: sentAt, Valid: true},
				Embedding: pgvector.NewVector(vec),
			})
		}()
		if err == nil {
			log.Printf("indexed a message from user %d in room %d", m.UserID, m.RoomID)
			return true
		}

		wait := broker.Backoff(attempt)
		log.Printf("index attempt %d failed: %v — retrying in %s", attempt, err, wait)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
}
