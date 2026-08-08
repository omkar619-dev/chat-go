package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pgvector/pgvector-go"
	"github.com/redis/go-redis/v9"

	"github.com/omkar619-dev/chat-go/internal/auth"
	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/config"
	"github.com/omkar619-dev/chat-go/internal/db"
	"github.com/omkar619-dev/chat-go/internal/embed"
	"github.com/omkar619-dev/chat-go/internal/llm"
	"github.com/omkar619-dev/chat-go/internal/redisclient"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

// botUsername is the account the bot posts as. It's a real row in `users` so that
// the foreign keys work and its replies persist and get indexed like anyone else's.
const botUsername = "bot"

// mention is what triggers the bot.
const mention = "@bot"

// mentionPattern is used for BOTH detecting and stripping the mention, so the two
// can never disagree. Previously detection lowercased the body but stripping
// didn't, so "@Bot hello" was recognised yet the mention survived into the
// question sent to the model.
//
//	(?i)          case-insensitive: @bot, @Bot, @BOT all match
//	QuoteMeta     escape the literal, so the constant can change safely
//	\b            word boundary: matches "@bot hello" but NOT "@bothell.com"
var mentionPattern = regexp.MustCompile(`(?i)` + regexp.QuoteMeta(mention) + `\b`)

// How much retrieved context actually reaches the model.
//
// These aren't cosmetic. On slow hardware the PROMPT costs as much as the answer:
// Ollama reports a separate "prompt eval rate", and every context message we send
// is tokens the model must chew through before writing a single word. Sending all
// ten hits when only four are relevant can double the time to first token.
//
// minSimilarity is set from observed data, not guessed: a persister question
// scored 80/79/74/47 for relevant messages and 16/15/11/11/6 for noise — a clear
// cliff between 47 and 16, so 0.30 sits safely inside the gap.
const (
	maxContext    = 5
	minSimilarity = 0.30

	// minTopSimilarity is a CONFIDENCE GATE: unless the single best retrieved
	// message clears this bar, we refuse to answer instead of asking the model.
	//
	// The system prompt already tells the model to say "I don't know" when the
	// excerpts don't cover the question — but a 1.5B model is weak at refusing and
	// will confidently invent something instead. Observed on this corpus:
	//
	//   "is the persister down btw?"      top hit 86%  -> correct answer
	//   "why did the messages survive?"   top hit 38%  -> HALLUCINATED
	//
	// The signal is clean, so we act on it in code rather than hoping the model
	// obeys an instruction. This makes refusal DETERMINISTIC, and it's also free
	// latency: we skip ~10s of generating an answer that was going to be wrong.
	minTopSimilarity = 0.50
)

// systemPrompt is the bot's standing instruction. The two rules that matter are
// "use ONLY the excerpts" and "say you don't know" — together they're what makes
// this RAG rather than just a chatbot. Without the second rule a model under
// pressure will invent something rather than admit the context didn't cover it.
const systemPrompt = `You are a helpful assistant inside a chat room.
Answer the question using ONLY the chat message excerpts provided.
If the excerpts do not contain the answer, say you don't know — do not guess or use outside knowledge.
Keep your answer to two or three sentences.`

// The bot is the THIRD independent consumer group on chat.messages. Like the
// indexer, it needed no changes to the gateway or to any other consumer — it just
// attaches to the log and starts reading.
//
// Its guarantees are deliberately WEAKER than the persister's. The persister must
// never lose a message, so it retries forever. The bot is best-effort: if
// generation fails we log it, commit, and move on. A missed bot reply is a minor
// annoyance; a bot that jams the log retrying forever is an outage.
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

	rdb, err := redisclient.Connect(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	embedder := embed.New(cfg.OllamaURL, cfg.EmbedModel)
	generator := llm.New(cfg.OllamaURL, cfg.ChatModel)
	producer := broker.NewProducer(cfg.KafkaBroker, cfg.KafkaTopic)
	defer producer.Close()

	// The bot needs a user id to post under. Create the account on first run.
	botID, err := ensureBotUser(ctx, queries)
	if err != nil {
		log.Fatalf("bot user: %v", err)
	}
	log.Printf("bot ready as user %d, model %q, watching for %q", botID, cfg.ChatModel, mention)

	consumer := broker.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, "bot")
	defer consumer.Close()

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
		if err := json.Unmarshal(msg.Value, &m); err == nil {
			handle(ctx, deps{queries, rdb, embedder, generator, producer, botID}, m)
		}

		// Commit whether or not we answered. Unlike the persister, we do NOT retry
		// on failure: the bot must never block the partition. At-most-once-ish, and
		// that's the right tradeoff for a non-critical consumer.
		if err := consumer.Commit(ctx, msg); err != nil {
			log.Printf("commit at offset %d: %v", msg.Offset, err)
		}
	}

	log.Println("bot shutting down")
}

// deps bundles what handle needs, so its signature stays readable.
type deps struct {
	queries   *sqlc.Queries
	rdb       *redis.Client
	embedder  *embed.Client
	generator *llm.Client
	producer  *broker.Producer
	botID     int64
}

func handle(ctx context.Context, d deps, m broker.ChatMessage) {
	// LOOP GUARD — the single most important line in this file. The bot's own
	// replies go back into the same log it's reading. Without this check, a reply
	// that happens to contain "@bot" would trigger another reply, forever, each one
	// costing a slow generation. Never let a consumer act on its own output.
	if m.UserID == d.botID {
		return
	}

	// Is this addressed to us?
	if !mentionPattern.MatchString(m.Body) {
		return
	}

	// Strip the mention to get the actual question — same pattern, so any case
	// that was matched above is also removed here.
	question := strings.TrimSpace(mentionPattern.ReplaceAllString(m.Body, ""))
	if question == "" {
		return
	}
	log.Printf("question from %s in room %d: %q", m.Username, m.RoomID, question)

	answer, err := answerQuestion(ctx, d, m.RoomID, question)
	if err != nil {
		log.Printf("could not answer: %v", err)
		return
	}

	if err := post(ctx, d, m.RoomID, answer); err != nil {
		log.Printf("could not post reply: %v", err)
	}
}

// answerQuestion is the RAG pipeline: RETRIEVE relevant messages, then GENERATE an
// answer grounded in them.
func answerQuestion(ctx context.Context, d deps, roomID int64, question string) (string, error) {
	// --- RETRIEVE ---
	embedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	vec, err := d.embedder.Embed(embedCtx, question)
	if err != nil {
		return "", fmt.Errorf("embed question: %w", err)
	}

	hits, err := d.queries.SearchMessages(ctx, sqlc.SearchMessagesParams{
		RoomID:         roomID,
		QueryEmbedding: pgvector.NewVector(vec),
	})
	if err != nil {
		return "", fmt.Errorf("retrieve: %w", err)
	}
	if len(hits) == 0 {
		return "I don't have any messages indexed for this room yet.", nil
	}

	// INSTRUMENTATION — log exactly what was retrieved, with scores, BEFORE the
	// model sees it. This is what makes a bad answer diagnosable: you can tell
	// whether retrieval fetched the wrong messages, or whether it fetched the right
	// ones and the model failed to use them. Judging retrieval by answer quality
	// alone confounds the two.
	log.Printf("retrieved %d candidates for %q:", len(hits), question)
	for i, h := range hits {
		log.Printf("  [%d] %.0f%% %s: %s", i+1, h.Similarity*100, h.Username, truncate(h.Body, 60))
	}

	// Filter before generating. Three exclusions, each for a different reason.
	kept := make([]sqlc.SearchMessagesRow, 0, maxContext)
	for _, h := range hits {
		switch {
		case h.UserID == d.botID:
			// Never ground an answer in our OWN previous answers. Generated text
			// treated as evidence is how a bot ends up citing itself and compounding
			// its own mistakes.
			continue
		case mentionPattern.MatchString(h.Body):
			// Skip questions addressed to the bot — including the one we're
			// answering right now, which the indexer may already have stored. A
			// question is not evidence.
			continue
		case h.Similarity < minSimilarity:
			// Below the cliff: noise, and noise costs prompt tokens.
			continue
		}
		kept = append(kept, h)
		if len(kept) == maxContext {
			break
		}
	}
	if len(kept) == 0 {
		return "I couldn't find anything relevant in this room's history.", nil
	}

	// Confidence gate. Note this checks the top of KEPT, not of hits: the raw top
	// hit is often the asking message itself (near-identical text scores high),
	// which would be false confidence. kept has already had mentions and the
	// bot's own replies removed, so kept[0] is the best real evidence we have.
	if kept[0].Similarity < minTopSimilarity {
		log.Printf("refusing: best real match only %.0f%%, below the %.0f%% gate",
			kept[0].Similarity*100, minTopSimilarity*100)
		return "I don't have enough in this room's history to answer that confidently.", nil
	}
	log.Printf("using %d of %d as context (top %.0f%%)", len(kept), len(hits), kept[0].Similarity*100)

	// --- GENERATE ---
	var b strings.Builder
	b.WriteString("Chat message excerpts, most relevant first:\n\n")
	for i, h := range kept {
		fmt.Fprintf(&b, "[%d] %s: %s\n", i+1, h.Username, h.Body)
	}
	fmt.Fprintf(&b, "\nQuestion: %s", question)

	// Generous timeout: a few hundred tokens on slow hardware takes minutes.
	genCtx, genCancel := context.WithTimeout(ctx, 4*time.Minute)
	defer genCancel()

	started := time.Now()
	answer, err := d.generator.Chat(genCtx, systemPrompt, b.String())
	if err != nil {
		return "", fmt.Errorf("generate: %w", err)
	}
	log.Printf("generated %d chars in %s", len(answer), time.Since(started).Round(time.Second))
	return strings.TrimSpace(answer), nil
}

// post publishes the bot's reply exactly the way the gateway publishes a human's:
// to Redis for live delivery, and to Kafka so it's persisted and indexed. The bot
// is just another producer — it needs no special path through the system.
func post(ctx context.Context, d deps, roomID int64, body string) error {
	payload, err := json.Marshal(broker.ChatMessage{
		RoomID:   roomID,
		UserID:   d.botID,
		Username: botUsername,
		Body:     body,
		SentAt:   time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	if err := d.rdb.Publish(ctx, fmt.Sprintf("room:%d", roomID), payload).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	if err := d.producer.Publish(ctx, strconv.FormatInt(roomID, 10), payload); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

// ensureBotUser looks up the bot's account and creates it on first run. The
// password is random and thrown away, so nobody can ever log in as the bot — it's
// a service account, not a person.
func ensureBotUser(ctx context.Context, queries *sqlc.Queries) (int64, error) {
	user, err := queries.GetUserByUsername(ctx, botUsername)
	if err == nil {
		return user.ID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return 0, err
	}
	hash, err := auth.HashPassword(hex.EncodeToString(secret))
	if err != nil {
		return 0, err
	}
	created, err := queries.CreateUser(ctx, sqlc.CreateUserParams{
		Username:     botUsername,
		PasswordHash: hash,
	})
	if err != nil {
		return 0, err
	}
	log.Printf("created bot account (user %d)", created.ID)
	return created.ID, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
