package main

import (
	"context"
	_ "embed"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/config"
	"github.com/omkar619-dev/chat-go/internal/db"
	"github.com/omkar619-dev/chat-go/internal/embed"
	"github.com/omkar619-dev/chat-go/internal/httpapi"
	"github.com/omkar619-dev/chat-go/internal/hub"
	"github.com/omkar619-dev/chat-go/internal/llm"
	"github.com/omkar619-dev/chat-go/internal/presence"
	"github.com/omkar619-dev/chat-go/internal/redisclient"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres/sqlc"
)

//go:embed index.html
var indexHTML []byte

func main() {
	// 1. Load config (env vars, with local-dev defaults).
	cfg := config.Load()

	// Refuse to run on a placeholder signing key. Every other setting has a safe
	// local default; this one does not, because the failure is silent — see
	// config.RequireJWTSecret. Checked before anything is dialled so a
	// misconfigured instance dies immediately rather than serving forgeable tokens.
	if err := cfg.RequireJWTSecret(); err != nil {
		log.Fatalf("refusing to start: %v", err)
	}

	// 2. Root context that cancels on Ctrl+C / SIGTERM — the basis of graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 3. Connect to Postgres (fatal if it fails — no point running without a DB).
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	log.Println("connected to postgres")

	// 4. Connect to Redis.
	rdb, err := redisclient.Connect(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()
	log.Println("connected to redis")

	// Bind sqlc's typed queries to the pool.
	queries := sqlc.New(pool)

	// Kafka producer for the durable message log.
	producer := broker.NewProducer(cfg.KafkaBroker, cfg.KafkaTopic)
	defer producer.Close()
	log.Println("kafka producer ready")

	// Embedding client — used only to turn a SEARCH QUERY into a vector. Message
	// embedding happens in the separate indexer, off the hot path.
	embedder := embed.New(cfg.OllamaURL, cfg.EmbedModel)

	// Generation client — used only for streaming "catch me up" summaries, which
	// are per-user and go straight down one socket. The RAG bot has its own client
	// in its own process; these two never share state.
	generator := llm.New(cfg.OllamaURL, cfg.ChatModel)

	// The hub is in its own variable because two things need it: the WebSocket
	// handler (to join rooms) and the presence tracker (to ask who is connected).
	roomHub := hub.New(rdb, cfg.HubBuffer)

	// Presence: every 15 seconds, write this gateway's connected users into Redis
	// so the OTHER gateways can see them too. Takes the root context, so Ctrl+C
	// stops it along with everything else.
	go presence.New(rdb, roomHub).Run(ctx)

	h := &httpapi.Handlers{
		Queries:   queries,
		JWTSecret: cfg.JWTSecret,
		Redis:     rdb,
		// One Redis subscription per ROOM for this process, rather than one per
		// connection. Shares the same client: PUBLISH still uses the pool, only
		// SUBSCRIBE moves behind the hub.
		Hub:       roomHub,
		Producer:  producer,
		Embedder:  embedder,
		Generator: generator,
	}

	// 5. Build the HTTP router with a couple of standard middlewares.
	r := chi.NewRouter()
	r.Use(middleware.Logger)    // logs each request (method, path, status, duration)
	r.Use(middleware.Recoverer) // turns a panic in a handler into a 500 instead of crashing the server
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	r.Post("/register", h.Register)
	r.Post("/login", h.Login)
	r.With(h.RequireAuth).Get("/me", h.Me) // protected: needs a valid Bearer token
	r.Route("/rooms", func(pr chi.Router) {
		pr.Use(h.RequireAuth) // every /rooms route requires a valid token
		pr.Post("/", h.CreateRoom)
		pr.Get("/", h.ListRooms)
		pr.Post("/{id}/join", h.JoinRoom)
		pr.Get("/{id}/search", h.SearchRoom) // semantic search over the room's history
	})
	r.Get("/ws", h.WS) // WebSocket upgrade; auth via ?token= query param

	// 6. Wrap the router in an http.Server so we can shut it down on command.
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: r}

	// 7. Serve in a goroutine so main() can move on and wait for the shutdown signal.
	go func() {
		log.Printf("gateway listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	// 8. Block here until Ctrl+C / SIGTERM fires (ctx gets cancelled), then shut down.
	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("bye")
}
