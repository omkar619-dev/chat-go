// Command migrate applies the database schema and exits.
//
// It exists because a Helm install has to come up complete. Locally the schema
// is applied by hand (`docker compose exec -T postgres psql ...`), which is
// fine for one machine and useless for a cluster: nobody is there to run it.
//
// It is a SEPARATE process rather than something the gateway does at startup,
// for two reasons. Two gateway replicas both creating the same tables is a
// race whose best case is wasted work and whose worst case is a deadlock
// between two concurrent CREATE INDEX statements. And a process that migrates
// the database it is about to serve has no way to report "the migration
// failed" other than by also failing to serve — the two outcomes become
// indistinguishable from outside, which is exactly the ambiguity that made the
// dead persister in phase 6 so hard to spot.
//
// As a Kubernetes Job it runs exactly once per install, its exit code is the
// success signal, and its logs outlive it.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omkar619-dev/chat-go/internal/config"
	"github.com/omkar619-dev/chat-go/internal/db"
	"github.com/omkar619-dev/chat-go/internal/repository/postgres"
)

const (
	// Kubernetes starts everything in a release at once, so this Job routinely
	// wins the race against Postgres finishing its first-boot initdb. Waiting
	// is not a workaround for missing ordering; ordering between pods is not
	// something Kubernetes offers, so every client has to tolerate its
	// dependencies not being up yet.
	//
	// Three minutes, and the size is MEASURED rather than guessed. The first
	// run of this job on k3d failed at 30 attempts (60s) and only succeeded on
	// the Job's automatic retry — because the failure is not "Postgres refused
	// the connection", it is "the hostname does not exist":
	//
	//	lookup chat-go-postgres on 10.43.0.10:53: no such host
	//
	// Postgres is behind a HEADLESS Service, and CoreDNS publishes records for
	// a headless Service only while it has READY endpoints. Until initdb
	// finishes and the readiness probe passes, the name is absent rather than
	// unreachable. So this budget has to cover the whole of first-boot initdb,
	// not merely the moment between the socket opening and accepting.
	//
	// 60 seconds barely covered it on a laptop with an SSD. The homelab's
	// target is a Core 2 Duo, where the same work takes considerably longer,
	// and a migration that fails there would look like a broken chart.
	connectAttempts = 90

	// A FIXED interval, deliberately not the exponential backoff used in
	// internal/broker. Backoff is for a dependency under strain, where retrying
	// hard makes things worse. This is a boot race against a process that will
	// be ready within seconds, and exponential waits would still be sleeping 30
	// seconds after Postgres started answering.
	connectInterval = 2 * time.Second
)

func main() {
	cfg := config.Load()

	// A ceiling on the whole job. Without it a Postgres that never comes up
	// leaves this pod running indefinitely, and Helm waiting on it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("migrate: %v", err)
	}
	defer pool.Close()

	// The entire schema goes in ONE Exec, and that is load-bearing. pgx sends a
	// query with no arguments over the simple protocol, which accepts several
	// statements in one round trip, and Postgres runs such a batch inside an
	// implicit transaction. So either every statement applies or none does.
	// Splitting the file on semicolons and looping would give up that
	// guarantee, and a half-applied schema is the state that is genuinely
	// awkward to recover from.
	if _, err := pool.Exec(ctx, postgres.Schema); err != nil {
		log.Fatalf("migrate: apply schema: %v", err)
	}

	log.Println("migrate: schema applied")
}

// connect retries until Postgres answers or the attempts run out.
//
// db.Connect already pings, so a returned pool is one that has completed a
// round trip — not merely one that resolved a hostname. The distinction
// matters here: the pod's DNS name exists the moment the Service does, well
// before anything is listening behind it.
func connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	var lastErr error

	for attempt := 1; attempt <= connectAttempts; attempt++ {
		pool, err := db.Connect(ctx, databaseURL)
		if err == nil {
			return pool, nil
		}
		lastErr = err
		log.Printf("migrate: postgres not ready (attempt %d/%d): %v", attempt, connectAttempts, err)

		select {
		case <-time.After(connectInterval):
		case <-ctx.Done():
			return nil, fmt.Errorf("gave up waiting for postgres: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("postgres unreachable after %d attempts: %w", connectAttempts, lastErr)
}
