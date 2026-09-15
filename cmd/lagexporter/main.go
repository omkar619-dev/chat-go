// Command lagexporter publishes how far each Kafka consumer group is behind.
//
// It exists as a SEPARATE process for one reason, and the reason is the whole
// point: a consumer that has died cannot report its own lag. Every number
// available inside a consumer — kafka-go's Reader.Lag(), the high water mark on
// the last fetched message — stops updating at exactly the moment the failure
// happens, and a frozen zero looks identical to a consumer keeping up.
//
// That was the H5 outage in docs/HARDENING.md. The persister was not running
// for several hours and nothing reported it: the gateway's promise is "this
// message reached Kafka", it kept that promise perfectly, and whether anything
// CONSUMED the message was a separate guarantee nobody was watching. It
// surfaced hours later as "my messages disappear when I switch rooms", which
// looks like a bug in history replay and is not.
//
// Lag is the right signal because one number catches both failure modes: a
// consumer that has died, and one that is merely too slow. A liveness probe
// catches neither — in both cases the process is perfectly healthy.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/omkar619-dev/chat-go/internal/broker"
	"github.com/omkar619-dev/chat-go/internal/config"
	"github.com/omkar619-dev/chat-go/internal/metrics"
)

// The consumer groups to watch.
//
// Hardcoded on purpose. These are not a deployment choice — they are fixed by
// the source, in cmd/persister, cmd/indexer and cmd/bot, where the group id is
// a string literal passed to broker.NewConsumer. Making them configurable would
// invite a deployment where a group is silently not watched, which is the one
// outcome this program exists to prevent.
var groups = []string{"persister", "indexer", "bot"}

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lag := broker.NewLagReader(cfg.KafkaBroker, cfg.KafkaTopic)

	metrics.Serve(cfg.MetricsAddr)
	log.Printf("watching lag for %v on topic %q every %s",
		groups, cfg.KafkaTopic, cfg.LagPollInterval)

	// Blocks until the signal arrives. This process has nothing else to do, so
	// unlike the consumers it does not need its polling on a goroutine.
	metrics.WatchGroups(ctx, groups, cfg.LagPollInterval, lag.Lag)

	log.Println("lagexporter shutting down")
}
