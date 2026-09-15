// Package metrics exposes the one number that would have caught the outage in
// H5 of docs/HARDENING.md.
//
// The persister was not running for several hours and NOTHING reported it. Not
// the gateway, not the browser, no error frame anywhere — because the gateway's
// promise is "this message reached Kafka", and it kept that promise perfectly.
// Whether anything CONSUMED the message is a separate guarantee, and nobody was
// watching it. It eventually surfaced as "my messages disappear when I switch
// rooms", hours later and looking like a bug in history replay.
//
// Consumer lag is the right signal because it catches both failure modes with
// one number: a consumer that has died, and one that is merely too slow. A
// liveness probe catches neither — in both cases the process is fine.
package metrics

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// A GAUGE, not a counter. Lag goes up and down — it is a depth, a queue
	// length, not an accumulating total. Modelling it as a counter would make
	// rate() meaningless and every dashboard wrong.
	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "chat_go_consumer_lag",
		Help: "Messages in the topic beyond this consumer group's committed offset.",
	}, []string{"group"})

	// Without this, a broker we cannot reach leaves the gauge holding its last
	// value — which looks like a perfectly healthy zero-lag consumer. That is
	// the same class of invisible failure the whole package exists for, so it
	// gets its own metric rather than only a log line.
	lagReadErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chat_go_consumer_lag_read_errors_total",
		Help: "Failed attempts to read consumer lag from Kafka.",
	}, []string{"group"})
)

// Serve starts an HTTP server exposing /metrics, and returns immediately.
//
// Deliberately NOT fatal on error. A consumer that cannot bind its metrics port
// should keep consuming: losing observability is bad, losing the pipeline
// because observability failed is worse.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("metrics listening on %s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()
}

// WatchGroups polls the lag of every named group and publishes it, blocking
// until ctx is cancelled.
//
// Polled on a ticker rather than computed inside the /metrics handler, for two
// reasons. Each read costs several round trips to Kafka, and a scrape endpoint
// that makes network calls can hang the scrape. And it decouples measurement
// from whoever is scraping, so the number means the same thing whether
// Prometheus asks every 15 seconds or a human curls it twice in a row.
func WatchGroups(ctx context.Context, groups []string, every time.Duration, read func(context.Context, string) (int64, error)) {
	// Create every series at startup, before the first successful read.
	//
	// A missing series and a zero one behave completely differently in an
	// alert: `chat_go_consumer_lag > 1000` never fires for a series that does
	// not exist. Without this, a consumer group that was broken from the very
	// first scrape would be invisible to the alert written to catch it.
	for _, g := range groups {
		consumerLag.WithLabelValues(g)
		lagReadErrors.WithLabelValues(g)
	}

	// Read once immediately rather than waiting a full interval, so a freshly
	// started exporter has numbers straight away.
	poll(ctx, groups, read)

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			poll(ctx, groups, read)
		case <-ctx.Done():
			return
		}
	}
}

func poll(ctx context.Context, groups []string, read func(context.Context, string) (int64, error)) {
	for _, group := range groups {
		// Bounded, so a broker that accepts the connection and then says nothing
		// cannot wedge the loop — which would freeze every gauge at a
		// healthy-looking value and recreate the exact blindness this exists to
		// remove.
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		lag, err := read(readCtx, group)
		cancel()

		if err != nil {
			lagReadErrors.WithLabelValues(group).Inc()
			log.Printf("lag for %q: %v", group, err)
			continue
		}
		consumerLag.WithLabelValues(group).Set(float64(lag))
	}
}
