package replication

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics exposes the health of the invalidation stream.
//
// gopro_replication_connected is the one that matters operationally: while it
// is 0 the caches are empty and every request goes to the database, so a
// sustained 0 shows up as a latency regression rather than as wrong answers.
// There are no per-instance or per-room labels — Synapse names the publishing
// worker and the room in every row, and that belongs in logs, where cardinality
// is free.
type metrics struct {
	connected     prometheus.Gauge
	commands      *prometheus.CounterVec
	invalidations *prometheus.CounterVec
	flushes       *prometheus.CounterVec
	malformed     *prometheus.CounterVec
	lastMessage   prometheus.Gauge
}

// Registered once for the process: Prometheus panics on a duplicate
// registration, and a Subscriber may legitimately be rebuilt.
var (
	metricsOnce   sync.Once
	sharedMetrics *metrics
)

func newMetrics() *metrics {
	metricsOnce.Do(func() { sharedMetrics = buildMetrics() })
	return sharedMetrics
}

func buildMetrics() *metrics {
	m := &metrics{
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gopro_replication_connected",
			Help: "1 while subscribed to Synapse's replication stream. Caches only serve while this is 1.",
		}),
		commands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gopro_replication_commands_total",
			Help: "Replication commands received, by command (RDATA rows are labelled RDATA/<stream>).",
		}, []string{"command"}),
		invalidations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gopro_replication_invalidations_total",
			Help: "Cache invalidation rows received, by Synapse cache function.",
		}, []string{"cache_func"}),
		flushes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gopro_replication_cache_flushes_total",
			Help: "Times the caches were emptied, by reason.",
		}, []string{"reason"}),
		malformed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gopro_replication_malformed_total",
			Help: "Replication rows that could not be parsed, by reason. Each one empties the caches.",
		}, []string{"reason"}),
		lastMessage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gopro_replication_last_message_timestamp_seconds",
			Help: "Unix time of the last RDATA received. Staleness here means the stream went quiet.",
		}),
	}
	prometheus.MustRegister(m.connected, m.commands, m.invalidations,
		m.flushes, m.malformed, m.lastMessage)
	return m
}
