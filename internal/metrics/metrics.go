// Package metrics defines the Prometheus instrumentation for the worker.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// latencyBuckets span the range that matters for these endpoints.
//
// The sub-millisecond end is not decoration. A warm /event resolves in about
// 0.9ms, so with the old floor of 1ms every fast request landed in the same
// bucket and the reported p50 was linear interpolation across 1ms..2.5ms
// rather than a measurement. Half the range we care about was invisible.
//
// The top end stays coarse on purpose: above 100ms the question is only "how
// bad", and /state_ids on a large room can take seconds.
var latencyBuckets = []float64{
	.0001, .00025, .0005, .00075, .001, .0015, .002, .003, .004, .005,
	.0075, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
}

// RegisterRateLimitHosts exports how many origin servers currently have rate
// limit state.
//
// Sampled on scrape rather than pushed from a ticker: a pushed gauge reads zero
// until the first tick fires, which for a ten-minute cleanup loop means it is
// wrong for ten minutes after every restart.
func RegisterRateLimitHosts(hosts func() int) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "gopro",
		Name:      "rate_limit_hosts",
		Help:      "Origin servers with live rate limit state.",
	}, func() float64 { return float64(hosts()) })
}

// RegisterCaches exports cache statistics, sampled on scrape.
//
// Cache counters live inside the cache itself rather than being incremented
// alongside it, so the two can never disagree about what happened.
func RegisterCaches(stats func() []cache.Stats) {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("gopro_cache_"+name, help, []string{"cache"}, nil)
	}
	c := &cacheCollector{
		stats:     stats,
		hits:      desc("hits_total", "Cache lookups served from memory."),
		misses:    desc("misses_total", "Cache lookups that required a database read."),
		evictions: desc("evictions_total", "Entries evicted to stay within the size bound."),
		entries:   desc("entries", "Entries currently cached."),
		bytes:     desc("bytes", "Approximate memory used by cached entries."),
		maxBytes:  desc("max_bytes", "Configured size bound."),
		hitRate:   desc("hit_rate", "Fraction of lookups served from memory since startup."),
	}
	prometheus.MustRegister(c)
}

type cacheCollector struct {
	stats                                                      func() []cache.Stats
	hits, misses, evictions, entries, bytes, maxBytes, hitRate *prometheus.Desc
}

func (c *cacheCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.hits, c.misses, c.evictions, c.entries, c.bytes, c.maxBytes, c.hitRate} {
		ch <- d
	}
}

func (c *cacheCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.stats() {
		ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(s.Hits), s.Name)
		ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(s.Misses), s.Name)
		ch <- prometheus.MustNewConstMetric(c.evictions, prometheus.CounterValue, float64(s.Evictions), s.Name)
		ch <- prometheus.MustNewConstMetric(c.entries, prometheus.GaugeValue, float64(s.Entries), s.Name)
		ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(s.Bytes), s.Name)
		ch <- prometheus.MustNewConstMetric(c.maxBytes, prometheus.GaugeValue, float64(s.MaxBytes), s.Name)
		ch <- prometheus.MustNewConstMetric(c.hitRate, prometheus.GaugeValue, s.HitRate(), s.Name)
	}
}

var (
	// RequestsTotal counts requests by endpoint, serving mode and status.
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "requests_total",
		Help:      "Federation requests handled, by endpoint, mode and status.",
	}, []string{"endpoint", "mode", "status"})

	// UpstreamDuration observes how long the Synapse worker took. This is the
	// baseline the native implementation is measured against.
	UpstreamDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "upstream_duration_seconds",
		Help:      "Time spent waiting on the upstream Synapse worker.",
		Buckets:   latencyBuckets,
	}, []string{"endpoint"})

	// UpstreamErrorsTotal counts requests where the upstream was unreachable.
	UpstreamErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "upstream_errors_total",
		Help:      "Requests that failed to reach the upstream Synapse worker.",
	}, []string{"endpoint", "backend"})

	// RateLimitedTotal counts requests refused by the per-origin rate limit.
	//
	// There is deliberately no origin label here. This server has exchanged
	// keys with over forty thousand others, and one label value per origin
	// would create a time series for each, which is exactly the cardinality
	// explosion Prometheus is worst at. Which server is being limited is
	// recorded in the logs, where high cardinality is free.
	RateLimitedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "rate_limited_total",
		Help:      "Requests rejected with 429 by the per-origin federation rate limit.",
	}, []string{"endpoint"})

	// RateLimitSleptTotal counts requests deliberately delayed. Sleeping is
	// throttling too, and it starts well before any rejection, so this is the
	// earlier warning of the two.
	RateLimitSleptTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "rate_limit_slept_total",
		Help:      "Requests delayed by the per-origin federation rate limit.",
	}, []string{"endpoint"})

	// RateLimitQueueWait observes how long requests spend waiting on the
	// limiter, which is latency we add rather than latency we measure.
	RateLimitQueueWait = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "rate_limit_queue_wait_seconds",
		Help:      "Time requests spent waiting on the per-origin rate limiter.",
		Buckets:   []float64{.001, .005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"endpoint"})

	// RateLimitedOrigins counts distinct origins limited since start, so the
	// question "is this one noisy server or many?" is answerable without a
	// per-origin label.
	RateLimitedOrigins = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gopro",
		Name:      "rate_limited_origins",
		Help:      "Distinct origin servers rejected by the rate limit since startup.",
	})

	// ResponseBytes observes response sizes, which drive the cost of /state.
	ResponseBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "response_bytes",
		Help:      "Size of federation responses in bytes.",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 10),
	}, []string{"endpoint"})
)
