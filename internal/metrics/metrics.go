// Package metrics defines the Prometheus instrumentation for the worker.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// latencyBuckets span the range that matters for these endpoints: /state_ids on
// a small room is single-digit milliseconds, while /state on a large room can
// take seconds.
var latencyBuckets = []float64{
	.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
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

	// RateLimitHosts reports how many origin servers currently have rate limit
	// state, which is also a rough measure of how many servers are talking to us.
	RateLimitHosts = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "gopro",
		Name:      "rate_limit_hosts",
		Help:      "Origin servers with live rate limit state.",
	})

	// ResponseBytes observes response sizes, which drive the cost of /state.
	ResponseBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "response_bytes",
		Help:      "Size of federation responses in bytes.",
		Buckets:   prometheus.ExponentialBuckets(256, 4, 10),
	}, []string{"endpoint"})
)
