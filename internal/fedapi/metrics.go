package fedapi

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// nativeServed counts requests answered from our own implementation, which
	// in canary and native modes are the ones that actually reach a remote
	// server.
	nativeServed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gopro_native_served_total",
		Help: "Requests answered by the native implementation, by endpoint and status class.",
	}, []string{"endpoint", "status"})

	// nativeFallback counts requests that were meant to be answered natively
	// and were proxied instead.
	//
	// This is the number that says whether a canary is healthy. A fallback is
	// not an outage — the client gets Synapse's answer either way — but a
	// rising rate means the native path is failing on real traffic, and the
	// reason label says whether that is our code, our verifier, or a timeout.
	nativeFallback = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gopro_native_fallback_total",
		Help: "Requests that fell back to Synapse instead of being answered natively, by reason.",
	}, []string{"endpoint", "reason"})

	// nativeFallbackServed counts requests that were sampled into the canary
	// and ended up served by Synapse anyway. It is the denominator for the
	// fallback reasons: a canary at 5% that falls back on all of them is
	// indistinguishable from shadow mode, and should look like it in metrics
	// rather than quietly appearing healthy.
	nativeFallbackServed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gopro_native_fallback_served_total",
		Help: "Requests sampled for native serving that were proxied instead.",
	}, []string{"endpoint"})

	// nativeDuration measures only requests actually served natively, so it
	// describes what remote servers experienced rather than what a shadow
	// comparison computed.
	nativeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gopro_native_duration_seconds",
		Help:    "Time to produce an answer that was served natively.",
		Buckets: latencyBuckets,
	}, []string{"endpoint"})
)

var latencyBuckets = []float64{
	.0001, .00025, .0005, .00075, .001, .0015, .002, .003, .004, .005,
	.0075, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
}
