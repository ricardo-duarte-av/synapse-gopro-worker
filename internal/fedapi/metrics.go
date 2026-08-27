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

	// fallbackUpstream records what Synapse did for a request that fell back,
	// and how long the client waited in total.
	//
	// Without this a fallback is invisible past the moment it happens: the
	// upstream histogram covers every request, so there is no way to ask "did
	// Synapse actually answer the one we gave up on". A timeout fallback is
	// the expensive case -- the client pays our budget *and* Synapse's -- so
	// it is the one worth being able to account for.
	fallbackUpstream = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gopro_native_fallback_upstream_total",
		Help: "Outcome at Synapse for requests that fell back, by fallback reason and upstream status class.",
	}, []string{"endpoint", "reason", "upstream"})

	// fallbackTotalDuration is the whole time the client waited on a
	// fallen-back request: our attempt plus Synapse's.
	fallbackTotalDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gopro_native_fallback_total_seconds",
		Help:    "End-to-end time for a request that fell back, covering our attempt and Synapse's answer.",
		Buckets: latencyBuckets,
	}, []string{"endpoint", "reason"})

	// nativeDuration measures only requests actually served natively, so it
	// describes what remote servers experienced rather than what a shadow
	// comparison computed.
	// upstreamSkipped counts requests never forwarded because the client had
	// already hung up.
	//
	// This is the work Synapse is spared. It is worth its own counter rather
	// than being inferred from the cancelled-request count, because those two
	// differ: a request can be cancelled after we forwarded it, and only the
	// ones cancelled *before* represent work avoided.
	upstreamSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gopro_upstream_skipped_total",
		Help: "Requests not forwarded to Synapse because the client had already gone.",
	}, []string{"endpoint"})

	nativeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gopro_native_duration_seconds",
		Help:    "Time to produce an answer that was served natively.",
		Buckets: latencyBuckets,
	}, []string{"endpoint"})

	// nativeRequestDuration covers the whole served attempt: X-Matrix
	// verification and then the answer.
	//
	// nativeDuration times only the answer, which is the part we optimise but
	// not the part a remote server waits for. Verification sits in front of it
	// and is not free -- an origin whose keys are not cached costs a network
	// fetch, and on this deployment that has taken tens of seconds when the
	// origin's key server is unhealthy (a dead AAAA record, in the case that
	// prompted looking). Reporting the answer time as "native latency" would
	// therefore report the number we control rather than the number the
	// federation sees.
	//
	// The limiter is deliberately outside this: it is acquired by the caller
	// before the attempt begins, and its wait is already measured by
	// gopro_rate_limit_queue_wait_seconds. Keeping it out means the gap
	// between this and nativeDuration is verification and nothing else.
	nativeRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gopro_native_request_seconds",
		Help:    "End-to-end time for a natively served request: verification plus the answer.",
		Buckets: latencyBuckets,
	}, []string{"endpoint"})
)

var latencyBuckets = []float64{
	.0001, .00025, .0005, .00075, .001, .0015, .002, .003, .004, .005,
	.0075, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
}
