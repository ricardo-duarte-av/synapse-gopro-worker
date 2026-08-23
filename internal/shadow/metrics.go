package shadow

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// latencyBuckets span the range that matters: a small room resolves in
// milliseconds, a large one can take seconds.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

var (
	// shadowResults counts comparison outcomes. Unlike the persisted counters
	// in the diff log, these reset on restart; they are for rate and alerting.
	shadowResults = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "shadow_results_total",
		Help:      "Shadow comparison outcomes, by endpoint and result kind.",
	}, []string{"endpoint", "result"})

	// shadowSkipped counts comparisons that were not performed, which matter
	// because they mean the match rate is measured over less traffic than it
	// appears.
	shadowSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "shadow_skipped_total",
		Help:      "Shadow comparisons skipped, by endpoint and reason.",
	}, []string{"endpoint", "reason"})

	// authVerdicts compares our X-Matrix verification against Synapse's.
	// "we_accept_synapse_rejects" is the dangerous direction and must be zero
	// before any endpoint is served natively.
	authVerdicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gopro",
		Name:      "auth_verdicts_total",
		Help:      "X-Matrix verification verdicts compared against Synapse's.",
	}, []string{"result"})

	// shadowDuration is how long the native implementation took.
	shadowDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "shadow_duration_seconds",
		Help:      "Time taken by the native implementation during shadow comparison.",
		Buckets:   latencyBuckets,
	}, []string{"endpoint"})

	// shadowUpstreamDuration is how long Synapse took, recorded only for the
	// requests we actually shadowed.
	//
	// This exists because gopro_upstream_duration_seconds covers every request,
	// including the cancelled ones that shadow comparison deliberately skips.
	// On this deployment those are the majority and they are by definition the
	// slow ones, so comparing the two histograms directly flatters the native
	// implementation enormously. Only this metric and shadow_duration_seconds
	// describe the same population, so only these two may be compared.
	shadowUpstreamDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "shadow_upstream_duration_seconds",
		Help:      "Time taken by Synapse, for the same requests shadow comparison measured.",
		Buckets:   latencyBuckets,
	}, []string{"endpoint"})
)
