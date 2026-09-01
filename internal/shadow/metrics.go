package shadow

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// latencyBuckets span the range that matters: a small room resolves in
// milliseconds, a large one can take seconds.
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

// canaryCompared counts answers we served natively that were afterwards
// checked against Synapse. Comparing it with gopro_native_served_total says
// what fraction of served answers were actually verified -- the number a
// promotion decision should rest on.
var canaryCompared = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "gopro",
	Name:      "canary_compared_total",
	Help:      "Natively served answers that were compared against Synapse afterwards.",
}, []string{"endpoint"})

// verifyWaited counts verifications that found every comparison slot busy and
// waited for one.
//
// A rising count is the early warning that shedding is close: the verified
// share is still 1.0 while this climbs, and only starts falling once the wait
// itself times out. Reading it alongside the verified share distinguishes
// "comfortable" from "coping".
var verifyWaited = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gopro_shadow_verify_waited_total",
	Help: "Verifications of a served answer that had to wait for a comparison slot.",
}, []string{"endpoint"})

// stateDiagnoses counts second-pass diagnoses of /state digest mismatches.
//
// The outcomes are not interchangeable. "diagnosed" means the disagreement
// reproduced and the events are named in the log; "not_reproducible" means it
// did not, which points at an unstable answer rather than a wrong one; and
// "failed" means the diagnosis itself could not run, leaving the original
// mismatch standing with only counts and digests behind it.
var stateDiagnoses = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gopro_state_diagnoses_total",
	Help: "Second-pass diagnoses of /state digest mismatches, by outcome.",
}, []string{"outcome"})
