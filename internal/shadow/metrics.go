package shadow

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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

	// shadowDuration is how long the native implementation took. Compared with
	// gopro_upstream_duration_seconds, this is the whole point of the project.
	shadowDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gopro",
		Name:      "shadow_duration_seconds",
		Help:      "Time taken by the native implementation during shadow comparison.",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	}, []string{"endpoint"})
)
