package store

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// storeMetrics exposes counters for query paths that have a cached shortcut.
//
// Registered once for the process, because Prometheus panics on a duplicate and
// a Store may legitimately be rebuilt in tests.
type metrics struct {
	// filteredState counts filtered state-group resolutions by whether the
	// whole map was already cached. The ratio is the thing to watch: it says
	// how much /event benefits from state groups that /state_ids has already
	// resolved, which is a question about traffic overlap that only production
	// can answer.
	filteredState *prometheus.CounterVec
}

var (
	metricsOnce   sync.Once
	sharedMetrics *metrics
)

func storeMetrics() *metrics {
	metricsOnce.Do(func() {
		sharedMetrics = &metrics{
			filteredState: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "gopro_filtered_state_total",
				Help: "Filtered state-group resolutions, by query kind and whether the whole map was already cached.",
			}, []string{"query", "source"}),
		}
		prometheus.MustRegister(sharedMetrics.filteredState)
	})
	return sharedMetrics
}
