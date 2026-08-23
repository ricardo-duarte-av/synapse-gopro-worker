package difflog

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Collector exposes the persisted shadow statistics to Prometheus.
//
// The values are read from the Writer on each scrape rather than mirrored into
// separate counters, so Prometheus and stats.json can never disagree. Because
// the underlying totals are restored from disk at startup, these counters
// survive restarts — which is the point: a promotion gate measured in weeks
// cannot be judged from counters that reset on every deploy.
type Collector struct {
	w *Writer

	compared     *prometheus.Desc
	matched      *prometheus.Desc
	mismatched   *prometheus.Desc
	matchRate    *prometheus.Desc
	lastMismatch *prometheus.Desc

	logged    *prometheus.Desc
	dropped   *prometheus.Desc
	writeErrs *prometheus.Desc
	rotations *prometheus.Desc
	since     *prometheus.Desc
	restarts  *prometheus.Desc
}

// NewCollector builds a Collector for w. A nil Writer yields a nil Collector,
// which prometheus.Registerer must not be handed; use Register instead.
func NewCollector(w *Writer) *Collector {
	if w == nil {
		return nil
	}
	ep := []string{"endpoint"}
	return &Collector{
		w: w,
		compared: prometheus.NewDesc("gopro_shadow_compared_total",
			"Shadow comparisons performed, cumulative across restarts.", ep, nil),
		matched: prometheus.NewDesc("gopro_shadow_matched_total",
			"Shadow comparisons where our answer agreed with Synapse.", ep, nil),
		mismatched: prometheus.NewDesc("gopro_shadow_mismatched_total",
			"Shadow comparisons where our answer differed from Synapse.", ep, nil),
		matchRate: prometheus.NewDesc("gopro_shadow_match_rate",
			"Fraction of shadow comparisons that agreed, over the whole run.", ep, nil),
		lastMismatch: prometheus.NewDesc("gopro_shadow_last_mismatch_timestamp_seconds",
			"Unix time of the most recent disagreement. Absent if there has never been one.", ep, nil),

		logged: prometheus.NewDesc("gopro_difflog_logged_total",
			"Diff records written to disk.", nil, nil),
		dropped: prometheus.NewDesc("gopro_difflog_dropped_total",
			"Diff records dropped because the writer queue was full. Non-zero means the log is incomplete.", nil, nil),
		writeErrs: prometheus.NewDesc("gopro_difflog_write_errors_total",
			"Failures writing the diff log or its statistics.", nil, nil),
		rotations: prometheus.NewDesc("gopro_difflog_rotations_total",
			"Diff log file rotations.", nil, nil),
		since: prometheus.NewDesc("gopro_shadow_since_timestamp_seconds",
			"Unix time when shadow comparison first started, preserved across restarts.", nil, nil),
		restarts: prometheus.NewDesc("gopro_shadow_restarts_total",
			"Worker starts against this diff log.", nil, nil),
	}
}

// Register adds the collector to reg when diff logging is enabled, and does
// nothing otherwise.
func Register(reg prometheus.Registerer, w *Writer) error {
	c := NewCollector(w)
	if c == nil {
		return nil
	}
	return reg.Register(c)
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.compared, c.matched, c.mismatched, c.matchRate, c.lastMismatch,
		c.logged, c.dropped, c.writeErrs, c.rotations, c.since, c.restarts,
	} {
		ch <- d
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s := c.w.Snapshot()

	counter := func(d *prometheus.Desc, v uint64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	for endpoint, e := range s.Endpoints {
		counter(c.compared, e.Compared, endpoint)
		counter(c.matched, e.Matched, endpoint)
		counter(c.mismatched, e.Mismatched, endpoint)
		gauge(c.matchRate, e.MatchRate(), endpoint)
		if e.LastMismatch != nil {
			gauge(c.lastMismatch, float64(e.LastMismatch.Unix()), endpoint)
		}
	}

	counter(c.logged, s.Logged)
	counter(c.dropped, s.Dropped)
	counter(c.writeErrs, s.WriteErrors)
	counter(c.rotations, s.Rotations)
	counter(c.restarts, uint64(s.Restarts))
	if !s.Since.IsZero() {
		gauge(c.since, float64(s.Since.Unix()))
	}
}
