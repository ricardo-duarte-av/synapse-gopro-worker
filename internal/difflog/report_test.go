package difflog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestReport(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, StatsEvery: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		w.Observe("state_ids", true)
	}
	w.Observe("state", false)
	w.Log(testRecord("state"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var sb strings.Builder
	if err := Report(dir, &sb); err != nil {
		t.Fatal(err)
	}
	got := sb.String()

	for _, want := range []string{"state_ids", "100", "state", "Records logged: 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("report is missing %q:\n%s", want, got)
		}
	}
	// A fresh run must never read as ready; the soak has not elapsed.
	if !strings.Contains(got, "soak elapsed") {
		t.Errorf("report should say the soak is incomplete:\n%s", got)
	}
	// An endpoint with a recent mismatch must be called out clearly.
	if !strings.Contains(got, "NOT READY") {
		t.Errorf("report should flag the mismatched endpoint as not ready:\n%s", got)
	}
}

func TestReportWarnsOnDroppedRecords(t *testing.T) {
	// A lossy log invalidates the promotion gate, so it must be loud.
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, QueueSize: 1, StatsEvery: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for range 5000 {
		w.Log(testRecord("state"))
	}
	w.Observe("state", true)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var sb strings.Builder
	if err := Report(dir, &sb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sb.String(), "WARNING") || !strings.Contains(sb.String(), "incomplete") {
		t.Errorf("dropped records were not warned about:\n%s", sb.String())
	}
}

func TestReportOnEmptyDir(t *testing.T) {
	if err := Report(t.TempDir(), &strings.Builder{}); err == nil {
		t.Error("Report on a directory with no statistics should error")
	}
}

func TestVerdict(t *testing.T) {
	long := 8 * 24 * time.Hour
	recent := time.Now().Add(-time.Hour)
	old := time.Now().Add(-10 * 24 * time.Hour)

	cases := []struct {
		name    string
		stats   *EndpointStats
		elapsed time.Duration
		want    string
	}{
		{"no data", &EndpointStats{}, long, "no comparisons yet"},
		{"recent mismatch", &EndpointStats{Compared: 10, Mismatched: 1, LastMismatch: &recent}, long, "NOT READY"},
		{"old mismatch", &EndpointStats{Compared: 10, Mismatched: 1, LastMismatch: &old}, long, "ready"},
		{"clean but young", &EndpointStats{Compared: 10, Matched: 10}, time.Hour, "soak elapsed"},
		{"clean and soaked", &EndpointStats{Compared: 10, Matched: 10}, long, "ready"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := verdict(tc.stats, tc.elapsed); !strings.Contains(got, tc.want) {
				t.Errorf("verdict = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

func TestStatsFileIsWorldReadable(t *testing.T) {
	// The stats file is read from outside the container, so it must not
	// inherit CreateTemp's 0600.
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, StatsEvery: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.Observe("state", true)
	time.Sleep(80 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(dir, statsName))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o644 {
		t.Errorf("stats.json mode = %o, want 644", perm)
	}
}

func TestReportWithNoEndpoints(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, StatsEvery: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var sb strings.Builder
	if err := Report(dir, &sb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sb.String(), "no comparisons recorded yet") {
		t.Errorf("empty report should explain itself:\n%s", sb.String())
	}
}

func TestCollectorExposesPersistedStats(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for range 9 {
		w.Observe("state_ids", true)
	}
	w.Observe("state_ids", false)
	w.Log(testRecord("state_ids"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the collector must report the totals restored from disk, not
	// start from zero. This is the whole reason the stats file exists.
	w2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg, w2); err != nil {
		t.Fatal(err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			name := f.GetName()
			for _, l := range m.GetLabel() {
				name += "/" + l.GetValue()
			}
			switch {
			case m.GetCounter() != nil:
				got[name] = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				got[name] = m.GetGauge().GetValue()
			}
		}
	}

	for name, want := range map[string]float64{
		"gopro_shadow_compared_total/state_ids":   10,
		"gopro_shadow_matched_total/state_ids":    9,
		"gopro_shadow_mismatched_total/state_ids": 1,
		"gopro_shadow_match_rate/state_ids":       0.9,
		"gopro_difflog_logged_total":              1,
		"gopro_shadow_restarts_total":             2,
	} {
		if got[name] != want {
			t.Errorf("%s = %v, want %v", name, got[name], want)
		}
	}
	if _, ok := got["gopro_shadow_last_mismatch_timestamp_seconds/state_ids"]; !ok {
		t.Error("last mismatch timestamp was not exported")
	}
}

func TestRegisterWithDiffLogDisabled(t *testing.T) {
	// Proxy-only mode has no Writer; registration must be a no-op rather than
	// registering a collector that would panic on scrape.
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg, nil); err != nil {
		t.Fatalf("Register(nil) = %v, want nil", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 {
		t.Errorf("registered %d metric families, want 0", len(families))
	}
}
