package difflog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Stats are cumulative counters that survive restarts.
//
// Prometheus counters reset when the process restarts, which is useless for a
// promotion gate phrased as "seven days at 100% agreement". These are
// checkpointed to disk so the record spans the whole shadow run.
type Stats struct {
	// Since is when shadow comparison first started, preserved across restarts.
	Since   time.Time `json:"since"`
	Updated time.Time `json:"updated"`
	// Restarts counts how many times the worker has started against this log.
	Restarts int `json:"restarts"`

	// Compared, Matched and Mismatched count shadow comparisons overall.
	Compared   uint64 `json:"compared"`
	Matched    uint64 `json:"matched"`
	Mismatched uint64 `json:"mismatched"`

	// Logged counts records actually written; Dropped counts records lost to a
	// full queue. Dropped being non-zero means the log is incomplete and the
	// promotion gate cannot be trusted.
	Logged  uint64 `json:"logged"`
	Dropped uint64 `json:"dropped"`

	Rotations   uint64 `json:"rotations"`
	WriteErrors uint64 `json:"write_errors"`

	// Endpoints breaks the totals down, since endpoints are promoted
	// individually.
	Endpoints map[string]*EndpointStats `json:"endpoints"`
}

// EndpointStats are the per-endpoint totals.
type EndpointStats struct {
	Compared   uint64 `json:"compared"`
	Matched    uint64 `json:"matched"`
	Mismatched uint64 `json:"mismatched"`
	// FirstMismatch and LastMismatch bracket the disagreements, so an operator
	// can tell a fixed bug from an ongoing one.
	FirstMismatch *time.Time `json:"first_mismatch,omitempty"`
	LastMismatch  *time.Time `json:"last_mismatch,omitempty"`
}

// MatchRate returns the fraction of comparisons that agreed, or 1 if none have
// been made.
func (e *EndpointStats) MatchRate() float64 {
	if e == nil || e.Compared == 0 {
		return 1
	}
	return float64(e.Matched) / float64(e.Compared)
}

// MatchRate returns the overall agreement fraction.
func (s *Stats) MatchRate() float64 {
	if s.Compared == 0 {
		return 1
	}
	return float64(s.Matched) / float64(s.Compared)
}

func (s *Stats) observe(endpoint string, matched bool) {
	if s.Endpoints == nil {
		s.Endpoints = map[string]*EndpointStats{}
	}
	e := s.Endpoints[endpoint]
	if e == nil {
		e = &EndpointStats{}
		s.Endpoints[endpoint] = e
	}

	s.Compared++
	e.Compared++
	if matched {
		s.Matched++
		e.Matched++
		return
	}
	s.Mismatched++
	e.Mismatched++

	now := time.Now().UTC()
	if e.FirstMismatch == nil {
		first := now
		e.FirstMismatch = &first
	}
	last := now
	e.LastMismatch = &last
}

func (s *Stats) clone() *Stats {
	out := *s
	out.Endpoints = make(map[string]*EndpointStats, len(s.Endpoints))
	for k, v := range s.Endpoints {
		e := *v
		out.Endpoints[k] = &e
	}
	return &out
}

func loadStats(path string) (*Stats, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &Stats{Endpoints: map[string]*EndpointStats{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("difflog: read stats: %w", err)
	}

	var s Stats
	if err := json.Unmarshal(data, &s); err != nil {
		// A truncated stats file must not stop the worker from starting; the
		// diff log itself is the authoritative record.
		return &Stats{Endpoints: map[string]*EndpointStats{}}, nil
	}
	if s.Endpoints == nil {
		s.Endpoints = map[string]*EndpointStats{}
	}
	return &s, nil
}

// saveStats writes atomically, so a crash mid-write cannot leave a corrupt
// file that loses the whole run's counters.
func saveStats(path string, s *Stats) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(filepath.Dir(path), ".stats-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// CreateTemp makes the file 0600; match the log files so an operator can
	// read the stats from outside the container.
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
