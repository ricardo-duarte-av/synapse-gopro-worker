package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// TestLiveCachedEventsAreNotMutated guards the interaction between caching and
// redaction.
//
// Redaction is resolved on every load, but the event itself is cached. Marking
// a cached event as redacted in place would both race with other in-flight
// requests and stamp redaction state onto the shared copy, so the marking must
// produce a new value instead.
func TestLiveCachedEventsAreNotMutated(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 4, Cache: cache.DefaultSettings()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A redacted event and a plain one, loaded together and repeatedly.
	var redacted, plain string
	if err := s.pool.QueryRow(ctx, `
		SELECT rd.redacts FROM redactions rd
		JOIN events e ON e.event_id = rd.redacts
		WHERE rd.recheck = false AND e.outlier = false LIMIT 1`).Scan(&redacted); err != nil {
		t.Skipf("no redacted event: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT event_id FROM events
		WHERE outlier = false AND event_id NOT IN (SELECT redacts FROM redactions WHERE redacts IS NOT NULL)
		ORDER BY stream_ordering DESC LIMIT 1`).Scan(&plain); err != nil {
		t.Skipf("no unredacted event: %v", err)
	}

	for i := range 5 {
		got, err := s.GetEvents(ctx, []string{redacted, plain})
		if err != nil {
			t.Fatal(err)
		}
		if r := got[redacted]; r == nil || !r.IsRedacted() {
			t.Errorf("pass %d: redacted event not marked", i)
		}
		// The crucial assertion: repeated loads must not leak redaction state
		// onto an event that was never redacted.
		if p := got[plain]; p == nil || p.IsRedacted() {
			t.Errorf("pass %d: unredacted event was marked redacted", i)
		}
	}
}

// TestLiveCacheIsConcurrencySafe exercises the cached paths under -race.
func TestLiveCacheIsConcurrencySafe(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 8, Cache: cache.DefaultSettings()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var roomID, eventID string
	if err := s.pool.QueryRow(ctx, `
		SELECT room_id, event_id FROM event_forward_extremities LIMIT 1`).Scan(&roomID, &eventID); err != nil {
		t.Skip(err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				if _, err := s.GetEvent(ctx, eventID); err != nil && err != ErrNotFound {
					t.Error(err)
					return
				}
				g, err := s.GetStateGroupForEvent(ctx, eventID)
				if err != nil {
					continue
				}
				if _, err := s.GetStateForGroup(ctx, g); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, st := range s.CacheStats() {
		t.Logf("%-20s entries=%-6d bytes=%-10d hits=%-5d misses=%-4d hit_rate=%.1f%%",
			st.Name, st.Entries, st.Bytes, st.Hits, st.Misses, st.HitRate()*100)
	}
}

// TestLiveCacheSpeedsUpStateResolution measures the effect the cache is for.
func TestLiveCacheSpeedsUpStateResolution(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 4, Cache: cache.DefaultSettings()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// The largest room, where resolution is most expensive.
	var roomID string
	if err := s.pool.QueryRow(ctx, `
		SELECT room_id FROM current_state_events
		GROUP BY room_id ORDER BY count(*) DESC LIMIT 1`).Scan(&roomID); err != nil {
		t.Fatal(err)
	}
	var eventID string
	if err := s.pool.QueryRow(ctx,
		`SELECT event_id FROM events WHERE room_id = $1 ORDER BY stream_ordering DESC LIMIT 1`,
		roomID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	group, err := s.GetStateGroupForEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	state, err := s.GetStateForGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	cold := time.Since(start)

	start = time.Now()
	if _, err := s.GetStateForGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	warm := time.Since(start)

	t.Logf("state group %d (%d events): cold %s, warm %s (%.0fx)",
		group, len(state), cold.Round(time.Microsecond), warm.Round(time.Microsecond),
		float64(cold)/float64(max(warm, 1)))

	if warm > cold {
		t.Errorf("a cached lookup (%s) was slower than the uncached one (%s)", warm, cold)
	}
}
