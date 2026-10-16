package store

import (
	"context"
	"os"
	"testing"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// TestLiveCacheTrace checks each traced lookup against the real database: a
// cold lookup must record a miss and the same lookup again a hit. A call site
// that recorded the wrong outcome -- or none -- would make the request log
// claim answers came from memory when they did not.
func TestLiveCacheTrace(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set; skipping live database test")
	}
	ctx := context.Background()
	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 4, Cache: cache.DefaultSettings()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var roomID, eventID string
	if err := s.Pool().QueryRow(ctx, `
		SELECT e.room_id, e.event_id FROM event_forward_extremities f
		JOIN events e USING (event_id)
		JOIN event_auth_chains USING (event_id)
		WHERE NOT e.outlier
		ORDER BY e.stream_ordering DESC LIMIT 1`).Scan(&roomID, &eventID); err != nil {
		t.Fatal(err)
	}

	// Each step runs twice on its own trace: cold, then warm.
	steps := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{"events", func(ctx context.Context) error { _, err := s.GetEvent(ctx, eventID); return err }},
		{"event_state_groups", func(ctx context.Context) error { _, err := s.GetStateGroupForEvent(ctx, eventID); return err }},
		{"state_groups", func(ctx context.Context) error {
			g, err := s.GetStateGroupForEvent(context.Background(), eventID)
			if err != nil {
				return err
			}
			_, err = s.GetStateForGroup(ctx, g)
			return err
		}},
		{"auth_chains", func(ctx context.Context) error {
			_, err := s.GetAuthChainIDs(ctx, roomID, []string{eventID})
			return err
		}},
	}
	for _, step := range steps {
		for i, want := range []string{"miss", "hit"} {
			tctx, tr := cache.WithTrace(ctx)
			if err := step.run(tctx); err != nil {
				t.Fatalf("%s: %v", step.name, err)
			}
			if got := tr.Summary(); got != want {
				t.Errorf("%s pass %d: %q (%s), want %q", step.name, i+1, got, tr.Detail(), want)
			}
		}
	}
}
