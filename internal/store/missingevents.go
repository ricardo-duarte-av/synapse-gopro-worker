package store

import (
	"context"
	"fmt"
	"sort"
)

// MaxMissingEvents caps how many events one /get_missing_events answer may
// contain, mirroring Synapse's `limit = min(limit, 20)`.
const MaxMissingEvents = 20

// GetMissingEvents walks backwards through the room's DAG from latest_events,
// stopping at earliest_events or once limit events have been collected.
//
// A faithful port of Synapse's _get_missing_events. Three details in that
// function are load-bearing and easy to lose:
//
//   - latest_events is pre-filtered to events we hold *in this room*, so an
//     event in the wrong room is treated exactly like an unknown one rather
//     than as an error.
//   - the per-step limit shrinks as results accumulate, so the walk stops
//     mid-frontier rather than overshooting.
//   - the result is reversed at the end, because it was built backwards and
//     the caller wants approximate chronological order.
//
// The `NOT is_state` predicate is kept even though every row in this
// deployment has is_state false: the column is legacy, but dropping the
// predicate would change behaviour on a database where it is not.
func (s *Store) GetMissingEvents(ctx context.Context, roomID string, earliest, latest []string, limit int) ([]string, error) {
	if limit > MaxMissingEvents {
		limit = MaxMissingEvents
	}
	if limit <= 0 {
		return nil, nil
	}

	seen := make(map[string]bool, len(earliest))
	for _, id := range earliest {
		seen[id] = true
	}

	// The caller should not be asking about events it says it already has, and
	// Synapse drops them before the query rather than after.
	var wanted []string
	for _, id := range latest {
		if !seen[id] {
			wanted = append(wanted, id)
		}
	}
	if len(wanted) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx,
		`SELECT event_id FROM events WHERE event_id = ANY($1) AND room_id = $2`,
		wanted, roomID)
	if err != nil {
		return nil, fmt.Errorf("store: missing events start: %w", err)
	}
	var front []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: scan start event: %w", err)
		}
		front = append(front, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: missing events start rows: %w", err)
	}

	var results []string
	for len(front) > 0 && len(results) < limit {
		// Sorted so our own walk is reproducible.
		//
		// Synapse iterates a Python set here, so its frontier order is
		// arbitrary and its answer can differ between identical calls whenever
		// the walk truncates at the limit. We cannot make Synapse
		// deterministic, but we can make ourselves so: a remote server that
		// retries gets the same answer twice, and a disagreement is a stable
		// fact rather than a coin flip.
		sort.Strings(front)

		var next []string
		for _, eventID := range front {
			if len(results) >= limit {
				break
			}
			prev, err := s.prevEvents(ctx, eventID, roomID, limit-len(results))
			if err != nil {
				return nil, err
			}
			for _, p := range prev {
				if seen[p] {
					continue
				}
				seen[p] = true
				next = append(next, p)
				results = append(results, p)
			}
		}
		front = next
	}

	// Built backwards; the caller wants approximate chronological order.
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}
	return results, nil
}

// prevEvents returns up to limit prev-events of eventID that are in roomID.
func (s *Store) prevEvents(ctx context.Context, eventID, roomID string, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ee.prev_event_id
		FROM event_edges AS ee
		JOIN events ON events.event_id = ee.prev_event_id
		WHERE ee.event_id = $1
		  AND events.room_id = $2
		  AND NOT ee.is_state
		ORDER BY ee.prev_event_id
		LIMIT $3`, eventID, roomID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: prev events: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan prev event: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
