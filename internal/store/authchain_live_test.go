package store

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

// referenceAuthChain computes the auth chain with a single recursive CTE, as
// an oracle independent of the Go walk under test.
//
// It shares no code with GetAuthChainIDsRecursive, which is the point. The
// working notes record a live test that compared the cached paths against each
// other and so passed even when all of them were wrong the same way; an oracle
// that reimplements the question in SQL cannot fail like that.
//
// The seed is inlined into the statement rather than passed as a parameter,
// and that is not laziness. Passed as $1, PostgreSQL plans the recursive term
// against a generic plan with no cardinality information for the array, picks a
// join order that does not terminate in reasonable time, and the same query
// that runs in ~1.5s takes over a minute. Inlining gives the planner the real
// row count. Test-only, and every id comes from the database.
func referenceAuthChain(ctx context.Context, s *Store, eventIDs []string) ([]string, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	lits := make([]string, 0, len(eventIDs))
	for _, id := range eventIDs {
		lits = append(lits, quoteLiteral(id))
	}
	q := `
		WITH RECURSIVE chain(event_id) AS (
				SELECT a.auth_id
				FROM event_auth AS a
				INNER JOIN events AS e ON e.event_id = a.auth_id
				WHERE a.event_id = ANY(ARRAY[` + strings.Join(lits, ",") + `]::text[])
			UNION
				SELECT a.auth_id
				FROM chain AS c
				INNER JOIN event_auth AS a ON a.event_id = c.event_id
				INNER JOIN events AS e ON e.event_id = a.auth_id
		)
		SELECT event_id FROM chain`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// quoteLiteral renders a SQL string literal, doubling embedded quotes.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// roomsNeedingFallback finds rooms whose current state is not fully covered by
// the chain cover index, which is what drives a request onto the recursive
// walk. Targets are discovered rather than named: the rooms with cover gaps on
// this deployment are historical accidents and any one of them may be purged.
func roomsNeedingFallback(ctx context.Context, t *testing.T, s *Store, limit int) []string {
	t.Helper()
	rows, err := s.pool.Query(ctx, `
		SELECT c.room_id, count(*) AS uncovered
		FROM current_state_events AS c
		LEFT JOIN event_auth_chains AS ac ON ac.event_id = c.event_id
		WHERE ac.event_id IS NULL
		GROUP BY c.room_id
		ORDER BY count(*) DESC
		LIMIT $1`, limit)
	if err != nil {
		t.Fatalf("find rooms needing fallback: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var roomID string
		var uncovered int
		if err := rows.Scan(&roomID, &uncovered); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Logf("  %s: %d current-state events without chain rows", roomID, uncovered)
		out = append(out, roomID)
	}
	return out
}

// maxOracleSeed bounds the seed handed to the SQL oracle; see the comment at
// its use for why the limit exists and why it does not weaken the test.
const maxOracleSeed = 3000

func currentStateIDs(ctx context.Context, t *testing.T, s *Store, roomID string) []string {
	t.Helper()
	rows, err := s.pool.Query(ctx,
		`SELECT event_id FROM current_state_events WHERE room_id = $1`, roomID)
	if err != nil {
		t.Fatalf("current state: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id)
	}
	return out
}

// TestLiveRecursiveAuthChainMatchesReference checks the CTE against the walk it
// replaced, on the rooms that actually reach this code path.
func TestLiveRecursiveAuthChainMatchesReference(t *testing.T) {
	s := liveStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	rooms := roomsNeedingFallback(ctx, t, s, 5)
	if len(rooms) == 0 {
		t.Skip("no room on this deployment has a chain cover gap")
	}

	for _, roomID := range rooms {
		seed := currentStateIDs(ctx, t, s, roomID)
		if len(seed) == 0 {
			continue
		}
		// Bounded, and deterministically so. The SQL oracle degrades badly
		// once the seed array grows past a few thousand literals -- the
		// planner stops hash-joining it and the query outruns the role's
		// statement_timeout, while the Go walk finishes the same room in
		// under a second. Capping keeps the oracle usable without weakening
		// the comparison: both sides are handed the identical seed, and the
		// walk that follows is the full auth DAG either way.
		sort.Strings(seed)
		if len(seed) > maxOracleSeed {
			seed = seed[:maxOracleSeed]
		}

		refStart := time.Now()
		want, err := referenceAuthChain(ctx, s, seed)
		refTook := time.Since(refStart)
		if err != nil {
			t.Fatalf("%s: reference CTE: %v", roomID, err)
		}

		gotStart := time.Now()
		got, err := s.GetAuthChainIDsRecursive(ctx, seed)
		gotTook := time.Since(gotStart)
		if err != nil {
			t.Fatalf("%s: recursive auth chain: %v", roomID, err)
		}

		sort.Strings(want)
		sort.Strings(got)
		if len(got) != len(want) {
			t.Errorf("%s: got %d events, reference has %d", roomID, len(got), len(want))
		}
		for i := range want {
			if i >= len(got) || got[i] != want[i] {
				t.Errorf("%s: first difference at %d: got %q want %q", roomID, i, got[i], want[i])
				break
			}
		}

		t.Logf("%s: %d seed -> %d events; CTE %s, walk %s",
			roomID, len(seed), len(want), refTook.Round(time.Millisecond),
			gotTook.Round(time.Millisecond))
	}
}

// TestLiveRecursiveAuthChainExcludesUnreachableSeeds pins the base case.
//
// Seeding the CTE with the input events instead of their auth events would
// return every input unconditionally. That is a different answer, and it is
// silent: the result is a superset, so nothing errors and the auth chain simply
// contains events that do not belong to it.
func TestLiveRecursiveAuthChainExcludesUnreachableSeeds(t *testing.T) {
	s := liveStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// A room's create event auth-reaches nothing, so it can never appear in
	// its own auth chain.
	var createID, roomID string
	err := s.pool.QueryRow(ctx, `
		SELECT event_id, room_id FROM events
		WHERE type = 'm.room.create' AND state_key = ''
		ORDER BY stream_ordering DESC LIMIT 1`).Scan(&createID, &roomID)
	if err != nil {
		t.Fatalf("find a create event: %v", err)
	}

	got, err := s.GetAuthChainIDsRecursive(ctx, []string{createID})
	if err != nil {
		t.Fatalf("recursive auth chain: %v", err)
	}
	for _, id := range got {
		if id == createID {
			t.Fatalf("create event %s appeared in its own auth chain", createID)
		}
	}
	t.Logf("create %s in %s: auth chain has %d events, none of them itself",
		createID, roomID, len(got))
}
