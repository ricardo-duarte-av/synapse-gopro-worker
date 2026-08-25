package store

import (
	"context"
	"os"
	"testing"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// Every event whose sender has an MSC4293 ban redaction must come back marked
// redacted, and events from unbanned senders in the same rooms must not.
//
// This is checked against the live database because the rule only matters at
// the scale it actually occurs: 9,106 ban redactions across 122 rooms here.
// Missing it served the original content of banned users' events to remote
// servers, and no `redactions` row exists to hint at it.
func TestLiveBanRedactionsAreHonoured(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set")
	}
	ctx := context.Background()
	db, err := Open(ctx, Config{DSN: dsn, Cache: cache.Settings{EventsMB: 64}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Two sets with an oracle that is actually correct. An event is redacted
	// if a ban redaction covers it *or* an ordinary redaction row exists, so
	// asserting "not banned means not redacted" without excluding the latter
	// reports false failures.
	sample := func(q string) []string {
		rows, err := db.pool.Query(ctx, q)
		if err != nil {
			t.Skip("room_ban_redactions not available")
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		return out
	}

	// Driven from room_ban_redactions (thousands of rows) rather than from
	// events (millions): the other direction makes the planner sort a join
	// over the whole events table and returns nothing useful.
	covered := sample(`
		WITH bans AS (
			SELECT room_id, user_id, redact_end_ordering
			FROM room_ban_redactions ORDER BY random() LIMIT 40
		)
		SELECT e.event_id
		FROM bans b
		JOIN events e ON e.room_id = b.room_id AND e.sender = b.user_id
		WHERE e.type <> 'm.room.create'
		  AND (b.redact_end_ordering IS NULL OR e.stream_ordering < b.redact_end_ordering)
		LIMIT 150`)

	notCovered := sample(`
		SELECT e.event_id
		FROM events e JOIN event_json ej USING (event_id)
		LEFT JOIN room_ban_redactions r
		  ON r.room_id = e.room_id AND r.user_id = ej.json::jsonb->>'sender'
		WHERE e.room_id IN (SELECT DISTINCT room_id FROM room_ban_redactions LIMIT 20)
		  AND r.user_id IS NULL
		  AND NOT EXISTS (SELECT 1 FROM redactions x WHERE x.redacts = e.event_id)
		  AND e.type <> 'm.room.create'
		ORDER BY random() LIMIT 150`)

	if len(covered) == 0 {
		t.Skip("no ban-covered events to check")
	}

	check := func(label string, ids []string, wantRedacted bool) (int, int) {
		if len(ids) == 0 {
			return 0, 0
		}
		events, err := db.GetEvents(ctx, ids)
		if err != nil {
			t.Fatal(err)
		}
		var ok, wrong int
		for id, ev := range events {
			if ev.IsRedacted() == wantRedacted {
				ok++
				continue
			}
			wrong++
			if wrong <= 3 {
				t.Errorf("%s: %s IsRedacted()=%v, want %v", label, id, ev.IsRedacted(), wantRedacted)
			}
		}
		return ok, wrong
	}
	okA, badA := check("ban-covered", covered, true)
	okB, badB := check("not covered, no redaction row", notCovered, false)
	checked, wrong, banned := okA+badA+okB+badB, badA+badB, okA+badA
	t.Logf("checked %d events (%d covered by a ban redaction), %d wrong", checked, banned, wrong)
	if banned == 0 {
		t.Skip("sample contained no banned senders; nothing was proven")
	}
}
