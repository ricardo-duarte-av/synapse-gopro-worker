package matrixstate

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

func liveResolver(t *testing.T) (*Resolver, *store.Store) {
	t.Helper()
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set; skipping live database test")
	}
	s, err := store.Open(context.Background(), store.Config{DSN: dsn, MaxConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return NewResolver(s), s
}

// ourServer is the homeserver this database belongs to; it is in every room.
const ourServer = "aguiarvieira.pt"

// TestLiveStateIDsAgainstCurrentState checks the state-before-event logic
// across many real rooms.
//
// For a room with a single forward extremity, the state at that extremity is
// the room's current state. Our result must therefore equal
// current_state_events, adjusted for the extremity itself: /state_ids returns
// state *before* the event, so a state extremity contributes the event it
// replaced rather than itself.
func TestLiveStateIDsAgainstCurrentState(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	rows, err := s.Pool().Query(ctx, `
		SELECT f.room_id, f.event_id
		FROM event_forward_extremities AS f
		JOIN (
			SELECT room_id FROM event_forward_extremities
			GROUP BY room_id HAVING count(*) = 1
		) AS single USING (room_id)
		JOIN rooms USING (room_id)
		LIMIT 40`)
	if err != nil {
		t.Fatal(err)
	}
	type target struct{ roomID, eventID string }
	var targets []target
	for rows.Next() {
		var tg target
		if err := rows.Scan(&tg.roomID, &tg.eventID); err != nil {
			t.Fatal(err)
		}
		targets = append(targets, tg)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(targets) == 0 {
		t.Skip("no single-extremity rooms to check")
	}

	var checked, skipped int
	for _, tg := range targets {
		resp, err := r.StateIDs(ctx, ourServer, tg.roomID, tg.eventID)
		if err != nil {
			// 403 from a server ACL, or a partial-state room, is a legitimate
			// answer rather than a failure of the state logic.
			if me, ok := err.(*MatrixError); ok {
				skipped++
				t.Logf("skip %s: %s", tg.roomID, me.Message)
				continue
			}
			t.Errorf("%s: %v", tg.roomID, err)
			continue
		}

		want, err := currentStateBefore(ctx, s, tg.roomID, tg.eventID)
		if err != nil {
			t.Fatal(err)
		}

		got := append([]string(nil), resp.PDUIDs...)
		sort.Strings(got)
		sort.Strings(want)

		if len(got) != len(want) {
			t.Errorf("%s: got %d state ids, want %d", tg.roomID, len(got), len(want))
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%s: state id %d = %s, want %s", tg.roomID, i, got[i], want[i])
				break
			}
		}
		checked++
	}
	t.Logf("verified state_ids for %d rooms (%d skipped by ACL or partial state)", checked, skipped)
	if checked == 0 {
		t.Error("no rooms were actually verified")
	}
}

// currentStateBefore returns current_state_events for the room, adjusted to the
// state before the given event the same way /state_ids is.
func currentStateBefore(ctx context.Context, s *store.Store, roomID, eventID string) ([]string, error) {
	rows, err := s.Pool().Query(ctx,
		`SELECT type, state_key, event_id FROM current_state_events WHERE room_id = $1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	state := map[store.StateKey]string{}
	for rows.Next() {
		var k store.StateKey
		var id string
		if err := rows.Scan(&k.Type, &k.StateKey, &id); err != nil {
			return nil, err
		}
		state[k] = id
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	ev, err := s.GetEvent(ctx, eventID)
	if err != nil {
		return nil, err
	}
	if ev.IsStateEvent() {
		key := store.StateKey{Type: ev.Type, StateKey: *ev.StateKey}
		if prev := replacesState(ev.JSON); prev != "" {
			state[key] = prev
		} else {
			delete(state, key)
		}
	}

	out := make([]string, 0, len(state))
	for _, id := range state {
		out = append(out, id)
	}
	return out, nil
}

func TestLiveStateIDsAccessControl(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	var roomID, eventID string
	err := s.Pool().QueryRow(ctx, `
		SELECT f.room_id, f.event_id FROM event_forward_extremities AS f
		JOIN rooms USING (room_id)
		WHERE NOT EXISTS (
			SELECT 1 FROM current_state_events c
			WHERE c.room_id = f.room_id AND c.type = 'm.room.server_acl')
		LIMIT 1`).Scan(&roomID, &eventID)
	if err != nil {
		t.Skipf("no ACL-free room available: %v", err)
	}

	// A server that is not in the room must be refused, and must be told
	// nothing more than that.
	_, err = r.StateIDs(ctx, "not-in-this-room.invalid", roomID, eventID)
	me, ok := err.(*MatrixError)
	if !ok {
		t.Fatalf("err = %v, want a MatrixError", err)
	}
	if me.Status != 403 || me.ErrCode != "M_FORBIDDEN" {
		t.Errorf("got %d %s, want 403 M_FORBIDDEN", me.Status, me.ErrCode)
	}
	if me.Message != "Host not in room." {
		t.Errorf("message = %q, want Synapse's exact wording", me.Message)
	}
}

func TestLiveStateIDsUnknownEvent(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	var roomID string
	if err := s.Pool().QueryRow(ctx,
		`SELECT room_id FROM event_forward_extremities LIMIT 1`).Scan(&roomID); err != nil {
		t.Fatal(err)
	}

	_, err := r.StateIDs(ctx, ourServer, roomID, "$this-event-does-not-exist")
	me, ok := err.(*MatrixError)
	if !ok {
		t.Fatalf("err = %v, want a MatrixError", err)
	}
	if me.Status != 404 {
		t.Errorf("status = %d, want 404", me.Status)
	}
}

// TestLiveStateIDsOutlier covers the case that motivated reading the outlier
// flag from the events table rather than internal_metadata: an outlier has no
// state group, and must produce a clean 404 rather than an internal error.
func TestLiveStateIDsOutlier(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	var roomID, eventID string
	err := s.Pool().QueryRow(ctx, `
		SELECT room_id, event_id FROM events
		WHERE outlier = true AND room_id IN (SELECT room_id FROM rooms)
		LIMIT 1`).Scan(&roomID, &eventID)
	if err != nil {
		t.Skipf("no outlier available: %v", err)
	}

	start := time.Now()
	_, err = r.StateIDs(ctx, ourServer, roomID, eventID)
	me, ok := err.(*MatrixError)
	if !ok {
		t.Fatalf("outlier %s in %s: err = %v, want a MatrixError", eventID, roomID, err)
	}
	if me.Status != 404 && me.Status != 403 {
		t.Errorf("status = %d, want 404 (or 403 if we are not in the room)", me.Status)
	}
	t.Logf("outlier handled in %s: %d %s", time.Since(start), me.Status, me.Message)
}
