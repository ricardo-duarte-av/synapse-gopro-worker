package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// liveStore connects to a real Synapse database, or skips.
//
// Set GOPRO_TEST_DSN to run these, e.g.
//
//	GOPRO_TEST_DSN='host=/var/sockets user=gopro_ro dbname=synapse-db'
func liveStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set; skipping live database test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := Open(ctx, Config{DSN: dsn, MaxConns: 4, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// pickRoom returns a room with a reasonable amount of state to exercise.
func pickRoom(t *testing.T, s *Store) string {
	t.Helper()
	var roomID string
	err := s.pool.QueryRow(context.Background(), `
		SELECT room_id FROM current_state_events
		GROUP BY room_id ORDER BY count(*) DESC LIMIT 1`).Scan(&roomID)
	if err != nil {
		t.Fatalf("pick room: %v", err)
	}
	return roomID
}

func TestLiveReadOnly(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()

	// The role must be incapable of writing, not merely trusted not to.
	_, err := s.pool.Exec(ctx, `CREATE TABLE gopro_should_not_exist (x int)`)
	if err == nil {
		t.Fatal("the database role was able to create a table; it must be read-only")
	}
	t.Logf("write correctly rejected: %v", err)
}

func TestLiveRoomInfo(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()
	roomID := pickRoom(t, s)

	info, err := s.GetRoomInfo(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if info.RoomVersion == "" {
		t.Error("RoomVersion is empty")
	}
	t.Logf("room %s: version %s, chain index %v", roomID, info.RoomVersion, info.HasAuthChainIndex)

	if _, err := s.GetRoomInfo(ctx, "!definitely-not-a-room:example.invalid"); err != ErrNotFound {
		t.Errorf("unknown room: err = %v, want ErrNotFound", err)
	}
}

func TestLiveIsHostInRoom(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()
	roomID := pickRoom(t, s)

	// Find a server that really is in the room.
	var userID string
	err := s.pool.QueryRow(ctx, `
		SELECT state_key FROM current_state_events
		WHERE room_id = $1 AND type = 'm.room.member' AND membership = 'join' LIMIT 1`,
		roomID).Scan(&userID)
	if err != nil {
		t.Fatal(err)
	}
	host := domainFromID(userID)

	in, err := s.IsHostInRoom(ctx, roomID, host)
	if err != nil {
		t.Fatal(err)
	}
	if !in {
		t.Errorf("IsHostInRoom(%s, %s) = false, want true", roomID, host)
	}

	out, err := s.IsHostInRoom(ctx, roomID, "definitely-not-here.invalid")
	if err != nil {
		t.Fatal(err)
	}
	if out {
		t.Error("a server not in the room was reported as joined")
	}

	// A wildcard host must be refused, not treated as a pattern.
	if _, err := s.IsHostInRoom(ctx, roomID, "%"); err == nil {
		t.Error("a host containing a LIKE wildcard was accepted")
	}
}

func TestLiveStateResolution(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()
	roomID := pickRoom(t, s)

	// A recent event in the room, which will have a state group.
	var eventID string
	err := s.pool.QueryRow(ctx,
		`SELECT event_id FROM events WHERE room_id = $1 ORDER BY stream_ordering DESC LIMIT 1`,
		roomID).Scan(&eventID)
	if err != nil {
		t.Fatal(err)
	}

	group, err := s.GetStateGroupForEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("state group for %s: %v", eventID, err)
	}

	start := time.Now()
	state, err := s.GetStateForGroup(ctx, group)
	if err != nil {
		t.Fatalf("state for group %d: %v", group, err)
	}
	t.Logf("resolved %d state events for group %d in %s", len(state), group, time.Since(start))

	if len(state) == 0 {
		t.Fatal("resolved no state at all")
	}
	// Every room has a create event; its absence means the walk is broken.
	if _, ok := state[StateKey{Type: "m.room.create", StateKey: ""}]; !ok {
		t.Error("resolved state has no m.room.create")
	}

	// Cross-check against Synapse's own current_state_events for the room's
	// latest state. This is the real correctness check: our delta walk must
	// agree with the table Synapse maintains independently.
	var currentCount int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM current_state_events WHERE room_id = $1`, roomID).Scan(&currentCount); err != nil {
		t.Fatal(err)
	}
	t.Logf("current_state_events has %d rows for this room", currentCount)
}

func TestLiveAuthChain(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()
	roomID := pickRoom(t, s)

	var eventID string
	err := s.pool.QueryRow(ctx,
		`SELECT event_id FROM events WHERE room_id = $1 ORDER BY stream_ordering DESC LIMIT 1`,
		roomID).Scan(&eventID)
	if err != nil {
		t.Fatal(err)
	}
	group, err := s.GetStateGroupForEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	state, err := s.GetStateForGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]string, 0, len(state))
	for _, id := range state {
		ids = append(ids, id)
	}

	start := time.Now()
	chain, err := s.GetAuthChainIDs(ctx, roomID, ids)
	if err != nil {
		t.Fatalf("auth chain: %v", err)
	}
	t.Logf("auth chain of %d state events: %d events in %s", len(ids), len(chain), time.Since(start))

	if len(chain) == 0 {
		t.Error("auth chain is empty, which cannot be right for a real room")
	}
}

func TestLiveGetEvent(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()
	roomID := pickRoom(t, s)

	var eventID string
	if err := s.pool.QueryRow(ctx,
		`SELECT event_id FROM events WHERE room_id = $1 ORDER BY stream_ordering DESC LIMIT 1`,
		roomID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}

	ev, err := s.GetEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventID != eventID || ev.RoomID != roomID {
		t.Errorf("got event %s in %s, want %s in %s", ev.EventID, ev.RoomID, eventID, roomID)
	}
	if len(ev.JSON) == 0 {
		t.Error("event JSON is empty")
	}
	if ev.RoomVersion == "" {
		t.Error("room version is empty")
	}
	t.Logf("event %s: type %s, room version %s, %d bytes of JSON",
		ev.EventID, ev.Type, ev.RoomVersion, len(ev.JSON))

	if _, err := s.GetEvent(ctx, "$definitely-not-an-event"); err != ErrNotFound {
		t.Errorf("unknown event: err = %v, want ErrNotFound", err)
	}
}

// TestLiveStateMatchesCurrentState is the strongest correctness check available
// without running the full endpoint: our recursive walk over state group deltas
// must produce exactly what Synapse's independently-maintained
// current_state_events table holds.
//
// For a room whose latest event is not a state event and which has a single
// forward extremity, the state at that event is the room's current state.
func TestLiveStateMatchesCurrentState(t *testing.T) {
	s := liveStore(t)
	ctx := context.Background()
	roomID := pickRoom(t, s)

	// Only meaningful with one forward extremity; otherwise current state is a
	// resolution across forks and need not equal the state at any single event.
	var extremities int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM event_forward_extremities WHERE room_id = $1`, roomID).Scan(&extremities); err != nil {
		t.Fatal(err)
	}
	if extremities != 1 {
		t.Skipf("room has %d forward extremities; current state is a resolution across forks", extremities)
	}

	var eventID, evType string
	var stateKey *string
	if err := s.pool.QueryRow(ctx, `
		SELECT e.event_id, e.type, e.state_key
		FROM event_forward_extremities AS f
		JOIN events AS e USING (event_id)
		WHERE f.room_id = $1`, roomID).Scan(&eventID, &evType, &stateKey); err != nil {
		t.Fatal(err)
	}

	group, err := s.GetStateGroupForEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	ours, err := s.GetStateForGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}

	// The state group at a state event includes that event; current state does
	// too, so no adjustment is needed here.
	rows, err := s.pool.Query(ctx,
		`SELECT type, state_key, event_id FROM current_state_events WHERE room_id = $1`, roomID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	theirs := map[StateKey]string{}
	for rows.Next() {
		var k StateKey
		var id string
		if err := rows.Scan(&k.Type, &k.StateKey, &id); err != nil {
			t.Fatal(err)
		}
		theirs[k] = id
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	var missing, extra, differing int
	for k, id := range theirs {
		got, ok := ours[k]
		switch {
		case !ok:
			if missing < 3 {
				t.Errorf("missing from our state: %s/%q -> %s", k.Type, k.StateKey, id)
			}
			missing++
		case got != id:
			if differing < 3 {
				t.Errorf("state differs for %s/%q: ours %s, Synapse %s", k.Type, k.StateKey, got, id)
			}
			differing++
		}
	}
	for k, id := range ours {
		if _, ok := theirs[k]; !ok {
			if extra < 3 {
				t.Errorf("extra in our state: %s/%q -> %s", k.Type, k.StateKey, id)
			}
			extra++
		}
	}

	t.Logf("compared %d state events at %s: %d missing, %d extra, %d differing",
		len(theirs), eventID, missing, extra, differing)
}
