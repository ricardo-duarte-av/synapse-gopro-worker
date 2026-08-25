package matrixstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestLiveEventSweep runs the native /event path over a broad sample of real
// events before production traffic arrives.
//
// /event is the endpoint where a mistake serves private history, and it has not
// yet been exercised by live requests. Sweeping a varied sample now surfaces
// systematic problems -- an event type that fails to parse, a room version
// whose redaction rules misbehave, a state lookup that errors -- rather than
// discovering them from mismatch logs during the morning traffic peak.
func TestLiveEventSweep(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	// Spread the sample across event types, room versions and visibility
	// settings rather than taking the most recent N, which would all come from
	// one busy room.
	rows, err := s.Pool().Query(ctx, `
		SELECT DISTINCT ON (e.type, r.room_version) e.event_id, e.type, r.room_version
		FROM events e
		JOIN rooms r USING (room_id)
		WHERE e.outlier = false
		ORDER BY e.type, r.room_version, e.stream_ordering DESC
		LIMIT 300`)
	if err != nil {
		t.Fatal(err)
	}
	type sample struct{ eventID, evType, roomVersion string }
	var samples []sample
	for rows.Next() {
		var sm sample
		if err := rows.Scan(&sm.eventID, &sm.evType, &sm.roomVersion); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, sm)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(samples) == 0 {
		t.Skip("no events to sweep")
	}

	var served, refused, notFound int
	failures := map[string]int{}
	versions := map[string]int{}
	start := time.Now()

	for _, sm := range samples {
		resp, err := r.Event(ctx, ourServer, ourServer, sm.eventID)
		switch {
		case err == ErrEventNotFound:
			notFound++
			continue
		case err != nil:
			if _, ok := err.(*MatrixError); ok {
				// 403 for a room we are not in is a legitimate answer.
				refused++
				continue
			}
			// Anything else is a real failure.
			failures[sm.evType+"/"+sm.roomVersion]++
			if len(failures) <= 5 {
				t.Errorf("%s (type %s, room version %s): %v",
					sm.eventID, sm.evType, sm.roomVersion, err)
			}
			continue
		}

		// A served response must be structurally sound.
		if len(resp.PDUs) != 1 {
			t.Errorf("%s: %d pdus, want 1", sm.eventID, len(resp.PDUs))
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal(resp.PDUs[0], &ev); err != nil {
			t.Errorf("%s: pdu is not valid JSON: %v", sm.eventID, err)
			continue
		}
		if unsigned, ok := ev["unsigned"].(map[string]any); ok {
			if _, bad := unsigned["age_ts"]; bad {
				t.Errorf("%s: unsigned.age_ts was not converted to age", sm.eventID)
			}
		}
		served++
		versions[sm.roomVersion]++
	}

	elapsed := time.Since(start)
	t.Logf("swept %d events in %s (%.1fms each): %d served, %d refused, %d not found, %d failed",
		len(samples), elapsed.Round(time.Millisecond),
		float64(elapsed.Microseconds())/float64(len(samples))/1000,
		served, refused, notFound, len(failures))
	t.Logf("room versions served: %v", versions)

	if served == 0 {
		t.Error("no events were served at all")
	}
	if len(failures) > 0 {
		t.Errorf("internal failures by type/version: %v", failures)
	}
}

// TestLiveRedactionSweep checks redaction across room versions, since the
// preserved-key rules differ between them.
func TestLiveRedactionSweep(t *testing.T) {
	_, s := liveResolver(t)
	ctx := context.Background()

	rows, err := s.Pool().Query(ctx, `
		SELECT event_id, type, room_version FROM (
			SELECT DISTINCT ON (r.room_version, e.type)
			       e.event_id, e.type, r.room_version, e.stream_ordering
			FROM events e JOIN rooms r USING (room_id)
			WHERE e.outlier = false
			ORDER BY r.room_version, e.type, e.stream_ordering DESC
		) x
		-- Order by length then value, which sorts numeric room versions
		-- correctly. Sorting room_version as plain text puts "10" before "3", so
		-- a LIMIT never reaches the middle versions and the sweep skips them.
		ORDER BY length(room_version), room_version, type
		LIMIT 400`)
	if err != nil {
		t.Fatal(err)
	}
	type sample struct{ eventID, evType, roomVersion string }
	var samples []sample
	for rows.Next() {
		var sm sample
		if err := rows.Scan(&sm.eventID, &sm.evType, &sm.roomVersion); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, sm)
	}
	rows.Close()

	byVersion := map[string]int{}
	var failed int
	for _, sm := range samples {
		ev, err := s.GetEvent(ctx, sm.eventID)
		if err != nil {
			continue
		}
		redacted, err := redactEvent(ev)
		if err != nil {
			failed++
			if failed <= 5 {
				t.Errorf("redaction failed for %s (type %s, version %s): %v",
					sm.eventID, sm.evType, sm.roomVersion, err)
			}
			continue
		}
		var out map[string]any
		if err := json.Unmarshal(redacted, &out); err != nil {
			t.Errorf("%s: redacted event is not valid JSON: %v", sm.eventID, err)
			continue
		}
		var stored map[string]any
		if err := json.Unmarshal(ev.JSON, &stored); err != nil {
			t.Errorf("%s: stored event is not valid JSON: %v", sm.eventID, err)
			continue
		}
		// Redaction must never destroy the fields that make an event
		// verifiable -- but only those the event actually had. In room version
		// 12 the room ID is derived from the create event's hash, so the
		// create event itself carries no room_id: measured here, 0 of the v12
		// m.room.create events have one and all 30,869 other v12 events do.
		// Requiring it unconditionally therefore fails on a v12 create event
		// that is perfectly well formed.
		for _, field := range []string{"type", "room_id", "sender", "signatures", "hashes"} {
			if _, had := stored[field]; !had {
				continue
			}
			if _, ok := out[field]; !ok {
				t.Errorf("%s (version %s): redaction dropped %q",
					sm.eventID, sm.roomVersion, field)
			}
		}
		byVersion[sm.roomVersion]++
	}
	t.Logf("redacted events by room version: %v (%d failures)", byVersion, failed)
}
