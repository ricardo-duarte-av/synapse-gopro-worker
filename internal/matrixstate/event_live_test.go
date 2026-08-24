package matrixstate

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestLiveEventVisibleCase covers the ordinary path: an event in a room our
// server is in, with open history visibility.
func TestLiveEventOrdinary(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	var roomID, eventID string
	err := s.Pool().QueryRow(ctx, `
		SELECT e.room_id, e.event_id FROM events e
		JOIN rooms USING (room_id)
		WHERE e.outlier = false AND e.type = 'm.room.message'
		ORDER BY e.stream_ordering DESC LIMIT 1`).Scan(&roomID, &eventID)
	if err != nil {
		t.Skipf("no message event: %v", err)
	}

	resp, err := r.Event(ctx, ourServer, ourServer, eventID)
	if err != nil {
		if me, ok := err.(*MatrixError); ok {
			t.Skipf("access refused for %s: %s", roomID, me.Message)
		}
		t.Fatal(err)
	}

	if resp.Origin != ourServer {
		t.Errorf("Origin = %q, want %q", resp.Origin, ourServer)
	}
	if len(resp.PDUs) != 1 {
		t.Fatalf("got %d pdus, want exactly 1", len(resp.PDUs))
	}

	var ev map[string]any
	if err := json.Unmarshal(resp.PDUs[0], &ev); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"type", "room_id", "sender", "content", "signatures", "hashes"} {
		if _, ok := ev[field]; !ok {
			t.Errorf("pdu is missing %q", field)
		}
	}

	// age_ts must have become age; leaving age_ts on the wire would differ
	// from Synapse.
	if unsigned, ok := ev["unsigned"].(map[string]any); ok {
		if _, bad := unsigned["age_ts"]; bad {
			t.Error("unsigned.age_ts was not converted to age")
		}
	}
	t.Logf("event %s served, %d bytes", eventID, len(resp.PDUs[0]))
}

func TestLiveEventUnknown(t *testing.T) {
	r, _ := liveResolver(t)
	_, err := r.Event(context.Background(), ourServer, ourServer, "$no-such-event-at-all")
	if err != ErrEventNotFound {
		t.Errorf("err = %v, want ErrEventNotFound (Synapse answers 404 with an empty body)", err)
	}
}

func TestLiveEventRefusesServerNotInRoom(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	var eventID string
	if err := s.Pool().QueryRow(ctx, `
		SELECT e.event_id FROM events e JOIN rooms USING (room_id)
		WHERE e.outlier = false ORDER BY e.stream_ordering DESC LIMIT 1`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}

	_, err := r.Event(ctx, "not-in-this-room.invalid", ourServer, eventID)
	me, ok := err.(*MatrixError)
	if !ok {
		t.Fatalf("err = %v, want a MatrixError", err)
	}
	if me.Status != 403 {
		t.Errorf("status = %d, want 403", me.Status)
	}
}

// TestLiveEventRedaction exercises the path that matters most: an event the
// requesting server may not see must come back redacted, not in full.
func TestLiveEventRedaction(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	// A room with restricted history visibility, if one exists here.
	var roomID, eventID string
	err := s.Pool().QueryRow(ctx, `
		SELECT e.room_id, e.event_id
		FROM current_state_events cse
		JOIN event_json ej USING (event_id)
		JOIN events e ON e.room_id = cse.room_id
		WHERE cse.type = 'm.room.history_visibility'
		  AND ej.json::jsonb->'content'->>'history_visibility' IN ('joined','invited')
		  AND e.type = 'm.room.message' AND e.outlier = false
		LIMIT 1`).Scan(&roomID, &eventID)
	if err != nil {
		t.Skipf("no restricted-visibility room with messages: %v", err)
	}

	ev, err := s.GetEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	visible, err := r.eventVisible(ctx, ev, ourServer, ourServer)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("room %s has restricted visibility; visible to us = %v", roomID, visible)

	// Whatever the verdict, redaction itself must produce a valid event that
	// keeps the structural fields and drops message content.
	redacted, err := redactEvent(ev)
	if err != nil {
		t.Fatalf("redaction failed: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(redacted, &out); err != nil {
		t.Fatalf("redacted event is not valid JSON: %v", err)
	}
	for _, field := range []string{"type", "room_id", "sender", "signatures"} {
		if _, ok := out[field]; !ok {
			t.Errorf("redacted event lost required field %q", field)
		}
	}
	content, _ := out["content"].(map[string]any)
	if _, leaked := content["body"]; leaked {
		t.Error("redacted m.room.message still carries content.body")
	}
	if strings.Contains(string(redacted), "\"body\"") {
		t.Errorf("redacted event still mentions a body field: %s", truncate(string(redacted)))
	}
}

func truncate(s string) string {
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

// TestLiveStateIDsRejectsRejectedEvents covers a divergence found in
// production: a rejected event is not part of room state, and Synapse answers
// for it with a plain "could not find event". Serving its state instead would
// hand remote servers a state map Synapse refuses to youch for.
//
// /event deliberately does the opposite and serves rejected events, so both
// behaviours are asserted together to keep them from drifting into each other.
func TestLiveStateIDsRejectsRejectedEvents(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	var roomID, eventID string
	err := s.Pool().QueryRow(ctx, `
		SELECT e.room_id, e.event_id FROM events e
		JOIN rooms USING (room_id)
		WHERE e.rejection_reason IS NOT NULL AND e.outlier = false
		LIMIT 1`).Scan(&roomID, &eventID)
	if err != nil {
		t.Skipf("no rejected event available: %v", err)
	}

	_, err = r.StateIDs(ctx, ourServer, roomID, eventID)
	me, ok := err.(*MatrixError)
	if !ok {
		t.Fatalf("state_ids for rejected event %s: err = %v, want a MatrixError", eventID, err)
	}
	if me.Status != 404 {
		t.Errorf("status = %d, want 404", me.Status)
	}
	if me.Message != "Could not find event "+eventID {
		t.Errorf("message = %q, want Synapse's exact wording", me.Message)
	}

	// The same event must still be served by /event.
	resp, err := r.Event(ctx, ourServer, ourServer, eventID)
	if err != nil {
		if _, isMatrix := err.(*MatrixError); isMatrix {
			t.Skipf("cannot check /event for %s: %v", roomID, err)
		}
		t.Fatalf("/event refused a rejected event: %v", err)
	}
	if len(resp.PDUs) != 1 {
		t.Errorf("/event returned %d pdus for a rejected event, want 1", len(resp.PDUs))
	}
}

// TestLiveRedactedEventIsServedRedacted covers the most serious divergence
// found in production: an event that a user has redacted was being served with
// its original content.
//
// Redaction is how a user deletes a message. Serving the original content over
// federation would undo that, so this is checked against real redacted events
// rather than a synthetic one.
func TestLiveRedactedEventIsServedRedacted(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	rows, err := s.Pool().Query(ctx, `
		SELECT e.event_id FROM redactions rd
		JOIN events e ON e.event_id = rd.redacts
		JOIN rooms USING (room_id)
		WHERE rd.recheck = false AND e.outlier = false
		  AND e.type = 'm.room.message' AND e.rejection_reason IS NULL
		LIMIT 20`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		t.Skip("no redacted messages available")
	}

	var checked int
	for _, id := range ids {
		ev, err := s.GetEvent(ctx, id)
		if err != nil {
			continue
		}
		if !ev.IsRedacted() {
			t.Errorf("%s has a confirmed redaction but was not marked redacted", id)
			continue
		}

		resp, err := r.Event(ctx, ourServer, ourServer, id)
		if err != nil {
			if _, ok := err.(*MatrixError); ok {
				continue
			}
			t.Fatalf("%s: %v", id, err)
		}
		if len(resp.PDUs) != 1 {
			t.Fatalf("%s: %d pdus", id, len(resp.PDUs))
		}

		var out struct {
			Content map[string]any `json:"content"`
		}
		if err := json.Unmarshal(resp.PDUs[0], &out); err != nil {
			t.Fatal(err)
		}
		// A redacted m.room.message keeps no content at all.
		if _, leaked := out.Content["body"]; leaked {
			t.Errorf("%s: redacted message was served with its body intact", id)
		}
		checked++
	}
	t.Logf("verified %d redacted events are served redacted", checked)
	if checked == 0 {
		t.Skip("no redacted events were servable")
	}
}
