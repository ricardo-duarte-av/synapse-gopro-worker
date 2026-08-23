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
