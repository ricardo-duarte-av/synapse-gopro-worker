package matrixstate

import (
	"encoding/json"
	"testing"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// A redacted event must keep unsigned.replaces_state and unsigned.age_ts.
// mautrix's Redact drops unsigned entirely, but Synapse's prune_event_dict
// copies these two onto the pruned event -- so serving "unsigned": {} here
// mismatched Synapse on every redacted state event.
func TestRedactionKeepsAgeTSAndReplacesState(t *testing.T) {
	stored := []byte(`{
		"type": "m.room.member",
		"room_id": "!r:example.org",
		"sender": "@u:example.org",
		"state_key": "@u:example.org",
		"depth": 14879,
		"origin_server_ts": 1787177801339,
		"content": {"membership": "join", "displayname": "Frank", "avatar_url": "mxc://x/y"},
		"unsigned": {
			"age_ts": 1787177801339,
			"replaces_state": "$replaced",
			"age": 12,
			"something_else": "dropped"
		}
	}`)

	got, err := redactEvent(&store.Event{JSON: stored, RoomVersion: "10"})
	if err != nil {
		t.Fatal(err)
	}

	var ev struct {
		Content  map[string]any             `json:"content"`
		Unsigned map[string]json.RawMessage `json:"unsigned"`
	}
	if err := json.Unmarshal(got, &ev); err != nil {
		t.Fatal(err)
	}

	// Redaction still has to do its job.
	if _, ok := ev.Content["displayname"]; ok {
		t.Error("displayname survived redaction")
	}
	if ev.Content["membership"] != "join" {
		t.Errorf("membership = %v, want join", ev.Content["membership"])
	}

	if string(ev.Unsigned["replaces_state"]) != `"$replaced"` {
		t.Errorf("replaces_state = %s, want it preserved", ev.Unsigned["replaces_state"])
	}
	if string(ev.Unsigned["age_ts"]) != "1787177801339" {
		t.Errorf("age_ts = %s, want it preserved", ev.Unsigned["age_ts"])
	}
	for _, dropped := range []string{"age", "something_else"} {
		if _, ok := ev.Unsigned[dropped]; ok {
			t.Errorf("unsigned.%s survived redaction, but prune_event_dict drops it", dropped)
		}
	}
}
