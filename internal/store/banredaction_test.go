package store

import "testing"

// MSC4293: a ban with redact_events redacts everything the user sent in the
// room. When the ban is later lifted, Synapse records the stream position
// where that stops, and events at or after it are served normally again.
func TestBanAppliesTo(t *testing.T) {
	for _, tc := range []struct {
		name string
		br   banRedaction
		ev   Event
		want bool
	}{
		{"open-ended ban covers everything",
			banRedaction{RedactingEventID: "$ban"}, Event{StreamOrdering: 500}, true},
		{"before the end ordering",
			banRedaction{RedactingEventID: "$ban", EndOrdering: 1000}, Event{StreamOrdering: 999}, true},
		{"at the end ordering is not covered",
			banRedaction{RedactingEventID: "$ban", EndOrdering: 1000}, Event{StreamOrdering: 1000}, false},
		{"after the end ordering is not covered",
			banRedaction{RedactingEventID: "$ban", EndOrdering: 1000}, Event{StreamOrdering: 1001}, false},
		// Backfilled events get a negative stream ordering, so they always
		// fall before the end point. Synapse notes this is deliberate.
		{"backfilled events are always covered",
			banRedaction{RedactingEventID: "$ban", EndOrdering: 1000}, Event{StreamOrdering: -42}, true},
		// Synapse ignores redactions of m.room.create, and a ban is no
		// different: redacting it would break the room.
		{"m.room.create is never redacted",
			banRedaction{RedactingEventID: "$ban"}, Event{Type: "m.room.create", StreamOrdering: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := banAppliesTo(tc.br, &tc.ev); got != tc.want {
				t.Errorf("banAppliesTo = %v, want %v", got, tc.want)
			}
		})
	}
}
