package store

import (
	"maps"
	"testing"
)

func TestFilterMembersForServerMatchesTheSQLPrefilter(t *testing.T) {
	// These cases mirror LIKE '%:server' exactly, including the malformed IDs
	// the SQL comment calls out. Real data cannot distinguish a suffix match
	// from a substring match here — no state key in the live database has a
	// mid-string ":server" — so the adversarial keys have to be synthetic.
	state := map[StateKey]string{
		{Type: "m.room.member", StateKey: "@alice:matrix.org"}:         "$1",
		{Type: "m.room.member", StateKey: "@bob:matrix.org"}:           "$2",
		{Type: "m.room.member", StateKey: "@carol:example.com"}:        "$3",
		{Type: "m.room.member", StateKey: "@dave:matrix.org.evil.com"}: "$4", // must NOT match
		{Type: "m.room.member", StateKey: "@eve:matrix.org:tail"}:      "$5", // must NOT match
		{Type: "m.room.member", StateKey: "@mal:b:matrix.org"}:         "$6", // matches; caller rejects
		{Type: "m.room.member", StateKey: "@fred:notmatrix.org"}:       "$7", // must NOT match
		{Type: "m.room.power_levels", StateKey: ""}:                    "$8", // wrong type
		{Type: "m.room.history_visibility", StateKey: ""}:              "$9",
	}

	got := filterMembersForServer(state, "matrix.org")
	want := map[StateKey]string{
		{Type: "m.room.member", StateKey: "@alice:matrix.org"}: "$1",
		{Type: "m.room.member", StateKey: "@bob:matrix.org"}:   "$2",
		{Type: "m.room.member", StateKey: "@mal:b:matrix.org"}: "$6",
	}
	if !maps.Equal(got, want) {
		t.Errorf("filterMembersForServer:\n got  %v\n want %v", got, want)
	}
}

func TestFilterByTypes(t *testing.T) {
	state := map[StateKey]string{
		{Type: "m.room.history_visibility", StateKey: ""}:   "$vis",
		{Type: "m.room.member", StateKey: "@a:example.org"}: "$mem",
		{Type: "m.room.create", StateKey: ""}:               "$create",
	}

	got := filterByTypes(state, []string{"m.room.history_visibility"})
	want := map[StateKey]string{{Type: "m.room.history_visibility", StateKey: ""}: "$vis"}
	if !maps.Equal(got, want) {
		t.Errorf("single type: got %v, want %v", got, want)
	}

	// An empty type list matches nothing, the same as SQL's type = ANY('{}').
	if got := filterByTypes(state, nil); len(got) != 0 {
		t.Errorf("empty type list returned %v, want nothing", got)
	}

	// The source map must be untouched.
	if len(state) != 3 {
		t.Errorf("filtering mutated the source map: %d entries left", len(state))
	}
}

func TestFiltersReturnIndependentMaps(t *testing.T) {
	state := map[StateKey]string{
		{Type: "m.room.member", StateKey: "@a:example.org"}: "$1",
	}
	got := filterByTypes(state, []string{"m.room.member"})
	for k := range got {
		delete(got, k)
	}
	if len(state) != 1 {
		t.Error("the filtered map aliased its source")
	}
}

// Different filters over the same state group must not share a cache entry.
// Getting this wrong returns one question's answer to another, and every
// cached path agrees with every other, so only an independent oracle catches it.
func TestFilteredStateKeysAreDistinct(t *testing.T) {
	seen := map[FilteredStateKey]string{}
	add := func(k FilteredStateKey, describes string) {
		if prev, ok := seen[k]; ok && prev != describes {
			t.Errorf("key %+v is shared by %q and %q", k, prev, describes)
		}
		seen[k] = describes
	}

	add(FilteredStateKey{Group: 1, Filter: "types:m.room.history_visibility"}, "history visibility at 1")
	add(FilteredStateKey{Group: 1, Filter: "members:matrix.org"}, "matrix.org members at 1")
	add(FilteredStateKey{Group: 1, Filter: "members:example.com"}, "example.com members at 1")
	add(FilteredStateKey{Group: 2, Filter: "types:m.room.history_visibility"}, "history visibility at 2")
	add(FilteredStateKey{Group: 2, Filter: "members:matrix.org"}, "matrix.org members at 2")

	if len(seen) != 5 {
		t.Errorf("expected 5 distinct keys, got %d", len(seen))
	}
}
