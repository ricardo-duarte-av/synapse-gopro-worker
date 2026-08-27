package shadow

import (
	"encoding/json"
	"fmt"
	"testing"
)

func gmeBody(events ...string) []byte {
	return []byte(`{"events":[` + join(events) + `]}`)
}

func join(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

func reqLimit(n int) []byte { return []byte(fmt.Sprintf(`{"limit":%d}`, n)) }

var (
	evA = `{"type":"m.room.message","sender":"@a:b","depth":1,"unsigned":{}}`
	evB = `{"type":"m.room.message","sender":"@b:b","depth":2,"unsigned":{}}`
	evC = `{"type":"m.room.message","sender":"@c:b","depth":3,"unsigned":{}}`
)

// Order must not matter. The two sides walk the DAG in different orders --
// Synapse iterates a Python set, we iterate a sorted slice -- so comparing by
// position would report a disagreement on nearly every response.
func TestCompareGetMissingEventsIgnoresOrder(t *testing.T) {
	diff, skip, err := CompareGetMissingEvents(reqLimit(10),
		gmeBody(evA, evB, evC), gmeBody(evC, evA, evB))
	if err != nil {
		t.Fatal(err)
	}
	if skip != "" || diff != nil {
		t.Errorf("reordering reported as a disagreement: skip=%q diff=%+v", skip, diff)
	}
}

// Key by content, not by event ID: room version 3 and later carry no event_id,
// so an ID-keyed index would fall back to position for most rooms.
func TestCompareGetMissingEventsKeysOnContentNotID(t *testing.T) {
	// Neither event carries an event_id, and they differ only in content.
	diff, skip, err := CompareGetMissingEvents(reqLimit(10), gmeBody(evA), gmeBody(evB))
	if err != nil {
		t.Fatal(err)
	}
	if skip != "" {
		t.Fatalf("unexpected skip %q", skip)
	}
	if diff == nil {
		t.Fatal("two different events compared equal")
	}
}

// A walk that completed is fully determined and must be compared exactly.
func TestCompareGetMissingEventsCompletedWalkIsStrict(t *testing.T) {
	// Three of a possible ten: the walk ran out of DAG, not out of budget.
	diff, skip, err := CompareGetMissingEvents(reqLimit(10),
		gmeBody(evA, evB, evC), gmeBody(evA, evB))
	if err != nil {
		t.Fatal(err)
	}
	if skip != "" {
		t.Errorf("a completed walk was reclassified as %q; only a truncated one may be", skip)
	}
	if diff == nil {
		t.Fatal("a missing event went undetected")
	}
	if len(diff.Fields) != 1 || len(diff.Fields[0].MissingFromNative) != 1 {
		t.Errorf("diff did not name the missing event: %+v", diff.Fields)
	}
}

// A walk that stopped at the limit has no single correct answer, because which
// events survive depends on an iteration order neither side controls.
func TestCompareGetMissingEventsTruncatedWalkIsReclassified(t *testing.T) {
	diff, skip, err := CompareGetMissingEvents(reqLimit(2),
		gmeBody(evA, evB), gmeBody(evA, evC))
	if err != nil {
		t.Fatal(err)
	}
	if skip != "walk_truncated" {
		t.Errorf("skip = %q, want walk_truncated", skip)
	}
	if diff != nil {
		t.Error("a truncated walk was also recorded as a disagreement")
	}
}

// Only when *both* sides filled their budget is the answer ambiguous. If one
// side stopped short it saw a different DAG, and that is a real difference.
func TestCompareGetMissingEventsOneSideShortIsAMismatch(t *testing.T) {
	diff, skip, err := CompareGetMissingEvents(reqLimit(2),
		gmeBody(evA, evB), gmeBody(evA))
	if err != nil {
		t.Fatal(err)
	}
	if skip != "" {
		t.Errorf("skip = %q; only a walk truncated on both sides is ambiguous", skip)
	}
	if diff == nil {
		t.Fatal("a short answer went undetected")
	}
}

func TestMissingEventsLimit(t *testing.T) {
	for _, tc := range []struct {
		body string
		want int
	}{
		{`{}`, 10},            // Synapse's default
		{`{"limit":5}`, 5},    //
		{`{"limit":20}`, 20},  //
		{`{"limit":100}`, 20}, // capped
		{`not json`, 10},      // unparseable falls back to the default
		{``, 10},              // absent body
	} {
		if got := missingEventsLimit([]byte(tc.body)); got != tc.want {
			t.Errorf("missingEventsLimit(%q) = %d, want %d", tc.body, got, tc.want)
		}
	}
}

func TestCompareGetMissingEventsRejectsUnparseable(t *testing.T) {
	if _, _, err := CompareGetMissingEvents(nil, []byte(`{`), gmeBody(evA)); err == nil {
		t.Error("unparseable Synapse body accepted")
	}
	if _, _, err := CompareGetMissingEvents(nil, gmeBody(evA), []byte(`{`)); err == nil {
		t.Error("unparseable native body accepted")
	}
}

// Two events we cannot canonicalise must stay distinct, or they collapse into
// one and a missing event looks like a match.
func TestIndexByContentKeepsUnparseableDistinct(t *testing.T) {
	m := indexByContent([]json.RawMessage{[]byte(`{bad`), []byte(`{alsobad`)})
	if len(m) != 2 {
		t.Errorf("two unparseable events indexed as %d entries", len(m))
	}
}
