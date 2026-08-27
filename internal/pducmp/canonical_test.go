package pducmp

import (
	"encoding/json"
	"testing"
)

// backslash builds an escape sequence without a literal backslash-u in the
// source, so nothing between here and the compiler can turn it into a real
// control character.
func escaped(seq string) string { return string([]byte{0x5c}) + seq }

func canon(t *testing.T, raw string) string {
	t.Helper()
	out, ok := Canonical(json.RawMessage(raw))
	if !ok {
		t.Fatalf("Canonical(%s) failed", raw)
	}
	return string(out)
}

// Our responses splice stored JSON while Synapse re-serialises from a dict, so
// the two differ in key order on essentially every event. A digest taken over
// raw bytes would therefore mismatch on all of them.
func TestCanonicalNormalisesKeyOrder(t *testing.T) {
	a := canon(t, `{"b":1,"a":2,"content":{"z":1,"y":2},"unsigned":{}}`)
	b := canon(t, `{"content":{"y":2,"z":1},"a":2,"b":1,"unsigned":{}}`)
	if a != b {
		t.Errorf("key order not normalised:\n  %s\n  %s", a, b)
	}
}

// The digest must still see a genuine difference.
func TestCanonicalKeepsRealDifferences(t *testing.T) {
	for _, tc := range [][2]string{
		{`{"depth":1,"unsigned":{}}`, `{"depth":2,"unsigned":{}}`},
		// Above 2^53, where a float64 decode would round both to the same value.
		{`{"depth":9007199254740993,"unsigned":{}}`, `{"depth":9007199254740992,"unsigned":{}}`},
		{`{"content":{"a":1},"unsigned":{}}`, `{"content":{"a":2},"unsigned":{}}`},
		{`{"unsigned":{"replaces_state":"$a"}}`, `{"unsigned":{"replaces_state":"$b"}}`},
	} {
		if canon(t, tc[0]) == canon(t, tc[1]) {
			t.Errorf("canonical form collapsed a real difference:\n  %s\n  %s", tc[0], tc[1])
		}
	}
}

// Synapse's cached EventBase sometimes carries prev_content/prev_sender into a
// federation response. Equal drops those from Synapse's side only; a digest
// cannot, so they must go from whichever side has them.
func TestCanonicalDropsCachePollutionSymmetrically(t *testing.T) {
	clean := canon(t, `{"type":"m.room.member","unsigned":{"age_ts":5}}`)
	polluted := canon(t, `{"type":"m.room.member","unsigned":{"age_ts":5,"prev_content":{"membership":"join"},"prev_sender":"@a:b"}}`)
	if clean != polluted {
		t.Errorf("cache pollution survived canonicalisation:\n  %s\n  %s", clean, polluted)
	}
	// age_ts itself must survive: /state preserves it, and it is the field
	// that distinguishes most events from each other in a redacted state set.
	if canon(t, `{"unsigned":{"age_ts":5}}`) == canon(t, `{"unsigned":{"age_ts":6}}`) {
		t.Error("age_ts was dropped or collapsed; /state serves it verbatim")
	}
}

// age is wall-clock derived, so its value can never match between the two
// sides. It is reduced to a marker rather than removed, so a side emitting it
// where the other does not is still a difference. /state never emits it --
// serialize_and_filter_pdus passes time_now=None -- but /event does, and this
// package holds one definition for both.
func TestCanonicalMarksAgeWithoutComparingIt(t *testing.T) {
	if canon(t, `{"unsigned":{"age":10}}`) != canon(t, `{"unsigned":{"age":99999}}`) {
		t.Error("age compared by value; it is derived at serialisation time and cannot match")
	}
	if canon(t, `{"unsigned":{"age":10}}`) == canon(t, `{"unsigned":{}}`) {
		t.Error("age present on one side only must still be a difference")
	}
}

// 14,654 events here contain an escaped NUL, which PostgreSQL's jsonb cannot
// even cast. Canonicalisation must round-trip it rather than corrupting or
// dropping it, or those events would digest differently on the two sides.
func TestCanonicalPreservesEscapedNUL(t *testing.T) {
	raw := `{"content":{"body":"a` + escaped("u0000") + `b"},"unsigned":{}}`
	out := canon(t, raw)

	// The escape must survive in some encoding of the same character; what
	// matters is that both sides produce the same bytes and that the character
	// is not lost.
	same := canon(t, raw)
	if out != same {
		t.Errorf("canonicalisation is not deterministic for escaped NUL:\n  %s\n  %s", out, same)
	}
	if out == canon(t, `{"content":{"body":"ab"},"unsigned":{}}`) {
		t.Errorf("the NUL was dropped: %s", out)
	}
	var probe struct {
		Content struct {
			Body string `json:"body"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(out), &probe); err != nil {
		t.Fatalf("canonical output is not valid JSON: %v", err)
	}
	if len(probe.Content.Body) != 3 || probe.Content.Body[1] != 0 {
		t.Errorf("body = %q, want three bytes with a NUL in the middle", probe.Content.Body)
	}
}

// A body we cannot parse must be reported rather than silently digested as
// something else.
func TestCanonicalRejectsUnparseable(t *testing.T) {
	if _, ok := Canonical(json.RawMessage(`{"broken":`)); ok {
		t.Error("unparseable body accepted")
	}
}
