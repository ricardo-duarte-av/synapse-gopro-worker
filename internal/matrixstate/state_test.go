package matrixstate

import (
	"encoding/json"
	"testing"
)

// Synapse drops out-of-range depths from /state but not from /event. This
// deployment holds 31 such events, 24 of them m.room.member state events
// appearing in 1,918 state_groups_state rows, so the filter is reachable in
// production rather than theoretical.
func TestDepthInCanonicalRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"in range", `{"depth":100}`, true},
		{"max allowed", `{"depth":9007199254740991}`, true},
		{"min allowed", `{"depth":-9007199254740991}`, true},
		{"one past max", `{"depth":9007199254740992}`, false},
		{"one past min", `{"depth":-9007199254740992}`, false},
		// The real events here sit just past the boundary.
		{"observed in production", `{"depth":9007199254741012}`, false},
		// Beyond int64 entirely: out of the canonical range by definition.
		{"beyond int64", `{"depth":123456789012345678901234567890}`, false},
		// Synapse guards on `"depth" in pdu`, so an absent depth is kept.
		{"no depth field", `{"type":"m.room.member"}`, true},
		// Synapse compares int to str here and raises TypeError -- it neither
		// keeps nor drops, it fails the request. Keeping the PDU surfaces the
		// difference to the comparator instead of inventing a rule.
		{"string depth", `{"depth":"12"}`, true},
		{"fractional depth", `{"depth":1.5}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := depthInCanonicalRange([]byte(tc.body)); got != tc.want {
				t.Errorf("depthInCanonicalRange(%s) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

// The digest must not depend on ordering, because the two sides may order the same
// set differently, and must still distinguish a set from a multiset.
func TestDigestAccumulator(t *testing.T) {
	build := func(pairs ...[2]string) [32]byte {
		var d digestAccumulator
		for _, p := range pairs {
			d.add(p[0], []byte(p[1]))
		}
		return d.digest()
	}
	a := [2]string{"$a", `{"x":1}`}
	b := [2]string{"$b", `{"y":2}`}

	if build(a, b) != build(b, a) {
		t.Error("digest depends on ordering; the two sides may order a set differently")
	}
	if build(a, b) == build(a) {
		t.Error("digest ignored an event")
	}
	// XOR-based accumulation would make this pass wrongly: a duplicated event
	// would cancel out and digest identically to its absence.
	if build(a, a) == build() {
		t.Error("a duplicated event cancelled out; the accumulator is not multiset-safe")
	}
	if build(a, a) == build(a) {
		t.Error("digest cannot distinguish one occurrence from two")
	}
	// Same ID, different body must differ.
	if build(a) == build([2]string{"$a", `{"x":2}`}) {
		t.Error("digest ignored the body")
	}
	// Same body, different ID must differ.
	if build(a) == build([2]string{"$c", `{"x":1}`}) {
		t.Error("digest ignored the event ID")
	}
}

// /state and /event serialise the same event differently: /state reaches
// get_pdu_json via serialize_and_filter_pdus, which passes time_now=None, so
// age_ts survives and no age is added. Roughly half the events here carry
// age_ts, so getting this wrong would differ on most of a response.
func TestAgeConversionOnlyWithAClock(t *testing.T) {
	body := []byte(`{"type":"m.room.member","unsigned":{"age_ts":1000,"replaces_state":"$p"}}`)

	unsignedOf := func(t *testing.T, b []byte) map[string]json.RawMessage {
		t.Helper()
		var ev struct {
			Unsigned map[string]json.RawMessage `json:"unsigned"`
		}
		if err := json.Unmarshal(b, &ev); err != nil {
			t.Fatal(err)
		}
		return ev.Unsigned
	}

	withClock, err := applyPDUJSONRules(body, 5000)
	if err != nil {
		t.Fatal(err)
	}
	u := unsignedOf(t, withClock)
	if _, ok := u["age_ts"]; ok {
		t.Error("/event serialisation kept age_ts; get_pdu_json deletes it when given a clock")
	}
	if string(u["age"]) != "4000" {
		t.Errorf("age = %s, want 4000", u["age"])
	}

	noClock, err := applyPDUJSONRulesAt(body, nil)
	if err != nil {
		t.Fatal(err)
	}
	u = unsignedOf(t, noClock)
	if string(u["age_ts"]) != "1000" {
		t.Errorf("age_ts = %s, want it preserved: /state passes time_now=None", u["age_ts"])
	}
	if _, ok := u["age"]; ok {
		t.Error("/state serialisation added age; with time_now=None get_pdu_json does not")
	}
	// The allowlist still applies either way.
	if string(u["replaces_state"]) != `"$p"` {
		t.Errorf("replaces_state = %s, want it kept", u["replaces_state"])
	}
}
