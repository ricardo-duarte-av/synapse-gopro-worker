package matrixstate

import (
	"encoding/json"
	"strings"
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

// The digest must not depend on ordering, because the two sides may order the
// same set differently, and must still distinguish a set from a multiset.
//
// Event IDs deliberately do not participate: Synapse's response carries no
// event_id for room version 3 and later, so a digest keyed on it could not be
// computed from the far side without re-hashing every event. Nothing is lost,
// because the ID is a function of the body.
func TestDigestAccumulator(t *testing.T) {
	build := func(bodies ...string) [32]byte {
		var d digestAccumulator
		for _, b := range bodies {
			d.add([]byte(b))
		}
		return d.digest()
	}
	a := `{"x":1}`
	b := `{"y":2}`

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
	if build(a) == build(`{"x":2}`) {
		t.Error("digest ignored the body")
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

// The counter must not fire on an ordinary event, and must not be fooled by a
// message that merely mentions the field names.
func TestEmitsCachePollution(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"ordinary event", `{"unsigned":{"age_ts":5}}`, false},
		{"empty unsigned", `{"unsigned":{}}`, false},
		{"no unsigned at all", `{"type":"m.room.member"}`, false},
		{"prev_content present", `{"unsigned":{"prev_content":{"membership":"join"}}}`, true},
		{"prev_sender present", `{"unsigned":{"prev_sender":"@a:b"}}`, true},
		// A body scan would count this; a parse does not.
		{"message text mentioning the field", `{"content":{"body":"what is prev_content for?"},"unsigned":{}}`, false},
		// Nor should a field of that name anywhere but unsigned.
		{"prev_content in content", `{"content":{"prev_content":1},"unsigned":{}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := emitsCachePollution([]byte(tc.body)); got != tc.want {
				t.Errorf("emitsCachePollution(%s) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func digestOf(t *testing.T, body string) StateResult {
	t.Helper()
	res, err := DigestStateResponse(strings.NewReader(body))
	if err != nil {
		t.Fatalf("DigestStateResponse(%s): %v", body, err)
	}
	return res
}

// The comparison must survive the ways two correct implementations legitimately
// differ, and must still catch the ways they must not.
func TestDigestStateResponse(t *testing.T) {
	base := `{"pdus":[{"a":1,"depth":5,"unsigned":{}},{"b":2,"unsigned":{}}],` +
		`"auth_chain":[{"c":3,"unsigned":{}}]}`
	want := digestOf(t, base)

	if want.PDUs != 2 || want.AuthChain != 1 {
		t.Fatalf("counts = %d/%d, want 2/1", want.PDUs, want.AuthChain)
	}

	t.Run("element order does not matter", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"b":2,"unsigned":{}},{"a":1,"depth":5,"unsigned":{}}],`+
			`"auth_chain":[{"c":3,"unsigned":{}}]}`)
		if !got.Agrees(want) {
			t.Error("reordering the arrays changed the digest")
		}
	})

	t.Run("key order inside a PDU does not matter", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"depth":5,"unsigned":{},"a":1},{"unsigned":{},"b":2}],`+
			`"auth_chain":[{"unsigned":{},"c":3}]}`)
		if !got.Agrees(want) {
			t.Error("key order inside a PDU changed the digest; our responses splice " +
				"stored JSON while Synapse re-serialises from a dict, so this differs on every event")
		}
	})

	t.Run("top-level field order does not matter", func(t *testing.T) {
		got := digestOf(t, `{"auth_chain":[{"c":3,"unsigned":{}}],`+
			`"pdus":[{"a":1,"depth":5,"unsigned":{}},{"b":2,"unsigned":{}}]}`)
		if !got.Agrees(want) {
			t.Error("top-level key order changed the digest")
		}
	})

	t.Run("an unknown field is ignored", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"a":1,"depth":5,"unsigned":{}},{"b":2,"unsigned":{}}],`+
			`"auth_chain":[{"c":3,"unsigned":{}}],"something_new":{"x":[1,2,3]}}`)
		if !got.Agrees(want) {
			t.Error("an added top-level field broke the comparison; Synapse may add one")
		}
	})

	t.Run("a changed PDU is caught", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"a":1,"depth":6,"unsigned":{}},{"b":2,"unsigned":{}}],`+
			`"auth_chain":[{"c":3,"unsigned":{}}]}`)
		if got.Agrees(want) {
			t.Error("a changed depth went undetected")
		}
	})

	t.Run("a missing PDU is caught", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"a":1,"depth":5,"unsigned":{}}],`+
			`"auth_chain":[{"c":3,"unsigned":{}}]}`)
		if got.Agrees(want) {
			t.Error("a dropped PDU went undetected")
		}
	})

	// The reason the two arrays are digested separately.
	t.Run("an event in the wrong array is caught", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"a":1,"depth":5,"unsigned":{}}],`+
			`"auth_chain":[{"c":3,"unsigned":{}},{"b":2,"unsigned":{}}]}`)
		if got.Agrees(want) {
			t.Error("moving an event from pdus to auth_chain went undetected; " +
				"one digest across both arrays would miss this")
		}
	})

	t.Run("cache pollution is tolerated", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[{"a":1,"depth":5,"unsigned":{"prev_content":{"x":1}}},`+
			`{"b":2,"unsigned":{}}],"auth_chain":[{"c":3,"unsigned":{}}]}`)
		if !got.Agrees(want) {
			t.Error("Synapse's cached prev_content was reported as a disagreement")
		}
	})

	t.Run("empty arrays", func(t *testing.T) {
		got := digestOf(t, `{"pdus":[],"auth_chain":[]}`)
		if got.PDUs != 0 || got.AuthChain != 0 {
			t.Errorf("counts = %d/%d, want 0/0", got.PDUs, got.AuthChain)
		}
		if got.Agrees(want) {
			t.Error("an empty response agreed with a populated one")
		}
	})

	t.Run("malformed body is an error, not a silent match", func(t *testing.T) {
		if _, err := DigestStateResponse(strings.NewReader(`{"pdus":[{"a":1}`)); err == nil {
			t.Error("a truncated response was accepted")
		}
		if _, err := DigestStateResponse(strings.NewReader(`not json`)); err == nil {
			t.Error("a non-JSON response was accepted")
		}
	})
}
