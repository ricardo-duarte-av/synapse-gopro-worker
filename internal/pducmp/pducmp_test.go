package pducmp

import (
	"encoding/json"
	"testing"
)

// Integers above 2^53 must compare exactly.
//
// Decoding into `any` without UseNumber turns every number into a float64,
// which rounds silently past 2^53: depth 9007199254740993 becomes
// 9007199254740992. Two events with different depths then compared equal, so
// the comparator could only ever hide a mismatch -- never invent one, which is
// why nothing in the metrics or diff log ever pointed at it.
//
// Not hypothetical: this deployment holds 31 events with depths in that range,
// the artefacts of an attack on one room, and depth decides whether /state
// serves an event at all.
func TestBigIntegersCompareExactly(t *testing.T) {
	for _, tc := range []struct {
		name            string
		synapse, native string
		want            bool
	}{
		{"differing depth just above 2^53",
			`{"depth":9007199254740993,"unsigned":{}}`,
			`{"depth":9007199254740992,"unsigned":{}}`, false},
		{"identical depth above 2^53",
			`{"depth":9007199254741012,"unsigned":{}}`,
			`{"depth":9007199254741012,"unsigned":{}}`, true},
		{"differing depth below 2^53 still caught",
			`{"depth":11,"unsigned":{}}`,
			`{"depth":12,"unsigned":{}}`, false},
		// Power levels are the other place large integers appear.
		{"differing power level above 2^53",
			`{"content":{"users":{"@a:b":9007199254740993}},"unsigned":{}}`,
			`{"content":{"users":{"@a:b":9007199254740992}},"unsigned":{}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Equal(json.RawMessage(tc.synapse), json.RawMessage(tc.native)); got != tc.want {
				t.Errorf("Equal = %v, want %v", got, tc.want)
			}
		})
	}
}
