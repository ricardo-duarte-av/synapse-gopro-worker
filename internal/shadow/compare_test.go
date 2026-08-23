package shadow

import (
	"encoding/json"
	"testing"
)

func body(t *testing.T, pdus, auth []string) []byte {
	t.Helper()
	b, err := json.Marshal(stateIDsBody{PDUIDs: pdus, AuthChainIDs: auth})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCompareStateIDsIgnoresOrder(t *testing.T) {
	// Synapse builds these from a Python set, so the order varies between
	// identical responses. Comparing byte-for-byte would report a mismatch on
	// essentially every request.
	syn := body(t, []string{"$a", "$b", "$c"}, []string{"$x", "$y"})
	nat := body(t, []string{"$c", "$a", "$b"}, []string{"$y", "$x"})

	diff, err := CompareStateIDs(syn, nat)
	if err != nil {
		t.Fatal(err)
	}
	if diff != nil {
		t.Errorf("reordered but identical sets reported a diff: %+v", diff)
	}
}

func TestCompareStateIDsFindsDifferences(t *testing.T) {
	syn := body(t, []string{"$a", "$b"}, []string{"$x"})
	nat := body(t, []string{"$a", "$c"}, []string{"$x"})

	diff, err := CompareStateIDs(syn, nat)
	if err != nil {
		t.Fatal(err)
	}
	if diff == nil {
		t.Fatal("expected a diff")
	}

	var pdus *struct {
		missing, extra []string
	}
	for _, f := range diff.Fields {
		if f.Field == "pdu_ids" {
			pdus = &struct{ missing, extra []string }{f.MissingFromNative, f.ExtraInNative}
		}
	}
	if pdus == nil {
		t.Fatal("no pdu_ids field in the diff")
	}
	if len(pdus.missing) != 1 || pdus.missing[0] != "$b" {
		t.Errorf("MissingFromNative = %v, want [$b]", pdus.missing)
	}
	if len(pdus.extra) != 1 || pdus.extra[0] != "$c" {
		t.Errorf("ExtraInNative = %v, want [$c]", pdus.extra)
	}
}

func TestCompareStateIDsEmptyResponses(t *testing.T) {
	// An empty list and a null must compare equal: both mean "no events", and
	// treating them as different would flag a mismatch on every empty response.
	for _, tc := range []struct{ name, syn, nat string }{
		{"both empty arrays", `{"pdu_ids":[],"auth_chain_ids":[]}`, `{"pdu_ids":[],"auth_chain_ids":[]}`},
		{"null versus empty", `{"pdu_ids":null,"auth_chain_ids":null}`, `{"pdu_ids":[],"auth_chain_ids":[]}`},
		{"missing keys", `{}`, `{"pdu_ids":[],"auth_chain_ids":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diff, err := CompareStateIDs([]byte(tc.syn), []byte(tc.nat))
			if err != nil {
				t.Fatal(err)
			}
			if diff != nil {
				t.Errorf("reported a diff for equivalent empty responses: %+v", diff)
			}
		})
	}
}

func TestCompareStateIDsDeduplicates(t *testing.T) {
	// These are sets; a repeated ID on one side is not a difference.
	syn := []byte(`{"pdu_ids":["$a","$a","$b"],"auth_chain_ids":[]}`)
	nat := []byte(`{"pdu_ids":["$b","$a"],"auth_chain_ids":[]}`)
	diff, err := CompareStateIDs(syn, nat)
	if err != nil {
		t.Fatal(err)
	}
	if diff != nil {
		t.Errorf("duplicate IDs reported as a difference: %+v", diff)
	}
}

func TestCompareStateIDsRejectsInvalidJSON(t *testing.T) {
	if _, err := CompareStateIDs([]byte("{bad"), []byte(`{"pdu_ids":[]}`)); err == nil {
		t.Error("expected an error for invalid Synapse body")
	}
	if _, err := CompareStateIDs([]byte(`{"pdu_ids":[]}`), []byte("{bad")); err == nil {
		t.Error("expected an error for invalid native body")
	}
}

func TestCompareStatus(t *testing.T) {
	cases := []struct {
		syn, nat             int
		agree, compareBodies bool
	}{
		{200, 200, true, true},
		// Matching errors agree, and their bodies add nothing.
		{403, 403, true, false},
		{404, 404, true, false},
		// A status difference is itself the mismatch.
		{200, 403, false, false},
		{403, 200, false, false},
		{404, 403, false, false},
	}
	for _, tc := range cases {
		agree, compare := CompareStatus(tc.syn, tc.nat)
		if agree != tc.agree || compare != tc.compareBodies {
			t.Errorf("CompareStatus(%d,%d) = (%v,%v), want (%v,%v)",
				tc.syn, tc.nat, agree, compare, tc.agree, tc.compareBodies)
		}
	}
}

func TestDecodeParam(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"%21room%3Aexample.com", "!room:example.com"},
		{"%24event", "$event"},
		{"!already:decoded", "!already:decoded"},
		// Invalid encoding must pass through rather than becoming empty, so a
		// malformed request still logs something identifiable.
		{"%zz", "%zz"},
	} {
		if got := DecodeParam(tc.in); got != tc.want {
			t.Errorf("DecodeParam(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
