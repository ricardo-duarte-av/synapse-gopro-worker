package shadow

import (
	"github.com/daedric/synapse-gopro-worker/internal/native"
	"time"

	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
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

// Synapse's client-facing reads stamp prev_content/prev_sender into the shared
// cached event, so /event sometimes carries them even though get_persisted_pdu
// never asks for them. Tolerated upstream-only, since we cannot observe what
// polluted Synapse's cache -- but never tolerated in our own output.
func TestSynapseOnlyPrevContentIsTolerated(t *testing.T) {
	const nativePDU = `{"event_id":"$a","type":"m.room.member","unsigned":{"replaces_state":"$old"}}`
	synapse := []byte(`{"origin":"h","origin_server_ts":1,"pdus":[{"event_id":"$a","type":"m.room.member","unsigned":{"replaces_state":"$old","prev_content":{"membership":"leave"},"prev_sender":"@u:x"}}]}`)
	native := []byte(`{"origin":"h","origin_server_ts":1,"pdus":[` + nativePDU + `]}`)

	diff, err := CompareEvent(synapse, native)
	if err != nil {
		t.Fatal(err)
	}
	if diff != nil {
		t.Errorf("upstream-only prev_content reported as a mismatch: %+v", diff)
	}

	// The reverse is still our bug: we must not invent these fields.
	diff, err = CompareEvent(native, synapse)
	if err != nil {
		t.Fatal(err)
	}
	if diff == nil {
		t.Error("prev_content present only in our answer was not reported")
	}

	// And a genuine unsigned difference is still caught.
	other := []byte(`{"origin":"h","origin_server_ts":1,"pdus":[{"event_id":"$a","type":"m.room.member","unsigned":{"replaces_state":"$different"}}]}`)
	diff, err = CompareEvent(other, native)
	if err != nil {
		t.Fatal(err)
	}
	if diff == nil {
		t.Error("a real replaces_state difference was not reported")
	}
}

// Synapse failing to *fetch* a key is not a verdict on the request, so it must
// not be counted as a disagreement. Synapse judging a signature bad is, and
// must never be suppressed — that is the security alarm.
func TestUpstreamKeyFetchFailureIsRecognised(t *testing.T) {
	body := func(errcode, msg string) []byte {
		return []byte(`{"errcode":"` + errcode + `","error":"` + msg + `"}`)
	}
	for _, tc := range []struct {
		name  string
		proxy ProxyResult
		want  bool
	}{
		{"key fetch failed", ProxyResult{Status: 401, Body: body("M_UNAUTHORIZED",
			`Failed to find any key to satisfy: _FetchKeyRequest(server_name='ryuu.eu', key_ids=['ed25519:a_EMUw'])`)}, true},

		// Everything below is a real verdict, or not judgeable, and must not
		// be waved through.
		{"invalid signature", ProxyResult{Status: 401, Body: body("M_UNAUTHORIZED",
			"Invalid signature for server ryuu.eu with key ed25519:a_EMUw")}, false},
		{"missing auth header", ProxyResult{Status: 401, Body: body("M_UNAUTHORIZED",
			"Missing Authorization headers")}, false},
		{"forbidden, not unauthorized", ProxyResult{Status: 403, Body: body("M_FORBIDDEN",
			"Failed to find any key to satisfy: whatever")}, false},
		{"wrong errcode", ProxyResult{Status: 401, Body: body("M_FORBIDDEN",
			"Failed to find any key to satisfy: whatever")}, false},
		{"truncated body cannot be judged", ProxyResult{Status: 401, Truncated: true, Body: body("M_UNAUTHORIZED",
			"Failed to find any key to satisfy: whatever")}, false},
		{"empty body", ProxyResult{Status: 401}, false},
		{"not json", ProxyResult{Status: 401, Body: []byte("nope")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := upstreamCouldNotFetchKeys(tc.proxy); got != tc.want {
				t.Errorf("upstreamCouldNotFetchKeys = %v, want %v", got, tc.want)
			}
		})
	}
}

// Synapse shedding load is not a disagreement about the answer. Our limiter is
// deliberately inactive while proxying or shadowing, so every 429 Synapse
// issues would otherwise be recorded as a mismatch -- 58 of them in one hour
// when two servers started hammering, none of which said anything about
// whether our answers were right.
//
// Driven through the runner rather than by restating the condition: the point
// is that no diff is recorded and the skip is counted.
func TestUpstreamRateLimitIsNotAMismatch(t *testing.T) {
	for _, tc := range []struct {
		name         string
		proxyStatus  int
		nativeStatus int
		wantMismatch bool
	}{
		{"synapse shed load, we answered", 429, 200, false},
		{"synapse shed load, we refused for another reason", 429, 403, false},
		{"a real status disagreement is still reported", 403, 200, true},
		{"synapse 404, we 200", 404, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeSkip := labelled(t, "gopro_shadow_skipped_total", "reason", "upstream_rate_limited")
			beforeRecorded := counterTotal(t, "gopro_shadow_results_total")

			r := NewRunner(nil, "example.com", nil, nil, zerolog.Nop(), Options{Concurrency: 1})
			r.finish(Request{Endpoint: "state_ids", Origin: "noisy.example"},
				ProxyResult{Status: tc.proxyStatus, Body: []byte(`{"errcode":"M_LIMIT_EXCEEDED"}`)},
				time.Millisecond, []byte(`{"pdu_ids":[],"auth_chain_ids":[]}`), tc.nativeStatus, native.Meta{})

			gotMismatch := counterTotal(t, "gopro_shadow_results_total") > beforeRecorded
			gotSkip := labelled(t, "gopro_shadow_skipped_total", "reason", "upstream_rate_limited") > beforeSkip

			if gotMismatch != tc.wantMismatch {
				t.Errorf("recorded a mismatch = %v, want %v", gotMismatch, tc.wantMismatch)
			}
			if !tc.wantMismatch && !gotSkip {
				t.Error("not counted as upstream_rate_limited either, so it vanished silently")
			}
		})
	}
}

func counterTotal(t *testing.T, name string) float64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

func labelled(t *testing.T, name, label, value string) float64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}
