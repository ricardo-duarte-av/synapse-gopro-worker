package store

import "testing"

// MSC2174 moved `redacts` from the top level into content, so both have to be
// read: room version 12 events here carry content.redacts, older ones the top
// level. Reading only one location would withhold every redaction in half the
// rooms, since a missing `redacts` is itself a reason to withhold.
func TestRedactsTarget(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		ok               bool
	}{
		{"top level (pre-MSC2174)", `{"type":"m.room.redaction","redacts":"$a"}`, "$a", true},
		{"in content (v11+)", `{"type":"m.room.redaction","content":{"redacts":"$b"}}`, "$b", true},
		{"top level wins when both present", `{"redacts":"$a","content":{"redacts":"$b"}}`, "$a", true},
		{"absent -- a redacted redaction", `{"type":"m.room.redaction","content":{}}`, "", false},
		{"empty string is not a target", `{"redacts":""}`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := redactsTarget([]byte(tc.body))
			if ok != tc.ok || got != tc.want {
				t.Errorf("redactsTarget(%s) = %q,%v want %q,%v", tc.body, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// Synapse clears recheck_redaction once the redaction is authorised, so an
// absent flag means the check already passed rather than that it never
// applied. Treating absent as "needs checking" would withhold redactions
// Synapse serves.
func TestNeedsRedactionRecheck(t *testing.T) {
	for _, tc := range []struct {
		name, meta string
		want       bool
	}{
		{"flag set", `{"recheck_redaction":true}`, true},
		{"flag explicitly false", `{"recheck_redaction":false}`, false},
		{"flag absent", `{"outlier":true}`, false},
		{"empty metadata", `{}`, false},
		{"no metadata at all", ``, false},
		{"unparseable", `{broken`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := needsRedactionRecheck([]byte(tc.meta)); got != tc.want {
				t.Errorf("needsRedactionRecheck(%s) = %v, want %v", tc.meta, got, tc.want)
			}
		})
	}
}
