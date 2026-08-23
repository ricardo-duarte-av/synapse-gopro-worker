package fedauth

import (
	"net/http"
	"testing"
)

func TestVerifyRejectsBadHeaders(t *testing.T) {
	v := New("example.com", Options{})

	cases := []struct {
		name string
		auth string
	}{
		{"no header at all", ""},
		{"not X-Matrix", "Bearer abcdef"},
		{"missing origin", `X-Matrix destination="example.com",key="ed25519:a",sig="AAAA"`},
		{"missing signature", `X-Matrix origin="remote.example",destination="example.com",key="ed25519:a"`},
		{"missing key id", `X-Matrix origin="remote.example",destination="example.com",sig="AAAA"`},
		// A request addressed to a different homeserver must be refused, or a
		// signature made for another server would be replayable against us.
		{"wrong destination", `X-Matrix origin="remote.example",destination="other.example",key="ed25519:a",sig="AAAA"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://example.com/_matrix/federation/v1/state_ids/%21r%3Aex", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}

			res := v.Verify(req)
			if res.OK() {
				t.Fatalf("Verify accepted %q", tc.auth)
			}
			if res.Origin != "" {
				t.Errorf("Origin = %q on a failed verification, want empty", res.Origin)
			}
			if res.Status() < 400 {
				t.Errorf("Status() = %d, want a client error", res.Status())
			}
		})
	}
}

func TestResultStatus(t *testing.T) {
	if got := (Result{}).Status(); got != http.StatusOK {
		t.Errorf("successful Status() = %d, want 200", got)
	}
	if !(Result{}).OK() {
		t.Error("a Result with no error should be OK")
	}
}

func TestServerName(t *testing.T) {
	if got := New("example.com", Options{}).ServerName(); got != "example.com" {
		t.Errorf("ServerName() = %q, want example.com", got)
	}
}
