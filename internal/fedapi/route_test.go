package fedapi

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		name     string
		path     string
		wantOK   bool
		endpoint Endpoint
		param    string
	}{
		{
			name:     "state ids",
			path:     "/_matrix/federation/v1/state_ids/%21room%3Aexample.com",
			wantOK:   true,
			endpoint: EndpointStateIDs,
			param:    "%21room%3Aexample.com",
		},
		{
			name:     "state",
			path:     "/_matrix/federation/v1/state/%21room%3Aexample.com",
			wantOK:   true,
			endpoint: EndpointState,
			param:    "%21room%3Aexample.com",
		},
		{
			name:     "event",
			path:     "/_matrix/federation/v1/event/%24abc",
			wantOK:   true,
			endpoint: EndpointEvent,
			param:    "%24abc",
		},
		{
			// Synapse's servlet regexes end in "/?", so a trailing slash is
			// equivalent.
			name:     "trailing slash accepted",
			path:     "/_matrix/federation/v1/state/%21room%3Aexample.com/",
			wantOK:   true,
			endpoint: EndpointState,
			param:    "%21room%3Aexample.com",
		},
		{
			// The parameter must stay escaped: %2F is part of the ID, not a
			// path separator.
			name:     "encoded slash stays in the parameter",
			path:     "/_matrix/federation/v1/event/%24abc%2Fdef",
			wantOK:   true,
			endpoint: EndpointEvent,
			param:    "%24abc%2Fdef",
		},
		{name: "state ids is not matched as state", path: "/_matrix/federation/v1/state_ids/%21r", wantOK: true, endpoint: EndpointStateIDs, param: "%21r"},
		{name: "missing parameter", path: "/_matrix/federation/v1/state/", wantOK: false},
		{name: "extra path segment", path: "/_matrix/federation/v1/event/%24abc/extra", wantOK: false},
		{name: "unrelated federation endpoint", path: "/_matrix/federation/v1/backfill/%21r", wantOK: false},
		{name: "v2 is not served", path: "/_matrix/federation/v2/send_join/%21r", wantOK: false},
		{name: "client api", path: "/_matrix/client/v3/sync", wantOK: false},
		{name: "root", path: "/", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Match(tc.path)
			if ok != tc.wantOK {
				t.Fatalf("Match(%q) ok = %v, want %v", tc.path, ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if got.Endpoint != tc.endpoint {
				t.Errorf("endpoint = %q, want %q", got.Endpoint, tc.endpoint)
			}
			if got.Param != tc.param {
				t.Errorf("param = %q, want %q", got.Param, tc.param)
			}
		})
	}
}

func TestOriginFromAuth(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   string
	}{
		{
			name:   "well formed",
			header: `X-Matrix origin="a.example",destination="b.example",key="ed25519:1",sig="AAAA"`,
			want:   "a.example",
		},
		{
			name:   "unquoted values",
			header: `X-Matrix origin=a.example,key=ed25519:1,sig=AAAA`,
			want:   "a.example",
		},
		{name: "empty", header: "", want: ""},
		{name: "bearer token is not X-Matrix", header: "Bearer abcdef", want: ""},
		{name: "no origin field", header: `X-Matrix key="ed25519:1",sig="AAAA"`, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := originFromAuth(tc.header); got != tc.want {
				t.Errorf("originFromAuth(%q) = %q, want %q", tc.header, got, tc.want)
			}
		})
	}
}
