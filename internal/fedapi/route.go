// Package fedapi routes and serves the federation read endpoints.
package fedapi

import "strings"

// Endpoint identifies one of the three federation read endpoints.
type Endpoint string

const (
	EndpointEvent    Endpoint = "event"
	EndpointState    Endpoint = "state"
	EndpointStateIDs Endpoint = "state_ids"
	EndpointUnknown  Endpoint = "unknown"
)

// Route describes a matched federation request.
type Route struct {
	Endpoint Endpoint
	// Param is the trailing path parameter, still percent-encoded exactly as it
	// arrived: the event ID for /event, the room ID for /state and /state_ids.
	Param string
}

// Prefixes are matched against the escaped path so that percent-encoding in the
// trailing parameter is preserved. Synapse's servlet regexes accept an optional
// trailing slash, so we do too.
var prefixes = []struct {
	prefix   string
	endpoint Endpoint
}{
	// state_ids must be tested before state: "/state_ids/" does not share a
	// prefix with "/state/", but listing the longer path first keeps the
	// ordering robust if either is ever loosened.
	{"/_matrix/federation/v1/state_ids/", EndpointStateIDs},
	{"/_matrix/federation/v1/state/", EndpointState},
	{"/_matrix/federation/v1/event/", EndpointEvent},
}

// Match classifies an escaped request path. It reports false for anything
// outside the three endpoints this worker serves.
//
// escapedPath must come from url.URL.EscapedPath(), not URL.Path: the trailing
// parameter carries percent-encoded characters ('!' and ':' in room IDs, '$' in
// event IDs) that must survive to the upstream unchanged.
func Match(escapedPath string) (Route, bool) {
	for _, p := range prefixes {
		if !strings.HasPrefix(escapedPath, p.prefix) {
			continue
		}
		param := strings.TrimSuffix(escapedPath[len(p.prefix):], "/")
		if param == "" || strings.Contains(param, "/") {
			// Synapse's regex is "[^/]*", so a further path segment is not this
			// endpoint.
			return Route{}, false
		}
		return Route{Endpoint: p.endpoint, Param: param}, true
	}
	return Route{}, false
}
