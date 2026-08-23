// Package fedauth verifies Matrix server-server request authentication.
//
// It wraps mautrix's federation.ServerAuth rather than reimplementing any of
// it: parsing the X-Matrix header, fetching and caching the origin server's
// published keys, and checking the ed25519 signature over the canonical JSON of
// the request are all things where a subtle mistake is a security hole.
//
// Note that verifying an incoming request needs only the *remote* server's
// public keys, which are fetched over unauthenticated federation. This worker
// therefore never needs the homeserver's private signing key, and deliberately
// does not mount it.
package fedauth

import (
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/federation"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// Verifier authenticates incoming federation requests.
type Verifier struct {
	auth       *federation.ServerAuth
	serverName string
	// notaries are the trusted key servers queried before falling back to a
	// direct fetch, mirroring Synapse's trusted_key_servers.
	notaries []string
}

// Options tunes key fetching.
type Options struct {
	// KeyRefetchDelay is the minimum time before re-querying a server whose
	// key fetch failed, so an unreachable server cannot be turned into a
	// stream of outbound requests. Zero uses one hour.
	KeyRefetchDelay time.Duration
	// Timeout bounds a key fetch. Zero uses 30s.
	Timeout time.Duration
	// Notaries are trusted key servers, queried before a direct fetch. Copy
	// these from Synapse's trusted_key_servers so both resolve keys the same
	// way; without them a server that is momentarily unreachable cannot be
	// verified even though Synapse can verify it.
	Notaries []string
	// DB is Synapse's database, read only, used to reuse its cached keys.
	// Optional; without it every unknown server needs a network fetch.
	DB *store.Store
	// Log is used for cache and notary diagnostics.
	Log zerolog.Logger
}

// New builds a Verifier for the given homeserver name.
func New(serverName string, opts Options) *Verifier {
	if opts.KeyRefetchDelay <= 0 {
		opts.KeyRefetchDelay = time.Hour
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}

	cache := newLayeredCache(opts.DB, opts.KeyRefetchDelay, opts.Log)

	settings := exhttp.SensibleClientSettings
	settings.GlobalTimeout = opts.Timeout

	// No signing key: this client only fetches published keys, which is an
	// unauthenticated federation request.
	// The resolution cache (well-known and SRV lookups) is separate from the
	// key cache; only the latter is layered over Synapse's.
	client := federation.NewClient(serverName, nil, cache.mem, settings)

	auth := federation.NewServerAuth(client, cache, func(federation.XMatrixAuth) string {
		// This worker serves exactly one homeserver, so the only acceptable
		// destination is that server. Returning it unconditionally makes
		// mautrix reject a request addressed to anyone else.
		return serverName
	})
	// These endpoints are all GET with no body; nothing legitimate is large.
	auth.MaxBodySize = 64 * 1024

	return &Verifier{auth: auth, serverName: serverName, notaries: opts.Notaries}
}

// Result describes the outcome of verifying a request.
type Result struct {
	// Origin is the verified server name. It is only meaningful when Err is nil.
	Origin string
	// Err is the Matrix error to return when verification failed.
	Err *mautrix.RespError
}

// OK reports whether the request was authenticated.
func (r Result) OK() bool { return r.Err == nil }

// Status returns the HTTP status for a failed verification.
func (r Result) Status() int {
	if r.Err == nil {
		return http.StatusOK
	}
	if r.Err.StatusCode != 0 {
		return r.Err.StatusCode
	}
	return http.StatusUnauthorized
}

// Verify authenticates a request and returns the verified origin server.
//
// The returned origin is the only value safe to use for access control: the
// origin field of an unverified X-Matrix header is attacker-controlled, and
// trusting it would let any server read state from rooms it is not in.
func (v *Verifier) Verify(r *http.Request) Result {
	// mautrix unconditionally closes the body, which panics when it is nil.
	// A server-built request always has one, but a request constructed in code
	// (http.NewRequest with a nil body, as verification replay does) does not.
	// Guard here rather than at every call site: a panic on the request path
	// would take down a worker that is otherwise serving correctly.
	if r.Body == nil {
		r = r.Clone(r.Context())
		r.Body = http.NoBody
	}

	// Try the cheap key sources first. mautrix falls back to fetching from the
	// origin server directly, which is the last resort in Synapse's order too.
	v.warmCache(r.Context(), r.Header.Get("Authorization"))

	modified, respErr := v.auth.Authenticate(r)
	if respErr != nil {
		return Result{Err: respErr}
	}
	return Result{Origin: federation.OriginServerNameFromRequest(modified)}
}

// ServerName returns the homeserver this verifier accepts requests for.
func (v *Verifier) ServerName() string { return v.serverName }
