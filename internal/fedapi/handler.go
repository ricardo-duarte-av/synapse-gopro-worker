package fedapi

import (
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/fedauth"
	"github.com/daedric/synapse-gopro-worker/internal/metrics"
	"github.com/daedric/synapse-gopro-worker/internal/native"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
	"github.com/daedric/synapse-gopro-worker/internal/ratelimit"
	"github.com/daedric/synapse-gopro-worker/internal/shadow"
)

// Handler serves the three federation read endpoints.
//
// Every request is currently forwarded to a Synapse worker. The per-endpoint
// mode already threads through so that shadow and native serving can be added
// without reshaping the request path.
type Handler struct {
	cfg     *config.Config
	proxy   *proxy.Proxy
	shadow  *shadow.Runner
	limiter *ratelimit.Limiter
	log     zerolog.Logger

	// resolver and verifier are what make canary and native modes possible:
	// an answer of our own, and proof of who asked for it. Both nil while
	// every endpoint is proxying or shadowing, in which case serveNative
	// declines and everything goes to Synapse.
	resolver   native.Resolver
	verifier   Verifier
	serverName string
	// nativeTimeout bounds a natively served answer. Exceeding it falls back
	// to the proxy, so a pathological room costs latency rather than a hung
	// request -- but the whole budget is spent *before* the proxy is asked, so
	// the client pays this plus Synapse's own time. It must stay short.
	nativeTimeout time.Duration
	// verifyTimeout bounds the after-the-fact fetch that checks a served
	// answer against Synapse.
	//
	// Deliberately *not* nativeTimeout, and much longer. Nobody is waiting on
	// this -- it runs after the response -- so it must outlive Synapse's worst
	// case rather than our own. Sharing the serving budget meant every answer
	// Synapse took longer than that to produce was abandoned and counted
	// unverified, which silently skipped exactly the slow, large-room requests
	// most likely to disagree.
	verifyTimeout time.Duration

	// limitedOrigins tracks which servers have been rejected, so their count
	// can be exported without exporting their names.
	limitedMu      sync.Mutex
	limitedOrigins map[string]struct{}

	// captureLimit is how much of the proxied body to retain for comparison.
	// A /state_ids response for a large room runs to several megabytes, so this
	// is generous; bodies past it are not compared rather than compared wrongly.
	captureLimit int64
}

// New builds a Handler. runner may be nil, in which case nothing is shadowed.
func New(cfg *config.Config, p *proxy.Proxy, runner *shadow.Runner, log zerolog.Logger, opts ...Option) *Handler {
	limit := int64(cfg.Shadow.CaptureMB) << 20
	if limit <= 0 {
		limit = 32 << 20
	}
	h := &Handler{
		cfg:           cfg,
		proxy:         p,
		shadow:        runner,
		limiter:       ratelimit.New(cfg.RCFederation),
		log:           log,
		captureLimit:  limit,
		serverName:    cfg.ServerName,
		nativeTimeout: 5 * time.Second,
		verifyTimeout: 30 * time.Second,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Verifier authenticates an incoming federation request.
//
// A narrow interface rather than the concrete type, so the request path can be
// tested without signing keys -- and so the decision "what happens when
// verification fails" can be exercised, which is the one that decides whether
// a stranger can read room state.
type Verifier interface {
	Verify(r *http.Request) fedauth.Result
}

// Option configures a Handler.
type Option func(*Handler)

// WithNative supplies what canary and native modes need: our own answer, and
// verification of who is asking. Without it those modes decline to serve and
// every request goes to Synapse, which is the safe default.
func WithNative(r native.Resolver, v Verifier, timeout, verifyTimeout time.Duration) Option {
	return func(h *Handler) {
		h.resolver = r
		h.verifier = v
		if timeout > 0 {
			h.nativeTimeout = timeout
		}
		if verifyTimeout > 0 {
			h.verifyTimeout = verifyTimeout
		}
	}
}

// Limiter exposes the rate limiter for metrics and maintenance.
func (h *Handler) Limiter() *ratelimit.Limiter { return h.limiter }

// noteLimited records that an origin was rejected and returns how many distinct
// origins have been. It answers "one noisy server or many?" without needing a
// per-origin metric label.
func (h *Handler) noteLimited(origin string) int {
	h.limitedMu.Lock()
	defer h.limitedMu.Unlock()
	if h.limitedOrigins == nil {
		h.limitedOrigins = make(map[string]struct{})
	}
	h.limitedOrigins[origin] = struct{}{}
	return len(h.limitedOrigins)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}

	// Match on the escaped path: the trailing parameter is percent-encoded and
	// must not be normalised on the way to the upstream.
	route, ok := Match(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}

	mode := h.modeFor(route.Endpoint)
	start := time.Now()

	origin := originFromAuth(r.Header.Get("Authorization"))

	endpointName := string(route.Endpoint)
	roomID := roomIDFor(route)
	eventID := eventIDFor(route, r)

	// Tracked so a fallback can be accounted for end to end: the client pays
	// our attempt plus Synapse's, and until now the second half was invisible.
	var (
		fellBack       bool
		fallbackReason string
		nativeSpent    time.Duration
	)

	// Canary and native modes answer some or all traffic themselves.
	//
	// The share is chosen by hashing the event ID rather than at random, so a
	// retrying server keeps getting the same implementation. Anything that
	// goes wrong inside -- verification, an error, a timeout, a panic -- falls
	// through to the proxy with nothing written, so a canary can cost latency
	// but not correctness.
	serveNatively := sampled(mode, eventID)

	// Rate limiting applies only to the share we answer ourselves.
	//
	// While a request is going to Synapse, its own limiter already protects
	// it, and adding ours in front would put that traffic through two limiters
	// in series -- delaying requests Synapse would have served and inflating
	// the upstream latency we measure against.
	//
	// The origin is the one claimed in the X-Matrix header, not a verified
	// one, and the limiter runs before verification on purpose: verifying can
	// require a network key fetch, so a limiter that ran after it could be
	// used to generate outbound load. Synapse limits on the claimed origin for
	// the same reason.
	if serveNatively {
		release, outcome, err := h.limiter.Acquire(r.Context(), origin)
		if err != nil {
			if errors.Is(err, ratelimit.ErrLimitExceeded) {
				metrics.RateLimitedTotal.WithLabelValues(endpointName).Inc()
				metrics.RequestsTotal.WithLabelValues(endpointName, mode.Kind, "429").Inc()
				metrics.RateLimitedOrigins.Set(float64(h.noteLimited(origin)))
				// Logged at warn with the origin: metrics deliberately carry
				// no per-origin label, so this is where "which server" is
				// answered.
				h.log.Warn().
					Str("endpoint", endpointName).
					Str("origin", origin).
					Str("uri", r.URL.RequestURI()).
					Int("reject_limit", h.limiter.Settings().RejectLimit).
					Int("concurrent", h.limiter.Settings().Concurrent).
					Msg("Rate limited: rejected request")
				writeLimitExceeded(w, h.limiter.Settings())
				return
			}
			// The client went away while queued; there is nobody left to answer.
			metrics.RequestsTotal.WithLabelValues(endpointName, mode.Kind, "canceled").Inc()
			return
		}
		if outcome.Slept || outcome.Queued {
			if outcome.Slept {
				metrics.RateLimitSleptTotal.WithLabelValues(endpointName).Inc()
			}
			h.log.Info().
				Str("endpoint", endpointName).
				Str("origin", origin).
				Bool("slept", outcome.Slept).
				Bool("queued", outcome.Queued).
				Dur("wait_ms", outcome.Wait).
				Msg("Rate limited: delayed request")
		}

		served, reason, spent := h.serveNative(w, r, mode, endpointName, roomID, eventID)
		release()
		if served {
			metrics.RequestsTotal.WithLabelValues(endpointName, mode.Kind, "native").Inc()
			return
		}
		nativeFallbackServed.WithLabelValues(endpointName).Inc()
		fellBack, fallbackReason, nativeSpent = true, reason, spent
	}

	// Shadowing continues through canary, on the share still going to Synapse.
	// Promoting an endpoint is exactly when disagreements matter most, so it
	// would be the wrong moment to stop looking for them.
	var capture int64
	shadowing := (mode.Kind == config.ModeShadow || mode.Kind == config.ModeCanary) && h.shadow != nil
	if shadowing {
		capture = h.captureLimit
	}

	res := h.proxy.Forward(w, r, capture)

	// The client has its answer by this point; comparison happens afterwards
	// and never delays a response.
	if shadowing && !res.Canceled && res.Err == nil {
		h.shadow.Go(
			shadow.Request{
				Endpoint: string(route.Endpoint),
				Origin:   origin,
				RoomID:   roomIDFor(route),
				EventID:  eventIDFor(route, r),
				URI:      r.URL.RequestURI(),
				Method:   r.Method,
				// Kept so verification can be replayed off the request path.
				AuthHeader: r.Header.Get("Authorization"),
			},
			shadow.ProxyResult{
				Status:    res.Status,
				Body:      res.Body,
				Duration:  res.Duration,
				Truncated: res.Truncated,
			},
		)
	}

	endpoint := string(route.Endpoint)
	if fellBack {
		fallbackUpstream.WithLabelValues(endpoint, fallbackReason, statusLabel(res)).Inc()
		fallbackTotalDuration.WithLabelValues(endpoint, fallbackReason).
			Observe((nativeSpent + res.Duration).Seconds())
	}
	metrics.RequestsTotal.WithLabelValues(endpoint, mode.Kind, statusLabel(res)).Inc()
	metrics.UpstreamDuration.WithLabelValues(endpoint).Observe(res.Duration.Seconds())
	metrics.ResponseBytes.WithLabelValues(endpoint).Observe(float64(res.Bytes))
	if res.Err != nil {
		metrics.UpstreamErrorsTotal.WithLabelValues(endpoint, res.Backend).Inc()
	}

	ev := h.log.Info()
	switch {
	case res.Err != nil:
		ev = h.log.Error().Err(res.Err)
	case res.Canceled:
		// Remote servers hang up on slow /state requests routinely; this is
		// worth seeing at debug level, not as noise in the request log.
		ev = h.log.Debug()
	}
	ev.
		Str("endpoint", endpoint).
		Str("param", route.Param).
		Str("mode", mode.String()).
		Str("backend", res.Backend).
		Str("status", statusLabel(res)).
		Int64("bytes", res.Bytes).
		Dur("upstream_ms", res.Duration).
		Dur("total_ms", time.Since(start)).
		Str("origin", originFromAuth(r.Header.Get("Authorization"))).
		Msg("Served federation request")
}

// statusLabel renders the outcome for logs and metrics. A cancelled request
// never received a status, so reporting 0 would create a meaningless metric
// series; it gets its own label instead.
func statusLabel(res proxy.Result) string {
	if res.Canceled {
		return "canceled"
	}
	if res.Status == 0 {
		return "none"
	}
	return strconv.Itoa(res.Status)
}

// roomIDFor returns the decoded room ID for the endpoints that take one.
func roomIDFor(route Route) string {
	if route.Endpoint == EndpointEvent {
		return ""
	}
	return shadow.DecodeParam(route.Param)
}

// eventIDFor returns the decoded event ID, which is the path parameter for
// /event and the required query parameter for the state endpoints.
func eventIDFor(route Route, r *http.Request) string {
	if route.Endpoint == EndpointEvent {
		return shadow.DecodeParam(route.Param)
	}
	return r.URL.Query().Get("event_id")
}

func (h *Handler) modeFor(e Endpoint) config.Mode {
	switch e {
	case EndpointEvent:
		return h.cfg.Endpoints.Event
	case EndpointState:
		return h.cfg.Endpoints.State
	case EndpointStateIDs:
		return h.cfg.Endpoints.StateIDs
	default:
		return config.Mode{Kind: config.ModeProxy}
	}
}
