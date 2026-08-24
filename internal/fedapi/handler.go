package fedapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/metrics"
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

	// captureLimit is how much of the proxied body to retain for comparison.
	// A /state_ids response for a large room runs to several megabytes, so this
	// is generous; bodies past it are not compared rather than compared wrongly.
	captureLimit int64
}

// New builds a Handler. runner may be nil, in which case nothing is shadowed.
func New(cfg *config.Config, p *proxy.Proxy, runner *shadow.Runner, log zerolog.Logger) *Handler {
	limit := int64(cfg.Shadow.CaptureMB) << 20
	if limit <= 0 {
		limit = 32 << 20
	}
	return &Handler{
		cfg:          cfg,
		proxy:        p,
		shadow:       runner,
		limiter:      ratelimit.New(cfg.RCFederation),
		log:          log,
		captureLimit: limit,
	}
}

// Limiter exposes the rate limiter for metrics and maintenance.
func (h *Handler) Limiter() *ratelimit.Limiter { return h.limiter }

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

	// Rate limit per origin, as Synapse does. The origin here is the one
	// claimed in the X-Matrix header rather than a verified one: verification
	// costs a network key fetch, and a limiter that had to do that first could
	// itself be used to generate load. Synapse limits on the claimed origin for
	// the same reason.
	origin := originFromAuth(r.Header.Get("Authorization"))
	release, err := h.limiter.Acquire(r.Context(), origin)
	if err != nil {
		if errors.Is(err, ratelimit.ErrLimitExceeded) {
			metrics.RateLimitedTotal.WithLabelValues(string(route.Endpoint)).Inc()
			metrics.RequestsTotal.WithLabelValues(string(route.Endpoint), mode.Kind, "429").Inc()
			h.log.Debug().
				Str("endpoint", string(route.Endpoint)).
				Str("origin", origin).
				Msg("Rejected by rate limit")
			writeLimitExceeded(w, h.limiter.Settings())
			return
		}
		// The client went away while queued; there is nobody left to answer.
		metrics.RequestsTotal.WithLabelValues(string(route.Endpoint), mode.Kind, "canceled").Inc()
		return
	}
	defer release()

	// Shadow mode still serves Synapse's answer; it only needs the body kept
	// so the comparison has something to check against.
	var capture int64
	shadowing := mode.Kind == config.ModeShadow && h.shadow != nil
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
