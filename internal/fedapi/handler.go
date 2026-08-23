package fedapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/metrics"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
)

// Handler serves the three federation read endpoints.
//
// Every request is currently forwarded to a Synapse worker. The per-endpoint
// mode already threads through so that shadow and native serving can be added
// without reshaping the request path.
type Handler struct {
	cfg   *config.Config
	proxy *proxy.Proxy
	log   zerolog.Logger
}

// New builds a Handler.
func New(cfg *config.Config, p *proxy.Proxy, log zerolog.Logger) *Handler {
	return &Handler{cfg: cfg, proxy: p, log: log}
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

	// Phase 1: everything is proxied regardless of mode. Modes beyond "proxy"
	// are accepted by config but not yet acted on.
	res := h.proxy.Forward(w, r, 0)

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
