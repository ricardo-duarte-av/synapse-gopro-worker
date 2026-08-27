package fedapi

import (
	"context"
	"errors"
	"hash/fnv"
	"net/http"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/native"
	"github.com/daedric/synapse-gopro-worker/internal/shadow"
)

// sampled decides whether this request is one of the canary's share.
//
// The choice is a hash of the event ID rather than a random draw, so the same
// event always gets the same implementation. A server that retries — and
// federation retries constantly — then sees one consistent answer instead of
// alternating between two, which would turn any disagreement into a
// heisenbug and make a canary far harder to reason about than a coin flip
// suggests.
//
// Native mode serves everything; proxy and shadow serve nothing.
func sampled(mode config.Mode, key string) bool {
	switch mode.Kind {
	case config.ModeNative:
		return true
	case config.ModeCanary:
		if mode.CanaryPercent <= 0 {
			return false
		}
		if mode.CanaryPercent >= 100 {
			return true
		}
		if key == "" {
			// Without a stable key there is no stable answer, and an unstable
			// one is worse than not participating.
			return false
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		return h.Sum64()%100 < uint64(mode.CanaryPercent)
	default:
		return false
	}
}

// nativeResult reports what the native path did.
//
// Status and Bytes exist so a natively served request can be logged like any
// other. They have no meaning unless Served is true.
type nativeResult struct {
	Served bool
	// Reason is why we fell back, when Served is false.
	Reason string
	Spent  time.Duration
	Status int
	Bytes  int64
}

// serveNative answers a request from our own implementation.
//
// It reports whether the request was answered. When it returns false nothing
// has been written and the caller must fall back to the proxy, which is the
// whole safety property of a canary: any doubt costs latency, never
// correctness. Nothing is written until a complete answer exists, so a failure
// halfway through cannot leave a half-written response.
func (h *Handler) serveNative(w http.ResponseWriter, r *http.Request, mode config.Mode, endpoint, roomID, eventID string) nativeResult {
	if h.resolver == nil {
		return nativeResult{Reason: "no_resolver"}
	}
	attemptStart := time.Now()

	// Verification happens on the real request here, not on a replay. A canary
	// answer is served, so an unverified origin would be reading room state it
	// may have no right to.
	//
	// The caller has already passed the rate limiter, deliberately before this
	// point: verification can require a network key fetch, and doing that first
	// would let unverifiable requests generate outbound load without ever being
	// limited. Acquiring it again here would deadlock against the slot the
	// caller is holding.
	//
	// A rejection falls back to the proxy rather than being served. Our
	// verifier has been wrong in both directions before, and Synapse verifies
	// independently — so falling back cannot let anything through that Synapse
	// would refuse, while serving our rejection could break legitimate
	// federation over a bug of ours.
	result := h.verifier.Verify(r)
	if !result.OK() {
		nativeFallback.WithLabelValues(endpoint, "auth_rejected").Inc()
		return nativeResult{Reason: "auth_rejected", Spent: time.Since(attemptStart)}
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.nativeTimeout)
	defer cancel()

	start := time.Now()
	body, status, err := h.answer(ctx, endpoint, result.Origin, roomID, eventID)
	elapsed := time.Since(start)

	if err != nil {
		// Distinguish our deadline expiring from the client hanging up. The
		// context carries both -- it is derived from the request's -- and
		// conflating them reports a remote server's disconnect as a timeout of
		// ours. On this deployment that is the common case, not the rare one:
		// Tuwunel-style servers hang up on /state_ids constantly.
		reason := "error"
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			reason = "timeout"
		case errors.Is(ctx.Err(), context.Canceled):
			reason = "client_gone"
		}
		nativeFallback.WithLabelValues(endpoint, reason).Inc()
		ev := h.log.Warn()
		if reason == "client_gone" {
			// Not a defect: nobody is left to answer.
			ev = h.log.Debug()
		}
		ev.Err(err).
			Str("endpoint", endpoint).Str("origin", result.Origin).
			Str("reason", reason).
			Dur("took", elapsed).
			Msg("Native answer not served; falling back to Synapse")
		return nativeResult{Reason: reason, Spent: time.Since(attemptStart)}
	}

	nativeDuration.WithLabelValues(endpoint).Observe(elapsed.Seconds())
	nativeServed.WithLabelValues(endpoint, statusText(status)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)

	// A canary verifies what it served; native does not.
	//
	// While an endpoint is proving itself, every served answer is replayed to
	// Synapse afterwards and compared -- a canary that only checked the share
	// it did not serve would verify exactly the wrong requests.
	//
	// Native is the end of that process. Verifying there would mean Synapse
	// still resolving every state group and running every recursive
	// state_groups_state walk, so its database load would be unchanged no
	// matter how much traffic we answered -- and that load is the whole point
	// of the project, not the latency. Promotion is the decision to stop
	// paying for a second opinion; an endpoint that still needs one is a
	// canary, whatever the config calls it.
	//
	// Note this is what makes native the first mode in which Synapse can fall
	// behind us unnoticed: nothing downstream of here compares anything. The
	// evidence has to be gathered before promotion, because afterwards it
	// stops arriving.
	if mode.Kind == config.ModeCanary {
		h.compareServed(r, endpoint, roomID, eventID, body, status, elapsed)
	}
	return nativeResult{
		Served: true,
		Spent:  time.Since(attemptStart),
		Status: status,
		Bytes:  int64(len(body)),
	}
}

// compareServed asks Synapse what it would have answered, and records any
// disagreement the same way shadow mode does.
//
// The request is cloned onto a detached context: the original is cancelled the
// moment the handler returns, and this deliberately runs after that.
func (h *Handler) compareServed(r *http.Request, endpoint, roomID, eventID string, body []byte, status int, elapsed time.Duration) {
	if h.shadow == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), h.verifyTimeout)
	replay := r.Clone(ctx)
	replay.Body = http.NoBody

	go func() {
		defer cancel()
		defer func() {
			if p := recover(); p != nil {
				h.log.Error().Interface("panic", p).Str("endpoint", endpoint).
					Msg("Canary comparison panicked")
			}
		}()
		res := h.proxy.Fetch(replay, h.captureLimit)
		h.shadow.CompareServed(
			shadow.Request{
				Endpoint:   endpoint,
				Origin:     originFromAuth(r.Header.Get("Authorization")),
				RoomID:     roomID,
				EventID:    eventID,
				URI:        r.URL.RequestURI(),
				Method:     r.Method,
				AuthHeader: r.Header.Get("Authorization"),
			},
			shadow.ProxyResult{
				Status:    res.Status,
				Body:      res.Body,
				Duration:  res.Duration,
				Truncated: res.Truncated,
			},
			elapsed, body, status,
		)
	}()
}

// answer computes the response, converting a panic into an error so a bug in
// the native path falls back to the proxy rather than killing the connection.
func (h *Handler) answer(ctx context.Context, endpoint, origin, roomID, eventID string) (body []byte, status int, err error) {
	defer func() {
		if p := recover(); p != nil {
			nativeFallback.WithLabelValues(endpoint, "panic").Inc()
			h.log.Error().Interface("panic", p).Str("endpoint", endpoint).
				Msg("Native answer panicked; falling back to Synapse")
			body, status, err = nil, 0, errPanicked
		}
	}()
	return native.Answer(ctx, h.resolver, h.serverName, native.Request{
		Endpoint: endpoint,
		Origin:   origin,
		RoomID:   roomID,
		EventID:  eventID,
	})
}

// errPanicked marks a recovered panic, so the caller falls back to the proxy
// rather than treating it as a real answer.
var errPanicked = errors.New("native answer panicked")

func statusText(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "2xx"
	case status == http.StatusNotFound:
		return "404"
	case status == http.StatusForbidden:
		return "403"
	case status == http.StatusUnauthorized:
		return "401"
	default:
		return "other"
	}
}
