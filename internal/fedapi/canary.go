package fedapi

import (
	"context"
	"errors"
	"hash/fnv"
	"net/http"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/native"
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

// serveNative answers a request from our own implementation.
//
// It reports whether the request was answered. When it returns false nothing
// has been written and the caller must fall back to the proxy, which is the
// whole safety property of a canary: any doubt costs latency, never
// correctness. Nothing is written until a complete answer exists, so a failure
// halfway through cannot leave a half-written response.
func (h *Handler) serveNative(w http.ResponseWriter, r *http.Request, endpoint, roomID, eventID string) bool {
	if h.resolver == nil {
		return false
	}

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
		return false
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.nativeTimeout)
	defer cancel()

	start := time.Now()
	body, status, err := h.answer(ctx, endpoint, result.Origin, roomID, eventID)
	elapsed := time.Since(start)

	if err != nil {
		reason := "error"
		if ctx.Err() != nil {
			reason = "timeout"
		}
		nativeFallback.WithLabelValues(endpoint, reason).Inc()
		h.log.Warn().Err(err).
			Str("endpoint", endpoint).Str("origin", result.Origin).
			Dur("took", elapsed).
			Msg("Native answer failed; falling back to Synapse")
		return false
	}

	nativeDuration.WithLabelValues(endpoint).Observe(elapsed.Seconds())
	nativeServed.WithLabelValues(endpoint, statusText(status)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	return true
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
