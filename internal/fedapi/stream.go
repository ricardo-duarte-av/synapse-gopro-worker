package fedapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

// lazyResponse defers the status line until the first body byte.
//
// This is what preserves the fallback guarantee for a streamed answer. Every
// other endpoint builds its response in memory and writes nothing until a
// complete answer exists, so any failure can fall through to the proxy. /state
// cannot: its responses reach 97MB and buffering one would recreate, on the
// request path, the problem the endpoint was written to solve.
//
// So the guarantee is narrowed rather than abandoned. Resolver.State performs
// every check that can produce a Matrix error -- partial state, host in room,
// server ACL, the event being unknown or an outlier -- before it writes a
// byte. While nothing has been written, a failure is indistinguishable from
// any other endpoint's and falls back cleanly. Once the first byte is out we
// are committed, and the remaining failure modes are database errors mid-walk,
// which no fallback could paper over anyway.
type lazyResponse struct {
	w       http.ResponseWriter
	written bool
	bytes   int64
}

func (l *lazyResponse) Write(p []byte) (int, error) {
	if !l.written {
		l.w.Header().Set("Content-Type", "application/json")
		l.w.WriteHeader(http.StatusOK)
		l.written = true
	}
	n, err := l.w.Write(p)
	l.bytes += int64(n)
	return n, err
}

// serveStreamed answers an endpoint that writes its response incrementally.
//
// Returns the same nativeResult as serveNative, so the caller cannot tell the
// two apart -- except that a streamed answer which failed after committing is
// reported as served, because it was: bytes reached the client and the proxy
// must not append to them.
func (h *Handler) serveStreamed(w http.ResponseWriter, r *http.Request, endpoint, roomID, eventID, origin string, attemptStart time.Time) nativeResult {
	ctx, cancel := context.WithTimeout(r.Context(), h.streamTimeout)
	defer cancel()

	out := &lazyResponse{w: w}
	start := time.Now()
	res, err := h.resolver.State(ctx, out, origin, roomID, eventID)
	elapsed := time.Since(start)

	if err != nil {
		if out.written {
			// Committed. The client has a partial body and will retry; the one
			// thing we must not do is hand the request to the proxy, which
			// would append a second response to the first.
			nativeFallback.WithLabelValues(endpoint, "stream_aborted").Inc()
			h.log.Error().Err(err).
				Str("endpoint", endpoint).Str("origin", origin).Str("room_id", roomID).
				Int64("bytes_written", out.bytes).Dur("took", elapsed).
				Msg("Streamed answer failed after the response had begun; it cannot be retried here")
			return nativeResult{Served: true, Status: http.StatusOK, Bytes: out.bytes, Spent: time.Since(attemptStart)}
		}

		// Nothing written: behave exactly like any other endpoint.
		var me *matrixstate.MatrixError
		if errors.As(err, &me) {
			body, status := me.Response()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(body)
			return nativeResult{Served: true, Status: status, Bytes: int64(len(body)), Spent: time.Since(attemptStart)}
		}

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
			ev = h.log.Debug()
		}
		ev.Err(err).Str("endpoint", endpoint).Str("origin", origin).
			Str("reason", reason).Dur("took", elapsed).
			Msg("Streamed answer not served; falling back to Synapse")
		return nativeResult{Reason: reason, Spent: time.Since(attemptStart)}
	}

	nativeDuration.WithLabelValues(endpoint).Observe(elapsed.Seconds())
	nativeServed.WithLabelValues(endpoint, "2xx").Inc()

	return nativeResult{
		Served:      true,
		Status:      http.StatusOK,
		Bytes:       res.Bytes,
		Spent:       time.Since(attemptStart),
		StateResult: &res,
	}
}
