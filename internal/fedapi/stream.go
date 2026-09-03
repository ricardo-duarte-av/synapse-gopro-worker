package fedapi

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

// lazyResponse defers the status line until the first body byte, and holds the
// idle write deadline that bounds a stalled client.
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
	w    http.ResponseWriter
	rc   *http.ResponseController
	idle time.Duration

	// noDeadline records that this writer cannot carry a write deadline, so
	// the idle guarantee does not apply and only the absolute cap does. Test
	// recorders are the usual case; a real connection supports it.
	noDeadline bool
	// refreshAt throttles deadline updates. /state writes twice per event --
	// 290k syscalls on the largest room here -- and the deadline only needs to
	// stay ahead of the write, not be exact.
	refreshAt time.Time

	written bool
	bytes   int64
}

func (l *lazyResponse) Write(p []byte) (int, error) {
	if !l.written {
		l.w.Header().Set("Content-Type", "application/json")
		l.w.WriteHeader(http.StatusOK)
		l.written = true
	}
	l.refreshDeadline()
	n, err := l.w.Write(p)
	l.bytes += int64(n)
	return n, err
}

// refreshDeadline pushes the write deadline out by the idle budget.
//
// This is the idle timeout, and it is deliberately enforced here rather than
// by a watchdog goroutine cancelling the context. Cancelling a context does
// not interrupt a Write that is already blocked in the kernel, so a client
// that stops reading entirely would hang the goroutine no matter what the
// context said. A write deadline is the only thing that unblocks it.
func (l *lazyResponse) refreshDeadline() {
	if l.idle <= 0 || l.rc == nil || l.noDeadline {
		return
	}
	now := time.Now()
	if now.Before(l.refreshAt) {
		return
	}
	if err := l.rc.SetWriteDeadline(now.Add(l.idle)); err != nil {
		l.noDeadline = true
		return
	}
	l.refreshAt = now.Add(l.idle / 4)
}

// clearDeadline removes the deadline once the body is complete, so a finished
// response does not leave one set on a connection that will be reused.
func (l *lazyResponse) clearDeadline() {
	if l.rc == nil || l.noDeadline {
		return
	}
	_ = l.rc.SetWriteDeadline(time.Time{})
}

// serveStreamed answers an endpoint that writes its response incrementally.
//
// Returns the same nativeResult as serveNative, so the caller cannot tell the
// two apart -- except that a streamed answer which failed after committing
// never returns at all: it aborts the connection. See below.
//
// # Two budgets, because "we are slow" and "the client is slow" are different
//
// A single total-duration budget conflates them, and cut the honest slow
// reader. Measured live on 2026-09-03: one peer drained an 18.8MB response at
// 23 KB/s -- it needed thirteen minutes and was making steady progress the
// whole time -- and the 120s cap truncated it at 3.06MB. The same room went
// out complete in 3.5s to the same peer four seconds later. A second peer, on
// a larger payload, has never once been slow. The variable is the client's
// drain rate, which is not ours to bound.
//
//	idle:     time with no progress. The real control. A stalled client is cut
//	          promptly -- faster than the old total cap -- and a slow one is
//	          left alone.
//	absolute: a backstop against a stream that progresses forever. It should
//	          never bind on a real transfer; at the slowest rate yet observed
//	          the largest organic room needs ~14 minutes.
func (h *Handler) serveStreamed(w http.ResponseWriter, r *http.Request, endpoint, roomID, eventID, origin string, attemptStart time.Time) nativeResult {
	ctx, cancel := context.WithTimeout(r.Context(), h.streamTimeout)
	defer cancel()

	out := &lazyResponse{
		w:    w,
		rc:   http.NewResponseController(w),
		idle: h.streamIdleTimeout,
	}
	start := time.Now()
	res, err := h.resolver.State(ctx, out, origin, roomID, eventID)
	elapsed := time.Since(start)

	if err != nil {
		if out.written {
			reason, ev := h.classifyCommitted(ctx, err)
			nativeFallback.WithLabelValues(endpoint, reason).Inc()
			ev.Err(err).
				Str("endpoint", endpoint).Str("origin", origin).Str("room_id", roomID).
				Str("reason", reason).
				Int64("bytes_written", out.bytes).Dur("took", elapsed).
				Msg("Streamed answer ended after the response had begun")

			// Committed, and the body is incomplete. Returning normally would
			// let net/http finish the chunked stream cleanly, and the peer
			// would receive a *valid-looking* 200 carrying truncated JSON --
			// silent corruption it cannot detect or retry. Aborting kills the
			// connection without a terminating chunk, which is a transfer
			// error the peer sees and retries.
			//
			// This is why the log line above is emitted here rather than left
			// to the shared request log: the panic unwinds past it.
			//
			// A write that failed on the idle deadline has already poisoned
			// the connection, so this is belt and braces there. It is load
			// bearing for the absolute cap, where writes were still
			// succeeding -- which is exactly the case observed live.
			panic(http.ErrAbortHandler)
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

	out.clearDeadline()
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

// classifyCommitted names why a committed stream ended, and at what level to
// say so.
//
// The three causes are genuinely different and were previously collapsed into
// one. A remote server hanging up mid-stream is not a defect and must not be
// reported as one -- it is routine here, since a /state response runs to a
// hundred megabytes and takes tens of seconds, so a client giving up part-way
// is far more likely than on any other endpoint.
func (h *Handler) classifyCommitted(ctx context.Context, err error) (string, *zerolog.Event) {
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		// The client stopped accepting bytes for the whole idle budget. Its
		// problem, not ours, but worth seeing: it is also what a wedged peer
		// looks like.
		//
		// Checked *before* the context, and that ordering is load bearing.
		// Breaking the connection on a write deadline makes net/http cancel
		// the request context, so by the time we look, ctx.Err() is Canceled
		// and a stall would be misreported as the client hanging up -- which
		// is the quieter of the two and logs at debug. The write error is the
		// more specific signal: we set that deadline ourselves.
		return "stream_stalled", h.log.Warn()
	case errors.Is(ctx.Err(), context.Canceled):
		// The remote server hung up. Routine.
		return "client_gone", h.log.Debug()
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		// The absolute backstop fired while the transfer was still
		// progressing. This should be rare enough to investigate: either the
		// cap is too tight for a legitimately huge room, or something is
		// crawling.
		return "timeout", h.log.Warn()
	default:
		// A database error mid-walk, or a serialisation failure. Ours.
		return "stream_aborted", h.log.Error()
	}
}
