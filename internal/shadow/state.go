package shadow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/difflog"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

// StateSink digests a /state response as it streams past on its way to the
// client.
//
// The other endpoints are compared by capturing Synapse's body and diffing it.
// /state cannot be: the largest response here is about 97MB, and buffering one
// would recreate the memory problem the endpoint is being rewritten to avoid.
// Instead the bytes are folded into a digest as they are written, so
// comparison costs no extra memory, no extra upstream request, and no latency.
//
// The reader always drains to EOF, including after a parse error. Without that
// a malformed response would leave the proxy blocked mid-write on a pipe
// nobody was reading -- a comparison stalling a live response, which is the
// one thing shadow work must never do.
type StateSink struct {
	pw   *io.PipeWriter
	done chan struct{}
	res  matrixstate.StateResult
	err  error
}

// NewStateSink starts digesting in the background.
func NewStateSink() *StateSink {
	pr, pw := io.Pipe()
	s := &StateSink{pw: pw, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		// Drain whatever is left so the writer can always finish, whether the
		// parse succeeded, failed, or stopped early.
		defer func() {
			_, _ = io.Copy(io.Discard, pr)
			_ = pr.Close()
		}()
		s.res, s.err = matrixstate.DigestStateResponse(pr)
	}()
	return s
}

// Writer is the sink to hand to the proxy.
func (s *StateSink) Writer() io.Writer { return s.pw }

// Wait closes the stream and returns what Synapse sent.
func (s *StateSink) Wait() (matrixstate.StateResult, error) {
	_ = s.pw.Close()
	<-s.done
	return s.res, s.err
}

// GoState compares a /state response against our own, by digest.
//
// Unlike Go, this is handed Synapse's answer already reduced to a digest,
// because it was never held whole. Everything else follows the same rules:
// dropped rather than queued when busy, and a failure to compare is recorded
// as a skip rather than as agreement.
func (r *Runner) GoState(req Request, proxy ProxyResult, upstream matrixstate.StateResult, upstreamErr error) {
	if r == nil {
		return
	}
	select {
	case r.sem <- struct{}{}:
	default:
		shadowSkipped.WithLabelValues(req.Endpoint, "busy").Inc()
		return
	}

	go func() {
		defer func() { <-r.sem }()
		defer func() {
			if p := recover(); p != nil {
				shadowSkipped.WithLabelValues(req.Endpoint, "panic").Inc()
				r.log.Error().Interface("panic", p).
					Str("endpoint", req.Endpoint).Str("uri", req.URI).
					Msg("State comparison panicked")
			}
		}()
		r.compareState(req, proxy, upstream, upstreamErr)
	}()
}

func (r *Runner) compareState(req Request, proxy ProxyResult, upstream matrixstate.StateResult, upstreamErr error) {
	// A response we could not parse tells us nothing about whether our answer
	// was right. Counted as a failure to compare, never as a match: with a
	// digest there is no partial credit.
	if upstreamErr != nil {
		shadowSkipped.WithLabelValues(req.Endpoint, "upstream_undigestible").Inc()
		r.log.Warn().Err(upstreamErr).
			Str("endpoint", req.Endpoint).Str("uri", req.URI).
			Msg("Could not digest Synapse's /state response")
		return
	}
	if proxy.Status == 0 || proxy.Status == http.StatusBadGateway {
		shadowSkipped.WithLabelValues(req.Endpoint, "upstream_unavailable").Inc()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	ctx = r.log.WithContext(ctx)

	// Same order as run(): verification is exercised alongside the state
	// logic, and a request we would reject has that rejection as its answer.
	origin, authErr := r.verifyOrigin(req, proxy)
	if authErr != nil {
		start := time.Now()
		body, _ := json.Marshal(authErr)
		r.finish(req, proxy, time.Since(start), body, authErr.Status)
		return
	}

	start := time.Now()
	ours, err := r.resolver.State(ctx, io.Discard, origin, req.RoomID, req.EventID)
	elapsed := time.Since(start)

	if err != nil {
		// A Matrix error is an answer, and its status is comparable in exactly
		// the same way as any other endpoint's -- including the reclassifications
		// finish applies for a Synapse that could not fetch keys or shed load.
		body, status, internal := matrixErrorResponse(err)
		if internal != nil {
			kind := difflog.KindNativeError
			if errors.Is(internal, context.DeadlineExceeded) {
				kind = difflog.KindNativeTimeout
			}
			r.record(req, proxy, elapsed, &difflog.Record{
				Kind:        kind,
				NativeError: internal.Error(),
			})
			return
		}
		r.finish(req, proxy, elapsed, body, status)
		return
	}

	shadowDuration.WithLabelValues(req.Endpoint).Observe(elapsed.Seconds())
	shadowUpstreamDuration.WithLabelValues(req.Endpoint).Observe(proxy.Duration.Seconds())

	if proxy.Status != http.StatusOK {
		// We produced an answer where Synapse produced an error status. There
		// is no body of ours to log -- it went to the client, not to memory.
		r.finish(req, proxy, elapsed, nil, http.StatusOK)
		return
	}

	if ours.Agrees(upstream) {
		r.match(req)
		return
	}

	// A digest says only that the two differ, never which event. Logging both
	// digests and both counts is what makes the next step decidable without a
	// replay: equal counts with unequal digests points at content, unequal
	// counts at membership of the set.
	r.log.Warn().
		Str("endpoint", req.Endpoint).Str("uri", req.URI).Str("origin", origin).
		Int("synapse_pdus", upstream.PDUs).Int("native_pdus", ours.PDUs).
		Int("synapse_auth_chain", upstream.AuthChain).Int("native_auth_chain", ours.AuthChain).
		Str("synapse_pdu_digest", shortDigest(upstream.PDUDigest)).
		Str("native_pdu_digest", shortDigest(ours.PDUDigest)).
		Str("synapse_auth_digest", shortDigest(upstream.AuthChainDigest)).
		Str("native_auth_digest", shortDigest(ours.AuthChainDigest)).
		Int64("native_bytes", ours.Bytes).
		Msg("/state digests disagree")

	r.record(req, proxy, elapsed, &difflog.Record{
		Kind:         difflog.KindBody,
		NativeStatus: http.StatusOK,
		Diff:         stateDiff(ours, upstream),
	})
}

// stateDiff reports what a digest can honestly say.
//
// The event ID lists other endpoints fill in are deliberately empty: neither
// side was ever held in memory, so nothing here knows which events differed.
// The counts still separate the two useful cases -- a set of the wrong size
// from a set of the right size with the wrong contents -- which is enough to
// choose what to do next.
func stateDiff(ours, upstream matrixstate.StateResult) *difflog.Diff {
	var fields []difflog.FieldDiff
	if ours.PDUs != upstream.PDUs || ours.PDUDigest != upstream.PDUDigest {
		fields = append(fields, difflog.FieldDiff{
			Field:        "pdus",
			SynapseCount: upstream.PDUs,
			NativeCount:  ours.PDUs,
		})
	}
	if ours.AuthChain != upstream.AuthChain || ours.AuthChainDigest != upstream.AuthChainDigest {
		fields = append(fields, difflog.FieldDiff{
			Field:        "auth_chain",
			SynapseCount: upstream.AuthChain,
			NativeCount:  ours.AuthChain,
		})
	}
	return &difflog.Diff{Fields: fields}
}

// shortDigest renders enough of a digest to tell two apart in a log line.
func shortDigest(b [32]byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 16)
	for _, c := range b[:8] {
		out = append(out, digits[c>>4], digits[c&0x0f])
	}
	return string(out)
}
