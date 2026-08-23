package shadow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/difflog"
	"github.com/daedric/synapse-gopro-worker/internal/fedauth"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

// Request describes the federation request being shadowed.
type Request struct {
	Endpoint string
	// Origin is the server that made the request, taken from the X-Matrix
	// header. It is NOT verified here: Synapse verified it before answering,
	// and returning 200 proves as much. Re-verifying would duplicate remote key
	// traffic and turn key-fetch failures into mismatches that are not logic
	// bugs. Native serving must verify for itself.
	Origin string
	// RoomID and EventID are already percent-decoded.
	RoomID  string
	EventID string
	URI     string
	// Method and AuthHeader let verification be replayed off the request path.
	// Verification fetches the origin server's keys over the network, so doing
	// it inline would delay the response we are supposed to be observing.
	Method     string
	AuthHeader string
}

// ProxyResult is what Synapse answered.
type ProxyResult struct {
	Status   int
	Body     []byte
	Duration time.Duration
	// Truncated reports that the captured body was cut short, in which case it
	// cannot be compared.
	Truncated bool
}

// matrixErrorResponse turns a Matrix error into the body and status it would
// be served with, leaving anything else as an internal failure.
func matrixErrorResponse(err error) ([]byte, int, error) {
	var me *matrixstate.MatrixError
	if errors.As(err, &me) {
		body, mErr := json.Marshal(me)
		if mErr != nil {
			return nil, 0, mErr
		}
		return body, me.Status, nil
	}
	return nil, 0, err
}

// StateIDsResolver computes native /state_ids answers.
//
// The Runner depends on this narrow interface rather than the concrete
// resolver so its scheduling behaviour — dropping work when busy, surviving
// panics, never blocking a request — can be tested without a database.
type StateIDsResolver interface {
	StateIDs(ctx context.Context, origin, roomID, eventID string) (*matrixstate.StateIDsResponse, error)
	Event(ctx context.Context, origin, serverName, eventID string) (*matrixstate.TransactionResponse, error)
}

// Runner computes native answers alongside proxied ones and records
// disagreements.
type Runner struct {
	resolver   StateIDsResolver
	serverName string
	verifier   *fedauth.Verifier
	diffs      *difflog.Writer
	log        zerolog.Logger

	// timeout bounds one native computation. Shadow work is best-effort: it
	// must never outlive its usefulness or pile up.
	timeout time.Duration
	// sem bounds concurrent native computations so that shadow work cannot
	// exhaust the database pool that real traffic will eventually depend on.
	sem chan struct{}
}

// Options configures a Runner.
type Options struct {
	// Timeout bounds one native computation. Zero uses 30s.
	Timeout time.Duration
	// Concurrency bounds simultaneous native computations. Zero uses 4.
	Concurrency int
}

// NewRunner builds a Runner. verifier may be nil, in which case X-Matrix
// verification is not exercised.
func NewRunner(resolver StateIDsResolver, serverName string, verifier *fedauth.Verifier, diffs *difflog.Writer, log zerolog.Logger, opts Options) *Runner {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	return &Runner{
		resolver:   resolver,
		serverName: serverName,
		verifier:   verifier,
		diffs:      diffs,
		log:        log,
		timeout:    opts.Timeout,
		sem:        make(chan struct{}, opts.Concurrency),
	}
}

// Go schedules a shadow comparison and returns immediately.
//
// The comparison runs after the client has already been answered, so it is
// deliberately detached from the request context: that context is cancelled the
// moment the handler returns. If the worker is saturated the comparison is
// dropped rather than queued, because a stale comparison is worth less than a
// responsive worker.
func (r *Runner) Go(req Request, proxy ProxyResult) {
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
			// A panic in shadow code must never take down a worker that is
			// serving real traffic correctly via the proxy.
			if p := recover(); p != nil {
				shadowSkipped.WithLabelValues(req.Endpoint, "panic").Inc()
				r.log.Error().Interface("panic", p).
					Str("endpoint", req.Endpoint).Str("uri", req.URI).
					Msg("Shadow comparison panicked")
			}
		}()
		r.run(req, proxy)
	}()
}

func (r *Runner) run(req Request, proxy ProxyResult) {
	// Detached from the request context, which is already cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	ctx = r.log.WithContext(ctx)

	// Exercise verification before computing, so the auth layer accumulates
	// evidence at the same time as the state logic. Its verdict is recorded
	// separately and does not affect the state match rate.
	origin := r.verifyOrigin(req, proxy)

	start := time.Now()
	nativeBody, nativeStatus, err := r.compute(ctx, req, origin)
	elapsed := time.Since(start)

	// Record both sides over the same set of requests, so the two are
	// legitimately comparable.
	shadowDuration.WithLabelValues(req.Endpoint).Observe(elapsed.Seconds())
	shadowUpstreamDuration.WithLabelValues(req.Endpoint).Observe(proxy.Duration.Seconds())

	if err != nil {
		// An internal failure is a finding in its own right: it means the
		// native path would have had to fall back to the proxy.
		kind := difflog.KindNativeError
		if errors.Is(err, context.DeadlineExceeded) {
			kind = difflog.KindNativeTimeout
		}
		r.record(req, proxy, elapsed, &difflog.Record{
			Kind:         kind,
			NativeStatus: 0,
			NativeError:  err.Error(),
		})
		return
	}

	agree, compareBodies := CompareStatus(proxy.Status, nativeStatus)
	if !agree {
		r.record(req, proxy, elapsed, &difflog.Record{
			Kind:         difflog.KindStatus,
			NativeStatus: nativeStatus,
			NativeBody:   nativeBody,
		})
		return
	}

	if !compareBodies {
		// Both sides returned the same non-200; nothing further to check.
		r.match(req)
		return
	}

	if proxy.Truncated {
		// We cannot judge a comparison against a body we only partly captured,
		// and guessing would corrupt the match rate that gates promotion.
		shadowSkipped.WithLabelValues(req.Endpoint, "truncated").Inc()
		return
	}

	diff, err := r.diff(req.Endpoint, proxy.Body, nativeBody)
	if err != nil {
		r.record(req, proxy, elapsed, &difflog.Record{
			Kind:         difflog.KindNativeError,
			NativeStatus: nativeStatus,
			NativeError:  err.Error(),
			NativeBody:   nativeBody,
		})
		return
	}
	if diff == nil {
		r.match(req)
		return
	}

	r.record(req, proxy, elapsed, &difflog.Record{
		Kind:         difflog.KindBody,
		NativeStatus: nativeStatus,
		NativeBody:   nativeBody,
		Diff:         diff,
	})
}

// compute produces the native answer, returning the body and the status it
// would have been served with.
// verifyOrigin exercises X-Matrix verification and reports whether our verdict
// agrees with Synapse's, returning the origin to compute with.
//
// While shadowing, Synapse's answer is what gets served, so a disagreement here
// is evidence rather than an outage. It is nonetheless the gate on native
// serving: accepting a request Synapse would have rejected is a security
// failure, not merely a wrong answer.
func (r *Runner) verifyOrigin(req Request, proxy ProxyResult) string {
	if r.verifier == nil {
		return req.Origin
	}

	synth, err := synthesiseRequest(req)
	if err != nil {
		authVerdicts.WithLabelValues("replay_failed").Inc()
		return req.Origin
	}

	result := r.verifier.Verify(synth)
	synapseAccepted := proxy.Status != http.StatusUnauthorized && proxy.Status != http.StatusForbidden

	switch {
	case result.OK() && proxy.Status == http.StatusUnauthorized:
		// We would have accepted what Synapse rejected. This is the dangerous
		// direction and must block promotion.
		authVerdicts.WithLabelValues("we_accept_synapse_rejects").Inc()
		r.logAuthMismatch(req, proxy, result.Origin, "", "we accepted a request Synapse rejected")
	case !result.OK() && synapseAccepted:
		// We would have rejected legitimate traffic.
		authVerdicts.WithLabelValues("we_reject_synapse_accepts").Inc()
		r.logAuthMismatch(req, proxy, "", result.Err.Err, "we rejected a request Synapse accepted")
	case result.OK():
		authVerdicts.WithLabelValues("agree_accept").Inc()
	default:
		authVerdicts.WithLabelValues("agree_reject").Inc()
	}

	if result.OK() {
		return result.Origin
	}
	return req.Origin
}

// synthesiseRequest rebuilds enough of the original request to verify its
// signature. The signature covers the method, the exact request URI and the
// body, and these endpoints are all GET with no body.
func synthesiseRequest(req Request) (*http.Request, error) {
	u, err := url.ParseRequestURI(req.URI)
	if err != nil {
		return nil, err
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	out := &http.Request{
		Method: method,
		URL:    u,
		Header: http.Header{},
		Body:   http.NoBody,
	}
	if req.AuthHeader != "" {
		out.Header.Set("Authorization", req.AuthHeader)
	}
	return out.WithContext(context.Background()), nil
}

func (r *Runner) logAuthMismatch(req Request, proxy ProxyResult, verifiedOrigin, authErr, why string) {
	r.log.Warn().
		Str("endpoint", req.Endpoint).
		Str("claimed_origin", req.Origin).
		Str("verified_origin", verifiedOrigin).
		Str("auth_error", authErr).
		Int("synapse_status", proxy.Status).
		Str("uri", req.URI).
		Msg("X-Matrix verification disagreed with Synapse: " + why)

	// Logged for inspection, but deliberately not passed to Observe: auth
	// agreement is its own gate and must not move the state match rate.
	r.diffs.Log(&difflog.Record{
		Kind:          difflog.KindAuth,
		Endpoint:      req.Endpoint,
		Origin:        req.Origin,
		URI:           req.URI,
		RoomID:        req.RoomID,
		EventID:       req.EventID,
		SynapseStatus: proxy.Status,
		NativeError:   why + ": " + authErr,
	})
}

func (r *Runner) compute(ctx context.Context, req Request, origin string) ([]byte, int, error) {
	switch req.Endpoint {
	case "state_ids":
		resp, err := r.resolver.StateIDs(ctx, origin, req.RoomID, req.EventID)
		if err != nil {
			return matrixErrorResponse(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, err
		}
		return body, 200, nil
	case "event":
		resp, err := r.resolver.Event(ctx, origin, r.serverName, req.EventID)
		if errors.Is(err, matrixstate.ErrEventNotFound) {
			// Synapse answers an unknown event with 404 and an empty body,
			// not a JSON error.
			return nil, 404, nil
		}
		if err != nil {
			return matrixErrorResponse(err)
		}
		body, err := json.Marshal(resp)
		if err != nil {
			return nil, 0, err
		}
		return body, 200, nil
	default:
		return nil, 0, errors.New("shadow: no native implementation for " + req.Endpoint)
	}
}

func (r *Runner) diff(endpoint string, synapseBody, nativeBody []byte) (*difflog.Diff, error) {
	switch endpoint {
	case "state_ids":
		return CompareStateIDs(synapseBody, nativeBody)
	case "event":
		return CompareEvent(synapseBody, nativeBody)
	default:
		return nil, errors.New("shadow: no comparator for " + endpoint)
	}
}

func (r *Runner) match(req Request) {
	r.diffs.Observe(req.Endpoint, true)
	shadowResults.WithLabelValues(req.Endpoint, "match").Inc()
}

// record fills in the request-side fields and persists a disagreement.
func (r *Runner) record(req Request, proxy ProxyResult, elapsed time.Duration, rec *difflog.Record) {
	r.diffs.Observe(req.Endpoint, false)
	shadowResults.WithLabelValues(req.Endpoint, string(rec.Kind)).Inc()

	rec.Endpoint = req.Endpoint
	rec.Origin = req.Origin
	rec.URI = req.URI
	rec.RoomID = req.RoomID
	rec.EventID = req.EventID
	rec.SynapseStatus = proxy.Status
	rec.SynapseDurationMS = float64(proxy.Duration.Nanoseconds()) / 1e6
	rec.NativeDurationMS = float64(elapsed.Nanoseconds()) / 1e6
	if !proxy.Truncated {
		rec.SynapseBody = proxy.Body
	}

	r.diffs.Log(rec)

	r.log.Warn().
		Str("endpoint", req.Endpoint).
		Str("kind", string(rec.Kind)).
		Str("room_id", req.RoomID).
		Str("event_id", req.EventID).
		Str("origin", req.Origin).
		Int("synapse_status", rec.SynapseStatus).
		Int("native_status", rec.NativeStatus).
		Str("native_error", rec.NativeError).
		Msg("Shadow comparison disagreed with Synapse")
}

// DecodeParam percent-decodes a path parameter, returning it unchanged if it is
// not valid encoding.
func DecodeParam(s string) string {
	if decoded, err := url.PathUnescape(s); err == nil {
		return decoded
	}
	return s
}
