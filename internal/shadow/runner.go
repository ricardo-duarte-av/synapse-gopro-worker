package shadow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/difflog"
	"github.com/daedric/synapse-gopro-worker/internal/fedauth"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
	"github.com/daedric/synapse-gopro-worker/internal/native"
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
	// Body is the request body for endpoints whose answer depends on it.
	Body []byte
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
type StateIDsResolver = native.Resolver

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
	// verifyWait bounds how long verification of a served answer waits for a
	// slot. See Options.VerifyWait.
	verifyWait time.Duration

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
	// VerifyWait is how long verification of a *served* answer waits for a
	// slot before giving up. Zero uses 5s.
	//
	// Only verification waits. An ordinary shadow comparison is dropped the
	// instant the slots are full, and should be: the request it describes was
	// answered by Synapse either way, so a dropped comparison costs one data
	// point out of many. A served answer is not interchangeable like that --
	// it is the only check on something a remote server actually received.
	VerifyWait time.Duration
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
	if opts.VerifyWait <= 0 {
		opts.VerifyWait = 5 * time.Second
	}
	return &Runner{
		resolver:   resolver,
		serverName: serverName,
		verifier:   verifier,
		diffs:      diffs,
		log:        log,
		timeout:    opts.Timeout,
		sem:        make(chan struct{}, opts.Concurrency),
		verifyWait: opts.VerifyWait,
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
	origin, authErr := r.verifyOrigin(req, proxy)

	// A request we would reject has the rejection as its answer. Computing a
	// response for it instead would be doubly wrong: it uses an origin we just
	// failed to verify, and it reports a disagreement on every request with a
	// bad or expired signature, which is ordinary federation traffic rather
	// than a defect. Where we reject and Synapse did not, that is still caught
	// -- as a status mismatch here and as we_reject_synapse_accepts above.
	if authErr != nil {
		start := time.Now()
		body, _ := json.Marshal(authErr)
		r.finish(req, proxy, time.Since(start), body, authErr.Status, native.Meta{})
		return
	}

	start := time.Now()
	nativeBody, nativeStatus, meta, err := r.compute(ctx, req, origin)
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

	r.finish(req, proxy, elapsed, nativeBody, nativeStatus, meta)
}

// finish compares a native answer against Synapse's and records the outcome.
func (r *Runner) finish(req Request, proxy ProxyResult, elapsed time.Duration, nativeBody []byte, nativeStatus int, meta native.Meta) {
	agree, compareBodies := CompareStatus(proxy.Status, nativeStatus)
	if !agree {
		if nativeStatus != http.StatusUnauthorized && upstreamCouldNotFetchKeys(proxy) {
			r.upstreamKeyFetchFailed(req, proxy, nativeStatus)
			return
		}
		if proxy.Status == http.StatusTooManyRequests && nativeStatus != http.StatusTooManyRequests {
			r.upstreamRateLimited(req, nativeStatus)
			return
		}
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

	diff, skipReason, diffErr := r.diff(req, proxy.Body, nativeBody, meta)
	if skipReason != "" {
		shadowSkipped.WithLabelValues(req.Endpoint, skipReason).Inc()
		return
	}
	if diffErr != nil {
		r.record(req, proxy, elapsed, &difflog.Record{
			Kind:         difflog.KindNativeError,
			NativeStatus: nativeStatus,
			NativeError:  diffErr.Error(),
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
func (r *Runner) verifyOrigin(req Request, proxy ProxyResult) (string, *matrixstate.MatrixError) {
	if r.verifier == nil {
		return req.Origin, nil
	}

	synth, err := synthesiseRequest(req)
	if err != nil {
		// Without a replayable request we cannot judge authentication, so fall
		// back to comparing the response alone.
		authVerdicts.WithLabelValues("replay_failed").Inc()
		return req.Origin, nil
	}

	result := r.verifier.Verify(synth)
	synapseAccepted := proxy.Status != http.StatusUnauthorized && proxy.Status != http.StatusForbidden

	switch {
	case result.OK() && proxy.Status == http.StatusUnauthorized && upstreamCouldNotFetchKeys(proxy):
		// Synapse never judged the signature: it could not obtain the key to
		// judge it with. Its 401 therefore says nothing about whether
		// accepting was right, and counting it as the dangerous direction
		// would bury real ones. Kept as its own verdict, not hidden.
		authVerdicts.WithLabelValues("synapse_key_fetch_failed").Inc()
	case result.OK() && proxy.Status == http.StatusUnauthorized:
		// We would have accepted what Synapse rejected *on the merits*. This
		// is the dangerous direction and must block promotion.
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
		return result.Origin, nil
	}
	return req.Origin, &matrixstate.MatrixError{
		Status:  result.Status(),
		ErrCode: result.Err.ErrCode,
		Message: result.Err.Err,
	}
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
	// The X-Matrix signature covers the request content, so a POST verified
	// without its body fails every time -- and fails as
	// we_reject_synapse_accepts, which reads as the dangerous direction while
	// being entirely our own doing.
	//
	// ContentLength matters as much as Body: mautrix reads the body only when
	// ContentLength is non-zero, so leaving it at zero verifies against empty
	// content no matter what Body holds.
	if len(req.Body) > 0 {
		out.Body = io.NopCloser(bytes.NewReader(req.Body))
		out.ContentLength = int64(len(req.Body))
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

// compute produces our answer, using the same code the request path uses so
// the two can never disagree about what "our answer" is.
func (r *Runner) compute(ctx context.Context, req Request, origin string) ([]byte, int, native.Meta, error) {
	return native.Answer(ctx, r.resolver, r.serverName, native.Request{
		Endpoint: req.Endpoint,
		Origin:   origin,
		RoomID:   req.RoomID,
		EventID:  req.EventID,
		Body:     req.Body,
	})
}

// diff compares two answers, and may report that they are not comparable.
//
// The skip reason exists for /get_missing_events, where a walk that stopped at
// the limit has no single correct answer. Returning it here rather than
// letting the endpoint fake a match keeps "we agree" meaning what it says.
func (r *Runner) diff(req Request, synapseBody, nativeBody []byte, meta native.Meta) (*difflog.Diff, string, error) {
	switch req.Endpoint {
	case "state_ids":
		d, err := CompareStateIDs(synapseBody, nativeBody)
		return d, "", err
	case "event":
		d, err := CompareEvent(synapseBody, nativeBody)
		return d, "", err
	case "get_missing_events":
		return CompareGetMissingEvents(req.Body, synapseBody, nativeBody, meta.WalkTruncated)
	default:
		return nil, "", errors.New("shadow: no comparator for " + req.Endpoint)
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

// upstreamCouldNotFetchKeys reports that Synapse's 401 says it failed to
// *obtain* a key, rather than that it judged the request's signature bad.
//
// The distinction is the whole point. "Invalid signature for server X" is a
// verdict on the request, and if we accepted something Synapse rejected that
// way it is a security failure on our side. "Failed to find any key to
// satisfy" is not a verdict at all — Synapse never evaluated the signature,
// because it could not get the key to evaluate it with. Its 401 then says
// nothing about whether accepting the request was right.
//
// This is not hypothetical or rare. matrix.ryuu.eu publishes an AAAA record
// pointing at a dead tunnel; Go's dialler falls back to IPv4 after 300ms
// (Happy Eyeballs) and fetches a perfectly valid key, while Twisted — which
// Synapse's federation client is built on — connects to what it resolved and
// waits. Every notary on the network is serving a stale copy of that server's
// key from 2025-12-23, so Synapse cannot recover on its own. Counting this as
// a mismatch would mean treating "we succeeded where Synapse could not" as a
// defect, and would bury real disagreements under it.
//
// Deliberately narrow: it matches only this one error, only on a 401, and
// only when the body was captured whole.
func upstreamCouldNotFetchKeys(proxy ProxyResult) bool {
	if proxy.Status != http.StatusUnauthorized || proxy.Truncated || len(proxy.Body) == 0 {
		return false
	}
	var e struct {
		ErrCode string `json:"errcode"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(proxy.Body, &e); err != nil {
		return false
	}
	return e.ErrCode == "M_UNAUTHORIZED" &&
		strings.Contains(e.Error, "Failed to find any key to satisfy")
}

// upstreamKeyFetchFailed records a disagreement caused by Synapse being unable
// to fetch the origin's keys.
//
// It is counted and logged rather than dropped. It does not belong in the
// mismatch ratio, because nothing here is wrong on our side — but it must stay
// visible, both because a sudden rise means something broke in key resolution
// and because "we answered where Synapse could not" is a claim that should
// always have evidence behind it.
func (r *Runner) upstreamKeyFetchFailed(req Request, proxy ProxyResult, nativeStatus int) {
	shadowSkipped.WithLabelValues(req.Endpoint, "upstream_key_fetch_failed").Inc()
	r.log.Info().
		Str("endpoint", req.Endpoint).
		Str("origin", req.Origin).
		Str("uri", req.URI).
		Int("synapse_status", proxy.Status).
		Int("native_status", nativeStatus).
		Str("synapse_error", string(proxy.Body)).
		Msg("Synapse could not fetch the origin's keys; we verified it. Not counted as a mismatch")
}

// CompareServed checks an answer we already served against Synapse's.
//
// This is the canary's safety net, and it inverts the shadow arrangement: in
// shadow mode the proxy answers and we compute a second opinion nobody sees,
// while here *we* answered and Synapse's opinion is the one fetched afterwards.
//
// That inversion is the point. Without it a canary verifies only the share it
// did not serve, so the requests being checked are exactly the ones that never
// reached a remote server — the reverse of what a promotion gate should watch.
//
// It runs after the client has its answer, so it costs an extra upstream
// request but no latency, and a disagreement is recorded exactly as a shadow
// mismatch is. Dropped rather than queued when busy: falling behind must never
// slow down serving.
func (r *Runner) CompareServed(req Request, proxy ProxyResult, elapsed time.Duration, nativeBody []byte, nativeStatus int, meta native.Meta) {
	if r == nil {
		return
	}
	if !r.acquireForVerification(req.Endpoint) {
		return
	}
	defer func() { <-r.sem }()
	defer func() {
		if p := recover(); p != nil {
			shadowSkipped.WithLabelValues(req.Endpoint, "panic").Inc()
			r.log.Error().Interface("panic", p).
				Str("endpoint", req.Endpoint).Str("uri", req.URI).
				Msg("Canary comparison panicked")
		}
	}()

	if proxy.Status == 0 || proxy.Status == http.StatusBadGateway {
		// Synapse gave us nothing to compare against; that is a failure to
		// verify, not a disagreement, and must not be counted as a match.
		shadowSkipped.WithLabelValues(req.Endpoint, "upstream_unavailable").Inc()
		return
	}
	canaryCompared.WithLabelValues(req.Endpoint).Inc()

	// Record both sides here too, over the same set of requests.
	//
	// These were observed only on the shadow path, which meant the
	// Native-vs-Synapse comparison went dark for exactly the traffic we
	// served -- and at canary:100 there is no shadow path left, so the panel
	// that demonstrates the point of the project emptied out at the moment
	// the project started working. proxy.Duration here is Synapse answering
	// the verification replay, which is the same work on the same request.
	shadowDuration.WithLabelValues(req.Endpoint).Observe(elapsed.Seconds())
	shadowUpstreamDuration.WithLabelValues(req.Endpoint).Observe(proxy.Duration.Seconds())

	r.finish(req, proxy, elapsed, nativeBody, nativeStatus, meta)
}

// upstreamRateLimited records a disagreement caused by Synapse shedding load
// rather than by answering differently.
//
// A 429 is not a verdict on the request: Synapse declined to compute an answer,
// so there is nothing to compare ours against. It is the same shape as a key
// fetch failure (§6.10) and is treated the same way.
//
// This is a designed divergence, not a defect. Our limiter is deliberately
// inactive while an endpoint is proxied or shadowed -- Synapse answers
// everything then, and its own limiter already protects it, so adding ours in
// front would put the traffic through two limiters in series. In canary it
// gates only the share we serve. So whenever Synapse sheds load, we will have
// computed an answer it did not.
//
// Counted and logged rather than dropped: a sustained rise means Synapse is
// struggling with traffic we are absorbing, which is worth seeing.
func (r *Runner) upstreamRateLimited(req Request, nativeStatus int) {
	shadowSkipped.WithLabelValues(req.Endpoint, "upstream_rate_limited").Inc()
	r.log.Info().
		Str("endpoint", req.Endpoint).
		Str("origin", req.Origin).
		Int("native_status", nativeStatus).
		Msg("Synapse rate limited this request; we computed an answer. Not counted as a mismatch")
}

// acquireForVerification takes a comparison slot for a served answer, waiting
// briefly rather than giving up the moment the slots are full.
//
// Shadow comparison drops immediately when busy, and should: the request it
// describes was answered by Synapse regardless. Verification of a served
// answer is not interchangeable that way -- it is the only check on something
// a remote server actually received, and dropping it leaves the match rate
// looking clean while the guarantee behind it weakens. That is not
// hypothetical: at canary:25, 75 busy skips took the verified share to 0.75,
// so roughly one served answer in four went unchecked.
//
// Waiting costs nothing here. This runs after the client already has its
// answer, on a detached context, so the only cost is a goroutine parked for a
// moment. The wait is bounded so a persistently saturated worker still sheds
// rather than accumulating goroutines without limit.
func (r *Runner) acquireForVerification(endpoint string) bool {
	select {
	case r.sem <- struct{}{}:
		return true
	default:
	}

	verifyWaited.WithLabelValues(endpoint).Inc()
	timer := time.NewTimer(r.verifyWait)
	defer timer.Stop()
	select {
	case r.sem <- struct{}{}:
		return true
	case <-timer.C:
		shadowSkipped.WithLabelValues(endpoint, "busy").Inc()
		return false
	}
}
