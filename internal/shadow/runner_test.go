package shadow

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/difflog"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

type fakeResolver struct {
	resp  *matrixstate.StateIDsResponse
	event *matrixstate.TransactionResponse
	err   error
	delay time.Duration
	panic bool
	calls atomic.Int64
}

func (f *fakeResolver) Event(ctx context.Context, origin, serverName, eventID string) (*matrixstate.TransactionResponse, error) {
	f.calls.Add(1)
	if f.panic {
		panic("boom")
	}
	return f.event, f.err
}

func (f *fakeResolver) StateIDs(ctx context.Context, origin, roomID, eventID string) (*matrixstate.StateIDsResponse, error) {
	f.calls.Add(1)
	if f.panic {
		panic("boom")
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return f.resp, f.err
}

func newTestRunner(t *testing.T, r StateIDsResolver, opts Options) (*Runner, *difflog.Writer) {
	t.Helper()
	w, err := difflog.Open(difflog.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return NewRunner(r, "example.com", nil, w, zerolog.Nop(), opts), w
}

func req() Request {
	return Request{Endpoint: "state_ids", Origin: "remote.example",
		RoomID: "!r:ex.com", EventID: "$e", URI: "/_matrix/federation/v1/state_ids/%21r%3Aex.com?event_id=%24e"}
}

func proxyOK(t *testing.T, pdus, auth []string) ProxyResult {
	t.Helper()
	return ProxyResult{Status: 200, Body: body(t, pdus, auth), Duration: time.Millisecond}
}

// waitFor polls until cond holds, so tests do not depend on a fixed sleep.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

func TestRunnerRecordsMatch(t *testing.T) {
	f := &fakeResolver{resp: &matrixstate.StateIDsResponse{
		PDUIDs: []string{"$a", "$b"}, AuthChainIDs: []string{"$x"}}}
	r, w := newTestRunner(t, f, Options{})

	r.Go(req(), proxyOK(t, []string{"$b", "$a"}, []string{"$x"}))

	waitFor(t, func() bool { return w.Snapshot().Compared == 1 })
	s := w.Snapshot()
	if s.Matched != 1 || s.Mismatched != 0 {
		t.Errorf("matched/mismatched = %d/%d, want 1/0", s.Matched, s.Mismatched)
	}
}

func TestRunnerRecordsMismatch(t *testing.T) {
	f := &fakeResolver{resp: &matrixstate.StateIDsResponse{
		PDUIDs: []string{"$a"}, AuthChainIDs: []string{}}}
	r, w := newTestRunner(t, f, Options{})

	r.Go(req(), proxyOK(t, []string{"$a", "$b"}, []string{}))

	// Logged trails Mismatched: the record is handed to a background writer,
	// which is what keeps logging off the request path.
	waitFor(t, func() bool { return w.Snapshot().Logged == 1 })
}

// TestRunnerSkipsTruncatedBodies guards the promotion gate: a body we only
// partly captured cannot be judged, and guessing either way would corrupt the
// match rate that decides whether an endpoint is promoted.
func TestRunnerSkipsTruncatedBodies(t *testing.T) {
	f := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	r, w := newTestRunner(t, f, Options{})

	p := proxyOK(t, []string{"$a"}, nil)
	p.Truncated = true
	r.Go(req(), p)

	waitFor(t, func() bool { return f.calls.Load() == 1 })
	time.Sleep(100 * time.Millisecond)

	s := w.Snapshot()
	if s.Compared != 0 || s.Mismatched != 0 {
		t.Errorf("a truncated body was counted: compared=%d mismatched=%d, want 0/0",
			s.Compared, s.Mismatched)
	}
}

func TestRunnerRecordsStatusMismatch(t *testing.T) {
	// Synapse allowed the request; we refused it. That is a serious
	// disagreement and must be logged even though there is no body to diff.
	f := &fakeResolver{err: &matrixstate.MatrixError{
		Status: 403, ErrCode: "M_FORBIDDEN", Message: "Host not in room."}}
	r, w := newTestRunner(t, f, Options{})

	r.Go(req(), proxyOK(t, []string{"$a"}, nil))

	waitFor(t, func() bool { return w.Snapshot().Mismatched == 1 })
}

func TestRunnerAgreesOnMatchingErrors(t *testing.T) {
	f := &fakeResolver{err: &matrixstate.MatrixError{
		Status: 403, ErrCode: "M_FORBIDDEN", Message: "Host not in room."}}
	r, w := newTestRunner(t, f, Options{})

	r.Go(req(), ProxyResult{Status: 403, Body: []byte(`{"errcode":"M_FORBIDDEN"}`)})

	waitFor(t, func() bool { return w.Snapshot().Compared == 1 })
	if got := w.Snapshot().Matched; got != 1 {
		t.Errorf("Matched = %d, want 1: identical error statuses agree", got)
	}
}

func TestRunnerRecordsInternalError(t *testing.T) {
	// A native failure means the native path would have had to fall back to
	// the proxy, which is a finding in its own right.
	f := &fakeResolver{err: context.DeadlineExceeded}
	r, w := newTestRunner(t, f, Options{})

	r.Go(req(), proxyOK(t, []string{"$a"}, nil))

	waitFor(t, func() bool { return w.Snapshot().Logged == 1 })

	recs := readLog(t, w)
	if len(recs) != 1 {
		t.Fatalf("logged %d records, want 1", len(recs))
	}
	if recs[0].Kind != difflog.KindNativeTimeout {
		t.Errorf("Kind = %q, want %q", recs[0].Kind, difflog.KindNativeTimeout)
	}
}

// TestRunnerSurvivesPanic matters because the worker is serving real federation
// traffic correctly via the proxy; a bug in comparison code must not take it
// down.
func TestRunnerSurvivesPanic(t *testing.T) {
	f := &fakeResolver{panic: true}
	r, _ := newTestRunner(t, f, Options{})

	r.Go(req(), proxyOK(t, []string{"$a"}, nil))

	waitFor(t, func() bool { return f.calls.Load() == 1 })
	time.Sleep(100 * time.Millisecond) // the panic must be absorbed, not fatal
}

// TestRunnerNeverBlocks is the property that protects live traffic: comparison
// is best-effort and must be dropped, never queued, when saturated.
func TestRunnerDropsWhenBusy(t *testing.T) {
	f := &fakeResolver{
		resp:  &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}},
		delay: 2 * time.Second,
	}
	r, _ := newTestRunner(t, f, Options{Concurrency: 1})

	start := time.Now()
	for range 100 {
		r.Go(req(), proxyOK(t, []string{"$a"}, nil))
	}
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Errorf("Go blocked for %s; it must return immediately", elapsed)
	}
	// With a concurrency of one and a slow resolver, almost all of those must
	// have been dropped rather than run.
	if n := f.calls.Load(); n > 5 {
		t.Errorf("resolver called %d times, want only a handful: work should be dropped", n)
	}
}

func TestRunnerHonoursTimeout(t *testing.T) {
	f := &fakeResolver{
		resp:  &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}},
		delay: 10 * time.Second,
	}
	r, w := newTestRunner(t, f, Options{Timeout: 100 * time.Millisecond})

	r.Go(req(), proxyOK(t, []string{"$a"}, nil))

	waitFor(t, func() bool { return w.Snapshot().Logged == 1 })
	recs := readLog(t, w)
	if len(recs) == 0 || recs[0].Kind != difflog.KindNativeTimeout {
		t.Errorf("expected a timeout record, got %+v", recs)
	}
}

func TestNilRunnerIsSafe(t *testing.T) {
	var r *Runner
	r.Go(req(), ProxyResult{Status: 200})
}

// readLog reads back the persisted diff records.
func readLog(t *testing.T, w *difflog.Writer) []difflog.Record {
	t.Helper()
	files, err := w.Files()
	if err != nil {
		t.Fatal(err)
	}
	var out []difflog.Record
	for _, f := range files {
		if filepath.Ext(f) == ".gz" {
			continue
		}
		out = append(out, readJSONL(t, f)...)
	}
	return out
}

// TestRunnerTreatsAuthRejectionAsTheAnswer covers a flaw found in production:
// when our own verification rejects a request, computing a response for it
// anyway used the very origin we failed to verify, and reported a disagreement
// against Synapse's 401. Requests with bad or expired signatures are ordinary
// federation traffic, so that turned routine traffic into false mismatches.
func TestRunnerTreatsAuthRejectionAsTheAnswer(t *testing.T) {
	// Without a verifier configured the runner must still behave as before,
	// computing an answer from the claimed origin.
	f := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	r, w := newTestRunner(t, f, Options{})

	r.Go(req(), proxyOK(t, []string{"$a"}, nil))
	waitFor(t, func() bool { return w.Snapshot().Compared == 1 })
	if got := w.Snapshot().Matched; got != 1 {
		t.Errorf("Matched = %d, want 1 when no verifier is configured", got)
	}
}
