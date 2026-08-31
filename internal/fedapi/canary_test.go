package fedapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/fedauth"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
	"github.com/daedric/synapse-gopro-worker/internal/native"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
	"github.com/daedric/synapse-gopro-worker/internal/ratelimit"
	"github.com/daedric/synapse-gopro-worker/internal/shadow"
)

func TestSamplingIsDeterministic(t *testing.T) {
	// A retrying server must keep getting the same implementation. If the
	// choice were random, a disagreement would appear and vanish between
	// retries, which is far harder to debug than either implementation being
	// consistently wrong.
	mode := config.Mode{Kind: config.ModeCanary, CanaryPercent: 37}
	for _, id := range []string{"$abc", "$def", "$xyz:example.org", ""} {
		want := sampled(mode, id)
		for i := 0; i < 50; i++ {
			if got := sampled(mode, id); got != want {
				t.Fatalf("sampled(%q) changed between calls", id)
			}
		}
	}
}

func TestSamplingHonoursTheMode(t *testing.T) {
	const id = "$some-event"
	for _, tc := range []struct {
		mode config.Mode
		want bool
		why  string
	}{
		{config.Mode{Kind: config.ModeProxy}, false, "proxy must never serve natively"},
		{config.Mode{Kind: config.ModeShadow}, false, "shadow must never serve natively"},
		{config.Mode{Kind: config.ModeNative}, true, "native serves everything"},
		{config.Mode{Kind: config.ModeCanary, CanaryPercent: 0}, false, "canary:0 serves nothing"},
		{config.Mode{Kind: config.ModeCanary, CanaryPercent: 100}, true, "canary:100 serves everything"},
	} {
		if got := sampled(tc.mode, id); got != tc.want {
			t.Errorf("%s: sampled = %v, want %v", tc.why, got, tc.want)
		}
	}
}

// Without a stable key there is no stable answer, so such a request must not
// join the canary rather than be assigned one at random.
func TestSamplingDeclinesWithoutAKey(t *testing.T) {
	mode := config.Mode{Kind: config.ModeCanary, CanaryPercent: 50}
	if sampled(mode, "") {
		t.Error("a request with no event ID joined the canary")
	}
	// Native mode still serves it: there is no sampling decision to make.
	if !sampled(config.Mode{Kind: config.ModeNative}, "") {
		t.Error("native mode declined a request with no event ID")
	}
}

// The share served natively should be roughly the configured percentage.
// Exactness is not the point -- a canary at 5% that silently serves 40% would
// be a much bigger problem than one that serves 4%.
func TestSamplingRoughlyMatchesThePercentage(t *testing.T) {
	const n = 20000
	for _, pct := range []int{1, 5, 25, 50, 90} {
		mode := config.Mode{Kind: config.ModeCanary, CanaryPercent: pct}
		hits := 0
		for i := 0; i < n; i++ {
			if sampled(mode, fmt.Sprintf("$event%d:example.org", i)) {
				hits++
			}
		}
		got := float64(hits) * 100 / n
		if got < float64(pct)-2 || got > float64(pct)+2 {
			t.Errorf("canary:%d served %.1f%% of requests, want within 2 points", pct, got)
		}
	}
}

// fakeResolver stands in for the real one so the canary's control flow can be
// tested without a database.
type fakeResolver struct {
	resp  *matrixstate.StateIDsResponse
	err   error
	panic bool
	calls int
	// stateBody is the /state response this resolver produces. Empty means
	// /state is not configured, and State then fails rather than silently
	// returning an empty result a test could mistake for agreement.
	stateBody string
}

func (f *fakeResolver) StateIDs(ctx context.Context, origin, roomID, eventID string) (*matrixstate.StateIDsResponse, error) {
	f.calls++
	if f.panic {
		panic("boom")
	}
	return f.resp, f.err
}

func (f *fakeResolver) Event(ctx context.Context, origin, serverName, eventID string) (*matrixstate.TransactionResponse, error) {
	f.calls++
	return nil, f.err
}

// canaryFrontend wires a handler with a canary that serves everything, so the
// fallback paths can be exercised deterministically.
func canaryFrontend(t *testing.T, upstreamAddr string, res native.Resolver) string {
	t.Helper()
	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event:    config.Mode{Kind: config.ModeProxy},
			State:    config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeCanary, CanaryPercent: 100},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{upstreamAddr}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A verifier with no trusted keys rejects everything, which is what makes
	// the auth-rejection fallback observable.
	h := New(cfg, p, nil, zerolog.Nop(),
		WithNative(res, fedauth.New("example.com", fedauth.Options{}), 5*time.Second, 30*time.Second))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// A request we cannot verify must fall back to Synapse rather than be served
// or refused. Synapse verifies independently, so falling back cannot admit
// anything it would reject -- while serving our own rejection would break
// legitimate federation over a bug in our verifier, which has happened.
func TestCanaryFallsBackWhenVerificationFails(t *testing.T) {
	const proxied = `{"pdu_ids":["$from-synapse"],"auth_chain_ids":[]}`
	var upstreamHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(proxied))
	}))
	defer upstream.Close()

	for _, tc := range []struct {
		name string
		res  *fakeResolver
	}{
		// Verification fails for all of these (no trusted keys), so every one
		// must fall back regardless of what the resolver would have done.
		{"resolver would error", &fakeResolver{err: errors.New("db is down")}},
		{"resolver would panic", &fakeResolver{panic: true}},
		{"resolver would succeed", &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$ours"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := upstreamHits
			front := canaryFrontend(t, strings.TrimPrefix(upstream.URL, "http://"), tc.res)
			const uri = "/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt"
			status, body := rawGet(t, front, uri,
				`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)

			if status != http.StatusOK {
				t.Errorf("status = %d, want 200 from the proxy", status)
			}
			if body != proxied {
				t.Errorf("body = %q, want Synapse's answer %q", body, proxied)
			}
			if upstreamHits != before+1 {
				t.Errorf("upstream was called %d times, want exactly 1 fallback", upstreamHits-before)
			}
			// Verification fails first, so the resolver must never be reached.
			// Asserting this keeps the test honest about what it proves: the
			// auth fallback, not the error or panic fallbacks.
			if tc.res.calls != 0 {
				t.Errorf("resolver was called %d times despite verification failing", tc.res.calls)
			}
		})
	}
}

// The error and panic fallbacks cannot be reached through the HTTP path
// without a validly signed request, so they are exercised directly. A panic in
// particular must become an error rather than escaping: unwound through the
// HTTP server it would kill the connection instead of falling back.
func TestNativeAnswerContainsFailures(t *testing.T) {
	h := New(&config.Config{ServerName: "example.com"}, nil, nil, zerolog.Nop())

	t.Run("panic becomes an error", func(t *testing.T) {
		h.resolver = &fakeResolver{panic: true}
		body, status, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e", nil)
		if err == nil {
			t.Fatal("a panic did not become an error, so it would escape into the HTTP server")
		}
		if body != nil || status != 0 {
			t.Errorf("a failed answer returned body=%q status=%d, want nothing writable", body, status)
		}
	})

	t.Run("resolver error propagates", func(t *testing.T) {
		h.resolver = &fakeResolver{err: errors.New("db is down")}
		if _, _, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e", nil); err == nil {
			t.Error("a resolver error was swallowed, so the caller would serve an empty answer")
		}
	})

	t.Run("a Matrix error is an answer, not a failure", func(t *testing.T) {
		// 403 "Host not in room" is Synapse's answer too, so it must be served
		// rather than treated as a reason to fall back.
		h.resolver = &fakeResolver{err: &matrixstate.MatrixError{
			Status: 403, ErrCode: "M_FORBIDDEN", Message: "Host not in room."}}
		body, status, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e", nil)
		if err != nil {
			t.Fatalf("a Matrix error was treated as a failure: %v", err)
		}
		if status != 403 || !strings.Contains(string(body), "M_FORBIDDEN") {
			t.Errorf("status=%d body=%q, want the 403 served as-is", status, body)
		}
	})
}

// A natively served request must not take a limiter slot twice.
//
// serveNative once acquired the limiter itself, while the caller was already
// holding a slot. With Synapse's default concurrent:3 that deadlocks the
// fourth request permanently -- and every one after it, since no slot is ever
// released. It survived unit tests because a single request with a generous
// concurrency limit works fine; it only appears when the limit actually binds.
func TestNativeServingDoesNotDeadlockOnTheLimiter(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeNative},
		},
		// Concurrency of one makes the slot bind immediately; a reject limit
		// high enough that everything queues rather than being refused, so a
		// deadlock shows up as a hang rather than as a 429.
		RCFederation: ratelimit.FederationSettings{
			WindowSize: 1000, SleepLimit: 1000, SleepDelay: 0, RejectLimit: 100, Concurrent: 1,
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(),
		WithNative(&fakeResolver{resp: &matrixstate.StateIDsResponse{}}, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
	defer front.Close()

	const n = 8
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			status, _ := rawGet(t, front.Listener.Addr().String(),
				"/_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e",
				`X-Matrix origin="noisy.example",destination="example.com",key="ed25519:a",sig="x"`)
			done <- status
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case status := <-done:
			if status != http.StatusOK {
				t.Errorf("status = %d, want 200", status)
			}
		case <-time.After(8 * time.Second):
			t.Fatalf("only %d of %d requests completed; the limiter slot is not being released", i, n)
		}
	}
}

// A canary must verify the answers it actually served, not only the share it
// proxied. Without this the promotion gate watches exactly the requests that
// never reached a remote server.
func TestCanaryComparesWhatItServed(t *testing.T) {
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pdu_ids":["$a"],"auth_chain_ids":[]}`))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeCanary, CanaryPercent: 100},
		},
		Shadow: config.Shadow{Concurrency: 4, CaptureMB: 1},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimSpace(strings.TrimPrefix(upstream.URL, "http://"))}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A runner with the same answer as the resolver: agreement, not a mismatch.
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	runner := shadow.NewRunner(res, "example.com", nil, nil, zerolog.Nop(),
		shadow.Options{Concurrency: 4, Timeout: 5 * time.Second})

	front := httptest.NewServer(New(cfg, p, runner, zerolog.Nop(),
		WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
	defer front.Close()

	status, body := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}
	if !strings.Contains(body, `"$a"`) {
		t.Errorf("body = %q, want our own answer", body)
	}

	// The comparison runs after the response, so give it a moment. The point
	// is that Synapse is consulted at all: before this change the upstream was
	// never contacted for a natively served request.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&upstreamHits) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&upstreamHits); got == 0 {
		t.Error("the served answer was never compared against Synapse")
	}
}

// A fallback must be accounted for end to end. The client pays our attempt
// *and* Synapse's, and until this was measured there was no way to answer the
// obvious question after a timeout: did Synapse actually answer it?
func TestFallbackIsAccountedForEndToEnd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pdu_ids":[],"auth_chain_ids":[]}`))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeCanary, CanaryPercent: 100},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A resolver slower than the native budget, so the request times out and
	// falls back -- the expensive case.
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(),
		WithNative(&slowResolver{delay: 2 * time.Second}, acceptingVerifier{}, 150*time.Millisecond, 30*time.Second)))
	defer front.Close()

	start := time.Now()
	status, body := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
	elapsed := time.Since(start)

	if status != http.StatusOK {
		t.Errorf("status = %d, want 200 from the fallback", status)
	}
	if !strings.Contains(body, "pdu_ids") {
		t.Errorf("body = %q, want Synapse's answer", body)
	}
	// The whole point of a short native budget: the client waits roughly the
	// budget plus Synapse, not the resolver's full runtime.
	if elapsed > time.Second {
		t.Errorf("client waited %s; the native budget was 150ms, so a fallback "+
			"should not cost the resolver's full 2s", elapsed)
	}
}

// The verification fetch must outlive Synapse's worst case, not our serving
// budget. Sharing one timeout meant every answer Synapse was slow to produce
// went unverified -- silently skipping the large, cold-cache requests most
// likely to disagree, while the match rate stayed clean.
//
// The oracle is gopro_canary_compared_total, not whether the upstream handler
// ran: the handler runs either way, and asserting on it passes even when our
// fetch has already given up.
func TestVerificationOutlivesTheServingBudget(t *testing.T) {
	before := counterValue(t, "gopro_canary_compared_total")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pdu_ids":["$a"],"auth_chain_ids":[]}`))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeCanary, CanaryPercent: 100},
		},
		Shadow: config.Shadow{Concurrency: 4, CaptureMB: 1},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	runner := shadow.NewRunner(res, "example.com", nil, nil, zerolog.Nop(),
		shadow.Options{Concurrency: 4, Timeout: 5 * time.Second})

	// Serving budget 100ms, verification budget 5s. Synapse takes 400ms: too
	// slow to serve behind, easily fast enough to verify against.
	front := httptest.NewServer(New(cfg, p, runner, zerolog.Nop(),
		WithNative(res, acceptingVerifier{}, 100*time.Millisecond, 5*time.Second)))
	defer front.Close()

	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && counterValue(t, "gopro_canary_compared_total") <= before {
		time.Sleep(20 * time.Millisecond)
	}
	if got := counterValue(t, "gopro_canary_compared_total"); got <= before {
		t.Errorf("canary_compared_total stayed at %v: verification gave up on an "+
			"upstream slower than the serving budget, so the answer we served was never checked", before)
	}
}

// counterValue reads a counter out of the default registry by name, summed
// across labels.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// End to end: a client that disconnects mid-request must be recorded as
// client_gone, never as a timeout.
func TestClientHangupIsNotReportedAsTimeout(t *testing.T) {
	beforeGone := labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone")
	beforeTimeout := labelledCounter(t, "gopro_native_fallback_total", "reason", "timeout")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeCanary, CanaryPercent: 100},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// Resolver slower than the client's patience but well inside the budget,
	// so only a disconnect can end it.
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(),
		WithNative(&slowResolver{delay: 3 * time.Second}, acceptingVerifier{}, 30*time.Second, 30*time.Second)))
	defer front.Close()

	// Connect, send, then hang up before the answer can arrive.
	conn, err := net.DialTimeout("tcp", front.Listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e HTTP/1.1\r\n"+
		"Host: example.com\r\n"+
		`Authorization: X-Matrix origin="n.example",destination="example.com",key="ed25519:a",sig="x"`+"\r\n"+
		"Connection: close\r\n\r\n")
	time.Sleep(200 * time.Millisecond)
	conn.Close()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) &&
		labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone") <= beforeGone {
		time.Sleep(50 * time.Millisecond)
	}

	// The positive assertion is the one that matters: asserting only that
	// "timeout" stayed at zero would pass even if nothing recorded anything.
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "client_gone"); got <= beforeGone {
		t.Fatalf("client_gone stayed at %v: a client hangup was not classified as one", beforeGone)
	}
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "timeout"); got > beforeTimeout {
		t.Errorf("a client hangup was also counted as reason=timeout (%v -> %v)", beforeTimeout, got)
	}
}

// labelledCounter reads one labelled counter out of the default registry.
func labelledCounter(t *testing.T, name, label, value string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// Native must not replay what it served to Synapse.
//
// Verification is what a canary is for. Keeping it in native mode would leave
// Synapse resolving every state group and running every recursive
// state_groups_state walk regardless of how much traffic we answered, so its
// database load -- the cost this project exists to remove -- would never fall.
//
// The test runs canary and native through the same harness on purpose. A test
// that only asserted "native did not contact the upstream" would pass just as
// well if the harness could never observe a hit at all, which is how three
// earlier tests in this package passed while broken. The canary case is the
// control that proves the observation works; the native case is the claim.
//
// The shadow runner is deliberately non-nil in both, so the mode check is the
// only thing that can suppress the replay.
func TestNativeDoesNotVerifyButCanaryDoes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       config.Mode
		wantVerify bool
	}{
		{"canary", config.Mode{Kind: config.ModeCanary, CanaryPercent: 100}, true},
		{"native", config.Mode{Kind: config.ModeNative}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamHits int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&upstreamHits, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"pdu_ids":["$a"],"auth_chain_ids":[]}`))
			}))
			defer upstream.Close()

			cfg := &config.Config{
				ServerName: "example.com",
				Endpoints: config.Endpoints{
					Event: config.Mode{Kind: config.ModeProxy},
					State: config.Mode{Kind: config.ModeProxy},
					// The endpoint under test.
					StateIDs: tc.mode,
				},
				Shadow: config.Shadow{Concurrency: 4, CaptureMB: 1},
			}
			p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
			runner := shadow.NewRunner(res, "example.com", nil, nil, zerolog.Nop(),
				shadow.Options{Concurrency: 4, Timeout: 5 * time.Second})

			front := httptest.NewServer(New(cfg, p, runner, zerolog.Nop(),
				WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
			defer front.Close()

			status, body := rawGet(t, front.Listener.Addr().String(),
				"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
				`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)

			// Both modes must have answered from our own implementation. Without
			// this the native case would also pass on a fallback, which contacts
			// the upstream for a completely different reason.
			if status != http.StatusOK || !strings.Contains(body, `"$a"`) {
				t.Fatalf("status = %d, body = %q; want our own answer served natively", status, body)
			}

			if tc.wantVerify {
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) && atomic.LoadInt32(&upstreamHits) == 0 {
					time.Sleep(20 * time.Millisecond)
				}
				if atomic.LoadInt32(&upstreamHits) == 0 {
					t.Error("canary served an answer without verifying it against Synapse")
				}
				return
			}

			// Absence needs a settling window: the replay is asynchronous, so
			// checking immediately would pass even if it were still queued.
			time.Sleep(500 * time.Millisecond)
			if got := atomic.LoadInt32(&upstreamHits); got != 0 {
				t.Errorf("native contacted the upstream %d time(s); promotion means "+
					"Synapse stops doing the work, so a served answer must not be replayed", got)
			}
		})
	}
}

// safeBuf collects log output written from the handler goroutine while the
// test reads it.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A natively served request must appear in the request log.
//
// It did not: the served path returns before the shared log statement, which
// runs after the proxy forward. While native traffic was a rounding error that
// was merely untidy; at state_ids canary:100 it meant 100% of that endpoint's
// requests produced no log line at all -- and since metrics carry no
// per-origin label by design, "which server asked for this" was recorded
// nowhere.
//
// The absences are asserted as tightly as the presences. upstream_ms and
// backend describe a request to Synapse that never happened, and reporting
// them as zero would be worse than omitting them: a zero upstream_ms reads as
// an instant upstream rather than no upstream.
func TestNativelyServedRequestIsLogged(t *testing.T) {
	for _, endpointMode := range []config.Mode{
		{Kind: config.ModeNative},
		{Kind: config.ModeCanary, CanaryPercent: 100},
	} {
		t.Run(endpointMode.String(), func(t *testing.T) {
			nativelyServedRequestIsLogged(t, endpointMode)
		})
	}
}

func nativelyServedRequestIsLogged(t *testing.T, endpointMode config.Mode) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("upstream must not be contacted in native mode")
	}))
	defer upstream.Close()

	// Both modes that serve natively are covered. Under ModeNative alone the
	// mode field is indistinguishable from the configured mode, so a
	// regression to mode.String() would pass unnoticed; canary is where the
	// two differ and where the field earns its keep, by making the served and
	// proxied shares greppable apart.
	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: endpointMode,
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}

	var logs safeBuf
	front := httptest.NewServer(New(cfg, p, nil, zerolog.New(&logs),
		WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
	defer front.Close()

	status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}

	// The response can reach the client before the handler writes its log
	// line, so this is a race without a settling window.
	var entry map[string]any
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var d map[string]any
			if json.Unmarshal([]byte(l), &d) == nil && d["message"] == "Served federation request" {
				entry = d
			}
		}
		if entry != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if entry == nil {
		t.Fatalf("a natively served request produced no request log line; got:\n%s", logs.String())
	}

	for field, want := range map[string]any{
		"mode":     "native",
		"endpoint": "state_ids",
		"status":   "200",
		"origin":   "remote.example",
	} {
		if got := entry[field]; got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	for _, absent := range []string{"upstream_ms", "backend"} {
		if v, ok := entry[absent]; ok {
			t.Errorf("%s = %v; it describes an upstream request that never happened", absent, v)
		}
	}
	for _, present := range []string{"bytes", "total_ms", "param"} {
		if _, ok := entry[present]; !ok {
			t.Errorf("%s missing from the log line", present)
		}
	}
}

// histogramCount returns how many observations a histogram holds, summed
// across label values.
func histogramCount(t *testing.T, name string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
	}
	return total
}

// A served request must be timed end to end, not just for the answer.
//
// gopro_native_duration_seconds times only h.answer, so it excludes X-Matrix
// verification -- which sits in front of it and needs a network key fetch for
// any origin whose keys are not cached. Reporting that as "native latency"
// would report the number we control rather than the number the federation
// waits for, and it is exactly the shape of mistake §9 of the working notes
// keeps recording: a measurement that answers an adjacent question.
//
// The assertion is that the end-to-end histogram observes at least as much as
// the answer histogram, request for request. Verification is microseconds
// against the fake verifier here, so asserting a strictly larger value would
// be flaky; asserting both were observed is the durable claim.
func TestNativeRequestIsTimedEndToEnd(t *testing.T) {
	beforeE2E := histogramCount(t, "gopro_native_request_seconds")
	beforeAnswer := histogramCount(t, "gopro_native_duration_seconds")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeNative},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(),
		WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
	defer front.Close()

	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 served natively", status)
	}

	if got := histogramCount(t, "gopro_native_request_seconds") - beforeE2E; got != 1 {
		t.Errorf("gopro_native_request_seconds observed %d times, want 1: a served "+
			"request must be timed end to end, verification included", got)
	}
	if got := histogramCount(t, "gopro_native_duration_seconds") - beforeAnswer; got != 1 {
		t.Errorf("gopro_native_duration_seconds observed %d times, want 1", got)
	}
}

// Response size must be observed for answers we served, not only proxied ones.
//
// metrics.ResponseBytes was recorded on the proxy path alone, so a promoted
// endpoint reported no payload sizes at all -- state_ids went native and its
// size histogram went empty. That is not a cosmetic gap: payload size is the
// measurement the /state decision turns on, and the case for reopening /state
// rests on knowing how large these responses actually get.
func TestNativelyServedResponseSizeIsObserved(t *testing.T) {
	before := histogramCount(t, "gopro_response_bytes")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeNative},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New(cfg, p,
		nil, zerolog.Nop(),
		WithNative(&fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}},
			acceptingVerifier{}, 5*time.Second, 30*time.Second)))
	defer front.Close()

	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatal("want 200 served natively")
	}

	if got := histogramCount(t, "gopro_response_bytes") - before; got != 1 {
		t.Errorf("gopro_response_bytes observed %d times, want 1", got)
	}
}

// A client that has gone must not cost Synapse an answer.
//
// The fallback exists to give a remote server a response we could not produce.
// When it hung up there is nobody to receive one, and forwarding only asks
// Synapse to compute a body that will be discarded -- on /state_ids that is
// seconds of database time each, and on this deployment the case is common:
// Tuwunel-style servers hang up on /state_ids constantly.
//
// The upstream handler counts its own calls, which is the only assertion that
// can distinguish "we skipped the forward" from "we forwarded and the write
// failed". A test that watched the response would not tell those apart,
// because there is no client left to observe either outcome.
func TestClientGoneDoesNotForwardToSynapse(t *testing.T) {
	before := labelledCounter(t, "gopro_upstream_skipped_total", "endpoint", "state_ids")

	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeCanary, CanaryPercent: 100},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(),
		WithNative(&slowResolver{delay: 3 * time.Second}, acceptingVerifier{}, 30*time.Second, 30*time.Second)))
	defer front.Close()

	conn, err := net.DialTimeout("tcp", front.Listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e HTTP/1.1\r\n"+
		"Host: example.com\r\n"+
		`Authorization: X-Matrix origin="n.example",destination="example.com",key="ed25519:a",sig="x"`+"\r\n"+
		"Connection: close\r\n\r\n")
	time.Sleep(200 * time.Millisecond)
	conn.Close()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) &&
		labelledCounter(t, "gopro_upstream_skipped_total", "endpoint", "state_ids") <= before {
		time.Sleep(50 * time.Millisecond)
	}

	if got := labelledCounter(t, "gopro_upstream_skipped_total", "endpoint", "state_ids"); got <= before {
		t.Fatalf("gopro_upstream_skipped_total stayed at %v: the forward was not skipped", before)
	}
	// Settle, so a forward racing behind the counter would still be seen.
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&upstreamHits); got != 0 {
		t.Errorf("Synapse was asked for %d answer(s) nobody was waiting for", got)
	}
}

// State satisfies native.Resolver. /state is not exercised by these tests; a
// fake that silently returned an empty result would let a test claiming to
// cover it pass without doing anything.
func (f *fakeResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	f.calls++
	if f.panic {
		panic("boom")
	}
	if f.stateBody == "" {
		return matrixstate.StateResult{}, errors.New("fakeResolver: State not configured")
	}
	if _, err := io.WriteString(w, f.stateBody); err != nil {
		return matrixstate.StateResult{}, err
	}
	// Digested the same way the real resolver digests what it writes, so the
	// test exercises the comparison rather than a shortcut around it.
	return matrixstate.DigestStateResponse(strings.NewReader(f.stateBody))
}

func (s *slowResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	select {
	case <-time.After(s.delay):
		return matrixstate.StateResult{}, nil
	case <-ctx.Done():
		return matrixstate.StateResult{}, ctx.Err()
	}
}

// End to end: /state in shadow mode is compared by digest, without either side
// being buffered.
//
// The upstream response never passes through a capture buffer -- it is folded
// into a digest on its way to the client -- so this is the only test that can
// show the comparison happening at all for an endpoint whose responses cannot
// be captured.
func TestStateIsComparedByDigest(t *testing.T) {
	const body = `{"pdus":[{"a":1,"depth":5,"unsigned":{}},{"b":2,"unsigned":{}}],` +
		`"auth_chain":[{"c":3,"unsigned":{}}]}`

	for _, tc := range []struct {
		name      string
		ours      string
		wantMatch bool
	}{
		{"identical", body, true},
		// Key order differs on every event in reality, because we splice stored
		// JSON while Synapse re-serialises from a dict.
		{"same content, different key order",
			`{"auth_chain":[{"unsigned":{},"c":3}],"pdus":[{"unsigned":{},"depth":5,"a":1},{"unsigned":{},"b":2}]}`, true},
		{"a PDU differs",
			`{"pdus":[{"a":1,"depth":6,"unsigned":{}},{"b":2,"unsigned":{}}],"auth_chain":[{"c":3,"unsigned":{}}]}`, false},
		{"a PDU is missing",
			`{"pdus":[{"a":1,"depth":5,"unsigned":{}}],"auth_chain":[{"c":3,"unsigned":{}}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeMatch := labelledCounter(t, "gopro_shadow_results_total", "result", "match")
			beforeBody := labelledCounter(t, "gopro_shadow_results_total", "result", "body_mismatch")

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer upstream.Close()

			cfg := &config.Config{
				ServerName: "example.com",
				Endpoints: config.Endpoints{
					Event: config.Mode{Kind: config.ModeProxy}, StateIDs: config.Mode{Kind: config.ModeProxy},
					State: config.Mode{Kind: config.ModeShadow},
				},
				Shadow: config.Shadow{Concurrency: 4, CaptureMB: 1},
			}
			p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			res := &fakeResolver{stateBody: tc.ours}
			runner := shadow.NewRunner(res, "example.com", nil, nil, zerolog.Nop(),
				shadow.Options{Concurrency: 4, Timeout: 5 * time.Second})

			front := httptest.NewServer(New(cfg, p, runner, zerolog.Nop(),
				WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
			defer front.Close()

			status, got := rawGet(t, front.Listener.Addr().String(),
				"/_matrix/federation/v1/state/%21r%3Aexample.com?event_id=%24evt",
				`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)

			// The client must still receive Synapse's answer untouched: shadow
			// mode serves the proxy, and the digest rides along.
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			if got != body {
				t.Errorf("client got %q, want Synapse's body verbatim", got)
			}

			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if labelledCounter(t, "gopro_shadow_results_total", "result", "match") > beforeMatch ||
					labelledCounter(t, "gopro_shadow_results_total", "result", "body_mismatch") > beforeBody {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}

			matched := labelledCounter(t, "gopro_shadow_results_total", "result", "match") > beforeMatch
			mismatched := labelledCounter(t, "gopro_shadow_results_total", "result", "body_mismatch") > beforeBody

			if tc.wantMatch && !matched {
				t.Error("identical responses were not recorded as a match")
			}
			if !tc.wantMatch && !mismatched {
				t.Error("differing responses were not recorded as a mismatch")
			}
			if tc.wantMatch && mismatched {
				t.Error("identical responses were recorded as a mismatch")
			}
		})
	}
}

// GetMissingEvents satisfies native.Resolver. Not exercised by these tests; a
// fake returning an empty result would let a test claiming to cover the
// endpoint pass without doing anything.
func (f *fakeResolver) GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*matrixstate.MissingEventsResponse, error) {
	return nil, errors.New("fakeResolver: GetMissingEvents not configured")
}

func (s *slowResolver) GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*matrixstate.MissingEventsResponse, error) {
	select {
	case <-time.After(s.delay):
		return &matrixstate.MissingEventsResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The limiter wait histogram must be fed on every acquisition.
//
// It was defined and queried by three dashboard panels but never observed, so
// those panels could not have shown anything -- the same shape as the metrics
// that existed without panels, inverted. Observing only the delayed requests
// would be almost as bad: the zero waits are what stop the quantiles reporting
// half a second as typical.
func TestLimiterWaitIsObservedOnEveryRequest(t *testing.T) {
	before := histogramCount(t, "gopro_rate_limit_queue_wait_seconds")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, State: config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeNative},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	res := &fakeResolver{resp: &matrixstate.StateIDsResponse{PDUIDs: []string{"$a"}}}
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(),
		WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)))
	defer front.Close()

	// A single uncontended request: it waits zero, and must still be observed.
	if status, _ := rawGet(t, front.Listener.Addr().String(),
		"/_matrix/federation/v1/state_ids/%21r%3Aexample.com?event_id=%24evt",
		`X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	if got := histogramCount(t, "gopro_rate_limit_queue_wait_seconds") - before; got != 1 {
		t.Errorf("limiter wait observed %d times, want 1 (an undelayed request still waits zero)", got)
	}
}

// Every routable endpoint must honour its configured mode.
//
// modeFor was a hand-written switch, so get_missing_events fell through to
// proxy the moment it was added: the config said shadow, the startup line said
// shadow, and every request ran as proxy with nothing compared. The failure was
// silent in the only place anyone would look -- the dashboard simply had no
// series for the endpoint, which is indistinguishable from no traffic.
//
// This walks the route table rather than a list written here, so an endpoint
// added without a mode fails the test instead of quietly proxying.
func TestEveryRoutedEndpointHonoursItsMode(t *testing.T) {
	// Derived from the route table, not from a list written here. A list
	// written here would need updating by the same person who forgot to
	// configure the endpoint, which is the mistake this test exists to catch.
	full := config.Endpoints{}.ByName()

	for _, p := range prefixes {
		t.Run(string(p.endpoint), func(t *testing.T) {
			if _, ok := full[string(p.endpoint)]; !ok {
				t.Fatalf("%s is routed but has no config field; every request "+
					"would silently proxy", p.endpoint)
			}

			// Configure only this endpoint to shadow; everything else proxies.
			var eps config.Endpoints
			v := reflect.ValueOf(&eps).Elem()
			typ := v.Type()
			for i := 0; i < typ.NumField(); i++ {
				kind := config.ModeProxy
				if typ.Field(i).Tag.Get("yaml") == string(p.endpoint) {
					kind = config.ModeShadow
				}
				v.Field(i).Set(reflect.ValueOf(config.Mode{Kind: kind}))
			}

			cfg := &config.Config{ServerName: "example.com", Endpoints: eps}
			h := &Handler{cfg: cfg, modes: cfg.Endpoints.ByName()}
			if got := h.modeFor(p.endpoint); got.Kind != config.ModeShadow {
				t.Errorf("%s configured as shadow but modeFor returned %q; "+
					"the mode lookup has drifted from the config", p.endpoint, got.Kind)
			}
		})
	}
}

// The comparator's request must carry everything verification needs.
//
// The X-Matrix signature covers method, URI, origin, destination and content,
// so a shadow request missing the body cannot verify a POST. It failed as
// we_reject_synapse_accepts -- correct, well-signed traffic reported as the
// direction that blocks promotion -- and it survived one fix because the body
// was threaded into the /state branch and not the other.
func TestShadowRequestCarriesEverythingVerificationNeeds(t *testing.T) {
	body := []byte(`{"earliest_events":[],"latest_events":["$a"],"limit":10}`)
	r, err := http.NewRequest(http.MethodPost,
		"http://x/_matrix/federation/v1/get_missing_events/%21r%3Aex.com", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", `X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`)

	h := &Handler{}
	got := h.shadowRequest(r, "get_missing_events", "remote.example", "!r:ex.com", "", body)

	if !bytes.Equal(got.Body, body) {
		t.Errorf("Body = %q, want %q: a POST cannot be verified without its content", got.Body, body)
	}
	if got.Method != http.MethodPost {
		t.Errorf("Method = %q, want POST", got.Method)
	}
	if got.URI != "/_matrix/federation/v1/get_missing_events/%21r%3Aex.com" {
		t.Errorf("URI = %q; the signature covers it byte-for-byte", got.URI)
	}
	if got.AuthHeader == "" {
		t.Error("AuthHeader empty; verification cannot be replayed")
	}
	if got.Endpoint != "get_missing_events" || got.Origin != "remote.example" || got.RoomID != "!r:ex.com" {
		t.Errorf("identity fields wrong: %+v", got)
	}
}

// A POST endpoint must still get a stable sampling key.
//
// sampled() declines any request without one, so an endpoint whose key comes
// out empty never joins the canary: it sits in canary mode serving nothing,
// which in the metrics is indistinguishable from sampling that has not come up
// yet. That is exactly what canary:25 did for /get_missing_events, whose key
// was read from an event_id query parameter a POST does not carry.
func TestSamplingKeyForPostEndpoints(t *testing.T) {
	route, ok := Match("/_matrix/federation/v1/get_missing_events/%21r%3Aex.com")
	if !ok {
		t.Fatal("route did not match")
	}
	body := []byte(`{"earliest_events":["$a"],"latest_events":["$b"],"limit":10}`)

	key := samplingKey(route, "", body)
	if key == "" {
		t.Fatal("empty sampling key: the endpoint would never join the canary")
	}
	// Stable: a retry sends the same bytes and must land the same way.
	if again := samplingKey(route, "", body); again != key {
		t.Error("sampling key is not stable across identical requests")
	}
	// Distinct questions about one room sample independently, so a busy room
	// is not all-or-nothing.
	other := samplingKey(route, "", []byte(`{"earliest_events":["$c"],"latest_events":["$b"],"limit":10}`))
	if other == key {
		t.Error("two different questions about the same room share a key")
	}

	// And the key actually admits requests at a non-zero percentage.
	m := config.Mode{Kind: config.ModeCanary, CanaryPercent: 50}
	var admitted int
	for i := range 200 {
		b := []byte(`{"latest_events":["$` + string(rune('a'+i%26)) + string(rune('a'+i/26)) + `"]}`)
		if sampled(m, samplingKey(route, "", b)) {
			admitted++
		}
	}
	if admitted == 0 {
		t.Error("no request was ever sampled; the canary is a no-op")
	}
	if admitted == 200 {
		t.Error("every request was sampled; the percentage is being ignored")
	}
	t.Logf("  canary:50 admitted %d of 200 distinct questions", admitted)
}

// GET endpoints keep keying on the event ID.
func TestSamplingKeyForGetEndpointsIsUnchanged(t *testing.T) {
	route, _ := Match("/_matrix/federation/v1/event/%24abc")
	if got := samplingKey(route, "$abc", nil); got != "$abc" {
		t.Errorf("samplingKey = %q, want the event ID", got)
	}
}

// A streamed answer writes nothing until it is sure of one.
//
// Every other endpoint builds its response in memory, so any failure can fall
// back to the proxy. /state cannot buffer 97MB, so the guarantee is narrowed
// rather than dropped: Resolver.State performs every check that can produce a
// Matrix error before writing a byte, and while nothing has been written a
// failure falls back exactly as elsewhere.
func TestStreamedAnswerFallsBackWhenNothingWasWritten(t *testing.T) {
	rec := httptest.NewRecorder()
	out := &lazyResponse{w: rec}

	// Nothing written yet: no status line has been sent.
	if out.written {
		t.Fatal("the status line was written before any body byte")
	}
	if rec.Code != 200 || rec.Body.Len() != 0 {
		t.Errorf("recorder already touched: code=%d body=%d", rec.Code, rec.Body.Len())
	}

	// The first byte commits.
	if _, err := out.Write([]byte(`{"pdus":[`)); err != nil {
		t.Fatal(err)
	}
	if !out.written {
		t.Error("writing a byte did not mark the response as committed")
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if out.bytes != 9 {
		t.Errorf("bytes = %d, want 9", out.bytes)
	}
}

// Only /state streams. Marking another endpoint would send it down a path
// whose fallback guarantee is weaker, for no reason.
func TestOnlyStateStreams(t *testing.T) {
	for _, e := range []Endpoint{EndpointEvent, EndpointStateIDs, EndpointGetMissingEvents} {
		if e.Streams() {
			t.Errorf("%s claims to stream; only /state should", e)
		}
	}
	if !EndpointState.Streams() {
		t.Error("/state does not claim to stream, so it would buffer a 97MB response")
	}
}
