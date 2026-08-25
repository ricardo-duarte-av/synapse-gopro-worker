package fedapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
		WithNative(res, fedauth.New("example.com", fedauth.Options{}), 5*time.Second))
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
		body, status, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e")
		if err == nil {
			t.Fatal("a panic did not become an error, so it would escape into the HTTP server")
		}
		if body != nil || status != 0 {
			t.Errorf("a failed answer returned body=%q status=%d, want nothing writable", body, status)
		}
	})

	t.Run("resolver error propagates", func(t *testing.T) {
		h.resolver = &fakeResolver{err: errors.New("db is down")}
		if _, _, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e"); err == nil {
			t.Error("a resolver error was swallowed, so the caller would serve an empty answer")
		}
	})

	t.Run("a Matrix error is an answer, not a failure", func(t *testing.T) {
		// 403 "Host not in room" is Synapse's answer too, so it must be served
		// rather than treated as a reason to fall back.
		h.resolver = &fakeResolver{err: &matrixstate.MatrixError{
			Status: 403, ErrCode: "M_FORBIDDEN", Message: "Host not in room."}}
		body, status, err := h.answer(context.Background(), "state_ids", "remote.example", "!r:example.com", "$e")
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
		WithNative(&fakeResolver{resp: &matrixstate.StateIDsResponse{}}, acceptingVerifier{}, 5*time.Second)))
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
		WithNative(res, acceptingVerifier{}, 5*time.Second)))
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
