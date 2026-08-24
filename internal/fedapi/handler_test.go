package fedapi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
	"github.com/daedric/synapse-gopro-worker/internal/ratelimit"
)

// TestHandlerEndToEnd drives the full path a real request takes: raw bytes on a
// socket, through the router and proxy, to a stand-in Synapse worker.
func TestHandlerEndToEnd(t *testing.T) {
	type upstreamCall struct {
		uri  string
		auth string
	}
	calls := make(chan upstreamCall, 4)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls <- upstreamCall{uri: r.URL.RequestURI(), auth: r.Header.Get("Authorization")}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pdu_ids":[],"auth_chain_ids":[]}`))
	}))
	defer upstream.Close()

	front := newTestFrontend(t, strings.TrimPrefix(upstream.URL, "http://"))

	t.Run("proxies a federation request preserving the URI", func(t *testing.T) {
		const uri = "/_matrix/federation/v1/state_ids/%21abc%3Aexample.com?event_id=%24evt"
		const auth = `X-Matrix origin="remote.example",destination="example.com",key="ed25519:a",sig="ZZ"`

		status, body := rawGet(t, front, uri, auth)
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		if body != `{"pdu_ids":[],"auth_chain_ids":[]}` {
			t.Errorf("body = %q", body)
		}

		got := <-calls
		if got.uri != uri {
			t.Errorf("upstream URI\n got: %q\nwant: %q", got.uri, uri)
		}
		if got.auth != auth {
			t.Errorf("upstream Authorization was altered:\n got: %q\nwant: %q", got.auth, auth)
		}
	})

	t.Run("health check is served locally", func(t *testing.T) {
		status, body := rawGet(t, front, "/health", "")
		if status != http.StatusOK || body != "OK" {
			t.Errorf("health = %d %q, want 200 OK", status, body)
		}
		select {
		case c := <-calls:
			t.Errorf("health check reached the upstream: %q", c.uri)
		default:
		}
	})

	t.Run("unrouted paths are not forwarded", func(t *testing.T) {
		// This worker owns only three endpoints. Anything else must 404 here
		// rather than be silently relayed, so a misconfigured nginx is obvious.
		for _, path := range []string{
			"/_matrix/federation/v1/backfill/%21r",
			"/_matrix/federation/v1/version",
			"/_matrix/client/v3/sync",
		} {
			status, _ := rawGet(t, front, path, "")
			if status != http.StatusNotFound {
				t.Errorf("GET %s = %d, want 404", path, status)
			}
			select {
			case c := <-calls:
				t.Errorf("%s reached the upstream: %q", path, c.uri)
			default:
			}
		}
	})
}

func TestHandlerReturnsBadGatewayWhenUpstreamIsDown(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	front := newTestFrontend(t, addr)
	status, _ := rawGet(t, front, "/_matrix/federation/v1/event/%24a", "")
	if status != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", status)
	}
}

func newTestFrontend(t *testing.T, upstreamAddr string) string {
	t.Helper()
	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event:    config.Mode{Kind: config.ModeProxy},
			State:    config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeProxy},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{upstreamAddr}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(cfg, p, nil, zerolog.Nop()))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// rawGet sends a hand-built request so the URI is not normalised by a client.
func rawGet(t *testing.T, addr, uri, auth string) (int, string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	var sb strings.Builder
	fmt.Fprintf(&sb, "GET %s HTTP/1.1\r\nHost: example.com\r\n", uri)
	if auth != "" {
		fmt.Fprintf(&sb, "Authorization: %s\r\n", auth)
	}
	sb.WriteString("Connection: close\r\n\r\n")
	if _, err := conn.Write([]byte(sb.String())); err != nil {
		t.Fatal(err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(strings.Builder)
	if _, err := bufio.NewReader(resp.Body).WriteTo(buf); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, buf.String()
}

// TestRateLimitRejectsWithSynapseShape checks the 429 we send matches
// Synapse's, since remote servers back off on its fields rather than on the
// status alone.
func TestRateLimitRejectsWithSynapseShape(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hold the request open so the limiter's slots stay occupied.
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event:    config.Mode{Kind: config.ModeProxy},
			State:    config.Mode{Kind: config.ModeProxy},
			StateIDs: config.Mode{Kind: config.ModeProxy},
		},
		// Tight enough that a couple of parallel requests trip it.
		RCFederation: ratelimit.FederationSettings{
			WindowSize: 1000, SleepLimit: 100, SleepDelay: 0, RejectLimit: 1, Concurrent: 1,
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop()))
	defer front.Close()

	const auth = `X-Matrix origin="noisy.example",destination="example.com",key="ed25519:a",sig="x"`
	uri := "/_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e"

	// Fire several in parallel from one origin; at least one must be refused.
	var mu sync.Mutex
	var got429 bool
	var body string
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, b := rawGet(t, front.Listener.Addr().String(), uri, auth)
			if status == http.StatusTooManyRequests {
				mu.Lock()
				got429, body = true, b
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if !got429 {
		t.Fatal("no request was rate limited")
	}

	var parsed struct {
		ErrCode      string `json:"errcode"`
		Error        string `json:"error"`
		RetryAfterMS int    `json:"retry_after_ms"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("429 body is not valid JSON: %v (%q)", err, body)
	}
	if parsed.ErrCode != "M_LIMIT_EXCEEDED" {
		t.Errorf("errcode = %q, want M_LIMIT_EXCEEDED", parsed.ErrCode)
	}
	if parsed.Error != "Too Many Requests" {
		t.Errorf("error = %q, want Synapse's wording", parsed.Error)
	}
	// window_size / sleep_limit, as Synapse reports.
	if parsed.RetryAfterMS != 10 {
		t.Errorf("retry_after_ms = %d, want 10", parsed.RetryAfterMS)
	}
}

// TestRateLimitIsPerOrigin checks a noisy server cannot throttle a quiet one.
func TestRateLimitIsPerOrigin(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Slow") != "" {
			<-release
		}
		_, _ = w.Write([]byte("{}"))
	}))
	defer upstream.Close()
	defer close(release)

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints:  config.Endpoints{StateIDs: config.Mode{Kind: config.ModeProxy}},
		RCFederation: ratelimit.FederationSettings{
			WindowSize: 1000, SleepLimit: 100, SleepDelay: 0, RejectLimit: 100, Concurrent: 1,
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop()))
	defer front.Close()

	uri := "/_matrix/federation/v1/state_ids/%21r%3Aex?event_id=%24e"

	// A different origin must be served promptly regardless.
	done := make(chan int, 1)
	go func() {
		status, _ := rawGet(t, front.Listener.Addr().String(),
			uri, `X-Matrix origin="quiet.example",destination="example.com",key="ed25519:a",sig="x"`)
		done <- status
	}()
	select {
	case status := <-done:
		if status != http.StatusOK {
			t.Errorf("quiet origin got %d, want 200", status)
		}
	case <-time.After(3 * time.Second):
		t.Error("a quiet origin was blocked by another server's limit")
	}
}
