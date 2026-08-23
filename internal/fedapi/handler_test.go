package fedapi

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
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
	srv := httptest.NewServer(New(cfg, p, zerolog.Nop()))
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
