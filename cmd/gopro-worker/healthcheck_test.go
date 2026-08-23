package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/daedric/synapse-gopro-worker/internal/config"
)

func TestHealthcheckOverTCP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("probed %q, want /health", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &config.Config{Listen: config.Listen{Addr: strings.TrimPrefix(srv.URL, "http://")}}
	if err := healthcheck(cfg); err != nil {
		t.Errorf("healthcheck = %v, want nil", err)
	}
}

func TestHealthcheckOverUnixSocket(t *testing.T) {
	// The deployment listens on a unix socket, and the runtime image has no
	// curl, so this is the path that actually matters in production.
	sock := filepath.Join(t.TempDir(), "w.sock")
	l := listenUnix(t, sock)
	defer l.Close()
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	if err := healthcheck(&config.Config{Listen: config.Listen{Socket: sock}}); err != nil {
		t.Errorf("healthcheck = %v, want nil", err)
	}
}

func TestHealthcheckFailsWhenNotServing(t *testing.T) {
	t.Run("nothing listening", func(t *testing.T) {
		cfg := &config.Config{Listen: config.Listen{Socket: filepath.Join(t.TempDir(), "absent.sock")}}
		if err := healthcheck(cfg); err == nil {
			t.Error("healthcheck succeeded with nothing listening")
		}
	})

	t.Run("unhealthy status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()
		cfg := &config.Config{Listen: config.Listen{Addr: strings.TrimPrefix(srv.URL, "http://")}}
		if err := healthcheck(cfg); err == nil {
			t.Error("healthcheck succeeded on a 503")
		}
	})
}

func TestLocalAddr(t *testing.T) {
	// A container binds to all interfaces, but the probe runs inside it and
	// must dial back on loopback.
	for _, tc := range []struct{ in, want string }{
		{":9200", "127.0.0.1:9200"},
		{"0.0.0.0:9200", "127.0.0.1:9200"},
		{"[::]:9200", "127.0.0.1:9200"},
		{"127.0.0.1:9200", "127.0.0.1:9200"},
		{"10.0.0.5:9200", "10.0.0.5:9200"},
	} {
		if got := localAddr(tc.in); got != tc.want {
			t.Errorf("localAddr(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func listenUnix(t *testing.T, path string) net.Listener {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
