package fedapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
	"github.com/daedric/synapse-gopro-worker/internal/native"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
)

// bulkResolver streams n chunks of the given size, as fast as the client will
// take them. It is the shape /state has: many writes, no thinking in between.
type bulkResolver struct {
	fakeResolver
	chunks int
	size   int
}

func (b *bulkResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	payload := strings.Repeat("x", b.size)
	total := 0
	for i := 0; i < b.chunks; i++ {
		if err := ctx.Err(); err != nil {
			return matrixstate.StateResult{}, err
		}
		n, err := io.WriteString(w, payload)
		total += n
		if err != nil {
			return matrixstate.StateResult{}, err
		}
	}
	return matrixstate.StateResult{Bytes: int64(total)}, nil
}

func streamFrontend(t *testing.T, res native.Resolver, opts ...Option) string {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{
		ServerName: "example.com",
		Endpoints: config.Endpoints{
			Event: config.Mode{Kind: config.ModeProxy}, StateIDs: config.Mode{Kind: config.ModeProxy},
			State: config.Mode{Kind: config.ModeNative},
		},
	}
	p, err := proxy.New(config.Upstream{Addrs: []string{strings.TrimPrefix(upstream.URL, "http://")}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	all := append([]Option{WithNative(res, acceptingVerifier{}, 5*time.Second, 30*time.Second)}, opts...)
	front := httptest.NewServer(New(cfg, p, nil, zerolog.Nop(), all...))
	t.Cleanup(front.Close)
	return front.Listener.Addr().String()
}

// request writes a /state request on a raw connection, so the test controls
// exactly how fast the response is read. An http.Client would drain eagerly,
// which is the one thing these tests must not do.
func request(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /_matrix/federation/v1/state/%21r%3Aex?event_id=%24e HTTP/1.1\r\n"+
		"Host: example.com\r\n"+
		`Authorization: X-Matrix origin="n.example",destination="example.com",key="ed25519:a",sig="x"`+"\r\n"+
		"Connection: close\r\n\r\n")
	return conn
}

// A client that is merely slow must not be truncated.
//
// This is the regression this whole change exists for. Measured live on
// 2026-09-03: one peer drained an 18.8MB /state response at 23 KB/s, was making
// steady progress the whole time, and the old 120s total-duration budget cut it
// at 3.06MB. The same room went out complete in 3.5s to the same peer four
// seconds later. The drain rate is the client's, not ours, and a budget that
// bounds it is bounding the wrong thing.
func TestSlowButProgressingClientIsNotTruncated(t *testing.T) {
	const chunks, size = 64, 64 * 1024 // 4MB, comfortably past any buffer

	// The idle budget is deliberately far shorter than this transfer takes.
	// That is the whole point: it must measure time *without progress*, not
	// elapsed time, so a deadline that is set once rather than pushed forward
	// on every write fails here.
	addr := streamFrontend(t, &bulkResolver{chunks: chunks, size: size},
		WithStreamTimeout(30*time.Second), WithStreamIdleTimeout(300*time.Millisecond))

	conn := request(t, addr)
	defer conn.Close()

	br := bufio.NewReader(conn)
	if _, err := http.ReadResponse(br, nil); err != nil {
		t.Fatalf("read response head: %v", err)
	}

	// Drain at roughly 1MB/s, well under the rate the resolver can produce, so
	// the server spends most of the transfer blocked on the client.
	got := 0
	buf := make([]byte, 32*1024)
	for got < chunks*size {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, err := br.Read(buf)
		got += n
		if err != nil {
			t.Fatalf("body truncated after %d of %d bytes: %v", got, chunks*size, err)
		}
		time.Sleep(8 * time.Millisecond)
	}
	if got < chunks*size {
		t.Errorf("got %d bytes, want %d", got, chunks*size)
	}
}

// A client that stops reading altogether must be cut, and promptly.
//
// The idle budget is enforced as a write deadline rather than by cancelling the
// context, because cancelling a context does not interrupt a Write already
// blocked in the kernel. A test that only cancelled would hang here.
func TestStalledClientIsCutByTheIdleDeadline(t *testing.T) {
	before := labelledCounter(t, "gopro_native_fallback_total", "reason", "stream_stalled")

	addr := streamFrontend(t, &bulkResolver{chunks: 512, size: 64 * 1024}, // 32MB
		WithStreamTimeout(60*time.Second), WithStreamIdleTimeout(300*time.Millisecond))

	conn := request(t, addr)
	defer conn.Close()
	// Read nothing at all. The server fills the socket buffer and blocks.

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) &&
		labelledCounter(t, "gopro_native_fallback_total", "reason", "stream_stalled") <= before {
		time.Sleep(25 * time.Millisecond)
	}
	if got := labelledCounter(t, "gopro_native_fallback_total", "reason", "stream_stalled"); got <= before {
		t.Error("a client that stopped reading was never cut; the idle deadline did not fire")
	}
}

// A truncated stream must abort the connection, not end cleanly.
//
// This is the half that turns a bug into a silent one. Returning normally after
// a partial body lets net/http write the terminating chunk, so the peer
// receives a *valid* 200 carrying truncated JSON -- undetectable, and not
// retryable. Aborting kills the connection without a terminator, which the peer
// sees as a transfer error.
func TestTruncatedStreamAbortsTheConnection(t *testing.T) {
	// The absolute backstop fires mid-body while writes are still succeeding,
	// which is exactly the case observed live: `context deadline exceeded`
	// after 127s with 3.06MB delivered.
	addr := streamFrontend(t, &slowChunkResolver{chunks: 100, size: 4096, gap: 30 * time.Millisecond},
		WithStreamTimeout(300*time.Millisecond), WithStreamIdleTimeout(10*time.Second))

	conn := request(t, addr)
	defer conn.Close()

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read response head: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err = io.ReadAll(resp.Body)
	if err == nil {
		t.Fatal("a truncated body was delivered as a clean, complete response; " +
			"the peer cannot tell it is short")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("test read timed out rather than observing the abort: %v", err)
	}
}

// slowChunkResolver writes with a gap between chunks, so a short absolute
// backstop fires while writes are still succeeding.
type slowChunkResolver struct {
	fakeResolver
	chunks int
	size   int
	gap    time.Duration
}

func (s *slowChunkResolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (matrixstate.StateResult, error) {
	payload := strings.Repeat("x", s.size)
	for i := 0; i < s.chunks; i++ {
		if err := ctx.Err(); err != nil {
			return matrixstate.StateResult{}, err
		}
		if _, err := io.WriteString(w, payload); err != nil {
			return matrixstate.StateResult{}, err
		}
		time.Sleep(s.gap)
	}
	return matrixstate.StateResult{}, nil
}
