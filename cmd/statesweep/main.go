// Command statesweep drives a /state soak that organic traffic will not.
//
// /state receives essentially no real traffic on this deployment: 59 organic
// requests over four days, 57 of them from a single misbehaving peer that has
// since gone quiet, against 4,652 /state_ids and 11,775 /event in a single
// day. Waiting for a shadow soak to accumulate would mean waiting past the
// deprecation of the very worker the comparison runs against.
//
// So this walks every room and asks for /state at each one's forward
// extremity, aimed at gopro-worker rather than at Synapse. That matters: the
// worker performs the real comparison, on the real code path, and records the
// result to the diff log and the metrics the promotion gate reads. This
// program generates traffic; it deliberately does not judge it.
//
// The evidence it produces is weaker than organic traffic in one specific way,
// and the difference should not be glossed over. Shadow mode's value on this
// project was that every bug it caught was found by a traffic pattern nobody
// anticipated. A sweep tests the rooms its author thought to enumerate. What
// it offers instead is exhaustiveness: organic /state traffic touched three
// rooms in four days, and this touches all of them -- every room version,
// every ACL, the rooms with rejected events, the room full of depth-invalid
// attack artefacts.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"maunium.net/go/mautrix/federation"
)

func main() {
	var (
		dsn        = flag.String("dsn", "host=/var/sockets user=gopro_ro dbname=synapse-db", "read-only PostgreSQL DSN")
		keyPath    = flag.String("key", "", "path to the homeserver signing key (required)")
		socket     = flag.String("socket", "/var/sockets/nginx/av-gopro-worker-1.sock", "gopro-worker unix socket")
		base       = flag.String("base", "", "HTTP base URL instead of the socket, e.g. https://example.com")
		serverName = flag.String("server-name", "aguiarvieira.pt", "our server name; used as request origin and destination")
		minState   = flag.Int("min-state", 0, "skip rooms with fewer state events than this")
		maxState   = flag.Int("max-state", 20000, "skip rooms larger than this; 0 for no limit")
		interval   = flag.Duration("interval", 2*time.Second, "minimum gap between requests")
		timeout    = flag.Duration("timeout", 5*time.Minute, "per-request timeout")
		limit      = flag.Int("limit", 0, "stop after this many rooms; 0 for all")
		progress   = flag.String("progress", "", "file recording completed rooms, so a sweep can resume")
		dryRun     = flag.Bool("dry-run", false, "list what would be swept and exit")
	)
	flag.Parse()

	if *keyPath == "" {
		fatal("-key is required: verifying nothing is worse than sweeping nothing")
	}
	key, err := loadSigningKey(*keyPath)
	if err != nil {
		fatal("signing key: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		fatal("database: %v", err)
	}
	defer pool.Close()

	rooms, err := loadRooms(ctx, pool, *minState, *maxState)
	if err != nil {
		fatal("enumerate rooms: %v", err)
	}
	done := loadProgress(*progress)
	var todo []room
	for _, r := range rooms {
		if !done[r.ID] {
			todo = append(todo, r)
		}
	}
	if *limit > 0 && len(todo) > *limit {
		todo = todo[:*limit]
	}

	fmt.Printf("rooms: %d eligible, %d already done, %d to sweep\n", len(rooms), len(done), len(todo))
	if *dryRun {
		for _, r := range todo {
			fmt.Printf("  %-60s %d state events\n", r.ID, r.StateEvents)
		}
		return
	}
	if len(todo) == 0 {
		return
	}
	// Smallest first, so a sweep that is interrupted has still covered the
	// most rooms, and so a mistake shows up on a cheap room rather than after
	// a minute of Synapse's time.
	fmt.Printf("target: %s   interval: %s\n\n", target(*socket, *base), *interval)

	client := newClient(*socket, *timeout)
	prog := openProgress(*progress)
	defer prog.Close()

	var ok, failed int
	var slowest time.Duration
	start := time.Now()

	for i, r := range todo {
		if ctx.Err() != nil {
			fmt.Println("\ninterrupted")
			break
		}
		reqStart := time.Now()
		status, bytes, err := askState(ctx, client, key, *serverName, urlBase(*socket, *base), r.ID, r.EventID)
		took := time.Since(reqStart)
		if took > slowest {
			slowest = took
		}

		switch {
		case err != nil:
			failed++
			fmt.Printf("[%4d/%4d] %-58s ERROR %v\n", i+1, len(todo), short(r.ID), err)
		case status != http.StatusOK:
			// Not necessarily wrong: a room we have left, or one whose ACL
			// bars us, answers 403. The worker compares statuses too, so this
			// is still a comparison -- just not a body one.
			failed++
			fmt.Printf("[%4d/%4d] %-58s %d  (%s, %s)\n", i+1, len(todo), short(r.ID), status, humanBytes(bytes), took.Round(time.Millisecond))
			prog.record(r.ID)
		default:
			ok++
			fmt.Printf("[%4d/%4d] %-58s 200 %8s %8s  (%d state)\n", i+1, len(todo), short(r.ID),
				humanBytes(bytes), took.Round(time.Millisecond), r.StateEvents)
			prog.record(r.ID)
		}

		// Paced rather than parallel. Every request costs Synapse a full state
		// resolution, and the point is to accumulate evidence over time
		// without becoming the load problem this endpoint was avoided for.
		if i+1 < len(todo) {
			select {
			case <-time.After(*interval):
			case <-ctx.Done():
			}
		}
	}

	fmt.Printf("\nswept %d rooms in %s: %d answered, %d failed, slowest %s\n",
		ok+failed, time.Since(start).Round(time.Second), ok, failed, slowest.Round(time.Millisecond))
	fmt.Println("results are in the worker's diff log and gopro_shadow_results_total{endpoint=\"state\"}; " +
		"this program does not judge them")
}

type room struct {
	ID          string
	EventID     string
	StateEvents int
}

// loadRooms lists rooms with a forward extremity to ask about, smallest first.
func loadRooms(ctx context.Context, pool *pgxpool.Pool, minState, maxState int) ([]room, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.room_id, min(f.event_id), count(*)::int
		FROM current_state_events AS c
		JOIN event_forward_extremities AS f USING (room_id)
		JOIN rooms USING (room_id)
		GROUP BY c.room_id
		HAVING count(*) >= $1 AND ($2 = 0 OR count(*) <= $2)`, minState, maxState)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []room
	for rows.Next() {
		var r room
		if err := rows.Scan(&r.ID, &r.EventID, &r.StateEvents); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StateEvents < out[j].StateEvents })
	return out, rows.Err()
}

// signableRequest mirrors the JSON a federation signature covers. The shape is
// part of the protocol, not an implementation detail.
type signableRequest struct {
	Method      string          `json:"method"`
	URI         string          `json:"uri"`
	Origin      string          `json:"origin"`
	Destination string          `json:"destination"`
	Content     json.RawMessage `json:"content,omitempty"`
}

func askState(ctx context.Context, c *http.Client, key *federation.SigningKey, serverName, base, roomID, eventID string) (int, int64, error) {
	// Built once and reused for the signature and the request: the signature
	// covers the URI byte-for-byte, so re-encoding between the two turns every
	// request into a 401.
	uri := "/_matrix/federation/v1/state/" + url.PathEscape(roomID) +
		"?event_id=" + url.QueryEscape(eventID)

	sig, err := key.SignJSON(&signableRequest{
		Method:      http.MethodGet,
		URI:         uri,
		Origin:      serverName,
		Destination: serverName,
	})
	if err != nil {
		return 0, 0, err
	}
	auth := federation.XMatrixAuth{
		Origin:      serverName,
		Destination: serverName,
		KeyID:       key.ID,
		Signature:   sig,
	}.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+uri, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", auth)
	req.Host = serverName

	resp, err := c.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	// Drained but not held. These responses reach 100MB, and buffering one
	// here would reintroduce exactly the memory problem the endpoint was
	// rewritten to avoid -- in the tool built to verify that it was.
	n, err := io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, n, err
}

func newClient(socket string, timeout time.Duration) *http.Client {
	c := &http.Client{Timeout: timeout}
	if socket != "" {
		c.Transport = &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		}
	}
	return c
}

func urlBase(socket, base string) string {
	if base != "" {
		return strings.TrimSuffix(base, "/")
	}
	return "http://gopro-worker"
}

func target(socket, base string) string {
	if base != "" {
		return base
	}
	return "unix:" + socket
}

func loadSigningKey(path string) (*federation.SigningKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// A Synapse signing_key file may hold several keys, one per line: the
	// first is active, the rest retired but still published.
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return federation.ParseSynapseKey(line)
		}
	}
	return nil, fmt.Errorf("no key in %s", path)
}

// progressFile lets a sweep resume, so an interrupted run is not wasted and a
// long one can be spread across days.
type progressFile struct{ f *os.File }

func loadProgress(path string) map[string]bool {
	done := map[string]bool{}
	if path == "" {
		return done
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return done
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			done[line] = true
		}
	}
	return done
}

func openProgress(path string) *progressFile {
	if path == "" {
		return &progressFile{}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fatal("progress file: %v", err)
	}
	return &progressFile{f: f}
}

func (p *progressFile) record(id string) {
	if p.f != nil {
		fmt.Fprintln(p.f, id)
	}
}

func (p *progressFile) Close() {
	if p.f != nil {
		_ = p.f.Close()
	}
}

func short(id string) string {
	if len(id) > 56 {
		return id[:53] + "..."
	}
	return id
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "statesweep: "+format+"\n", args...)
	os.Exit(1)
}
