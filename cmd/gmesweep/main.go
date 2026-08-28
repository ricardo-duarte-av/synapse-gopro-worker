// Command gmesweep drives a /get_missing_events soak across every room.
//
// Shadow mode gave this endpoint 237 clean comparisons in three hours, but
// across only twelve rooms: eighty-eight servers asking about a dozen rooms.
// The risk surface here is per-room -- history visibility, server ACLs, room
// versions, rejected events reachable through the DAG -- so depth in twelve
// rooms exercises almost none of it. That is the same shape of coverage that
// let the /state sweep find two bugs which the hand-picked large rooms had
// missed.
//
// Each request is built to look like real traffic rather than like a probe.
// The requester names an ancestor it already has, so the walk *completes*
// instead of stopping at the limit -- which matters, because a truncated walk
// has no single correct answer and is reclassified rather than compared. The
// overnight logs show real servers behave this way: median earliest_events
// around three to five, and only one request in a night sent an empty list.
//
// Like the other sweeps this generates traffic and does not judge it. The
// worker compares, on the real code path, and records to the diff log.
package main

import (
	"bytes"
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
		serverName = flag.String("server-name", "aguiarvieira.pt", "our server name; request origin and destination")
		depth      = flag.Int("depth", 6, "how many hops back to claim as already-held, so the walk completes")
		limit      = flag.Int("limit", 20, "limit sent in the request body")
		interval   = flag.Duration("interval", time.Second, "minimum gap between requests")
		maxRooms   = flag.Int("limit-rooms", 0, "stop after this many rooms; 0 for all")
		progress   = flag.String("progress", "", "file recording completed rooms, so a sweep can resume")
		dryRun     = flag.Bool("dry-run", false, "list targets and exit")
	)
	flag.Parse()

	if *keyPath == "" {
		fatal("-key is required")
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

	targets, err := loadTargets(ctx, pool, *depth)
	if err != nil {
		fatal("enumerate rooms: %v", err)
	}
	done := loadProgress(*progress)
	var todo []target
	for _, t := range targets {
		if !done[t.Room] {
			todo = append(todo, t)
		}
	}
	if *maxRooms > 0 && len(todo) > *maxRooms {
		todo = todo[:*maxRooms]
	}

	fmt.Printf("rooms: %d with a usable ancestor, %d already done, %d to sweep\n",
		len(targets), len(done), len(todo))
	if *dryRun || len(todo) == 0 {
		return
	}
	fmt.Printf("depth: %d hops   limit: %d   interval: %s\n\n", *depth, *limit, *interval)

	client := &http.Client{Timeout: time.Minute, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
		},
	}}
	prog := openProgress(*progress)
	defer prog.Close()

	counts := map[int]int{}
	start := time.Now()
	for i, t := range todo {
		if ctx.Err() != nil {
			fmt.Println("\ninterrupted")
			break
		}
		status, bytesRead, err := ask(ctx, client, key, *serverName, t, *limit)
		switch {
		case err != nil:
			fmt.Printf("[%4d/%4d] %-52s ERROR %v\n", i+1, len(todo), short(t.Room), err)
		default:
			counts[status]++
			prog.record(t.Room)
			if status != http.StatusOK || (i+1)%25 == 0 {
				fmt.Printf("[%4d/%4d] %-52s %d %s\n", i+1, len(todo), short(t.Room), status, humanBytes(bytesRead))
			}
		}
		if i+1 < len(todo) {
			select {
			case <-time.After(*interval):
			case <-ctx.Done():
			}
		}
	}

	fmt.Printf("\nswept in %s: %v\n", time.Since(start).Round(time.Second), counts)
	fmt.Println("results are in the worker's diff log and " +
		"gopro_shadow_results_total{endpoint=\"get_missing_events\"}; this program does not judge them")
}

type target struct {
	Room     string
	Latest   string
	Earliest string
}

// loadTargets finds, per room, a forward extremity and an ancestor `depth`
// hops behind it.
//
// The ancestor is what makes the walk complete rather than truncate. Without
// one the comparator reclassifies the answer as walk_truncated -- correctly,
// since the surviving events then depend on an iteration order neither side
// controls -- and the sweep would exercise the endpoint while verifying
// nothing.
func loadTargets(ctx context.Context, pool *pgxpool.Pool, depth int) ([]target, error) {
	rows, err := pool.Query(ctx, `
		WITH RECURSIVE tips AS (
		  SELECT DISTINCT ON (room_id) room_id, event_id
		  FROM event_forward_extremities
		  ORDER BY room_id, event_id
		),
		back AS (
		  SELECT t.room_id, t.event_id AS tip, t.event_id AS cur, 0 AS hop
		  FROM tips t
		  UNION ALL
		  SELECT b.room_id, b.tip, ee.prev_event_id, b.hop + 1
		  FROM back b
		  JOIN event_edges ee ON ee.event_id = b.cur AND NOT ee.is_state
		  WHERE b.hop < $1
		)
		SELECT room_id, tip, min(cur)
		FROM back WHERE hop = $1
		GROUP BY room_id, tip`, depth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []target
	for rows.Next() {
		var t target
		if err := rows.Scan(&t.Room, &t.Latest, &t.Earliest); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

type signableRequest struct {
	Method      string          `json:"method"`
	URI         string          `json:"uri"`
	Origin      string          `json:"origin"`
	Destination string          `json:"destination"`
	Content     json.RawMessage `json:"content,omitempty"`
}

func ask(ctx context.Context, c *http.Client, key *federation.SigningKey, serverName string, t target, limit int) (int, int64, error) {
	body, err := json.Marshal(map[string]any{
		"earliest_events": []string{t.Earliest},
		"latest_events":   []string{t.Latest},
		"limit":           limit,
	})
	if err != nil {
		return 0, 0, err
	}

	// Built once and used for both the signature and the request: the
	// signature covers the URI and the content byte-for-byte.
	uri := "/_matrix/federation/v1/get_missing_events/" + url.PathEscape(t.Room)
	sig, err := key.SignJSON(&signableRequest{
		Method: http.MethodPost, URI: uri, Origin: serverName,
		Destination: serverName, Content: body,
	})
	if err != nil {
		return 0, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://gopro-worker"+uri, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", federation.XMatrixAuth{
		Origin: serverName, Destination: serverName, KeyID: key.ID, Signature: sig,
	}.String())
	req.Header.Set("Content-Type", "application/json")
	req.Host = serverName

	resp, err := c.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, n, err
}

func loadSigningKey(path string) (*federation.SigningKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return federation.ParseSynapseKey(line)
		}
	}
	return nil, fmt.Errorf("no key in %s", path)
}

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

func short(s string) string {
	if len(s) > 50 {
		return s[:47] + "..."
	}
	return s
}

func humanBytes(n int64) string {
	if n >= 1<<10 {
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gmesweep: "+format+"\n", args...)
	os.Exit(1)
}
