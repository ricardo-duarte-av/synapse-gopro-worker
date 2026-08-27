// Command eventsweep drives an /event soak across the paths that carry risk.
//
// /event has the cleanest record of any endpoint here -- tens of thousands of
// comparisons without a content disagreement -- but that record covers only
// the events remote servers happened to ask for, which is a small and
// self-selecting slice of the database. The padded-hash bug found by the
// /state sweep lived in redactEvent, shared with /event, and had never fired
// there in over 100,000 requests. It was not rare because the code was rarely
// wrong; it was rare because nobody had asked for those eight events.
//
// So this samples by *risk surface* rather than by volume. Sweeping 7.7M
// events uniformly would take weeks and spend almost all of it re-testing the
// path that already has the most evidence behind it. The strata below are the
// places where this project has actually found bugs, plus the format corners
// that would break silently.
//
// The single most valuable stratum is `invisible`: events that predate our own
// server's join in rooms whose history visibility is `joined` or `invited`.
// Roughly 347,000 exist. Synapse must return those redacted, and if our
// filter_events_for_server is wrong we serve private room history to a server
// that should not see it -- the one failure on this endpoint that cannot be
// taken back. It is reachable with our own signing key, because our membership
// has a beginning: everything before it is invisible to us too.
//
// Like statesweep, this generates traffic and does not judge it. /event runs
// at canary:100, so each request is served natively and then verified against
// Synapse by the worker itself, on the real code path, with any disagreement
// landing in the diff log.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
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

// stratum is one risk area, and the query that finds events in it.
//
// Every query is bounded and given its own statement timeout: this runs
// read-only against a production database, and a stratum that cannot be
// sampled cheaply is skipped rather than allowed to sit on the server. A full
// scan of event_json has already been seen to time out, so nothing here
// depends on one completing.
type stratum struct {
	Name  string
	Why   string
	Query string
}

var strata = []stratum{
	{
		Name: "invisible",
		Why:  "predates our own join in a joined/invited room; Synapse must redact these for us",
		Query: `
			WITH vis AS (
			  SELECT c.room_id
			  FROM current_state_events c JOIN event_json ej USING (event_id)
			  WHERE c.type = 'm.room.history_visibility'
			    AND split_part(split_part(ej.json, '"history_visibility":"', 2), '"', 1)
			        IN ('joined','invited')
			),
			ourjoin AS (
			  SELECT e.room_id, min(e.stream_ordering) AS first_join
			  FROM current_state_events c
			  JOIN events e ON e.event_id = c.event_id
			  JOIN room_memberships rm ON rm.event_id = c.event_id
			  WHERE rm.user_id LIKE '%:' || $2 AND rm.membership = 'join'
			  GROUP BY e.room_id
			)
			SELECT event_id FROM (
			  SELECT e.event_id,
			         row_number() OVER (PARTITION BY e.room_id
			                            ORDER BY e.stream_ordering DESC) AS rn
			  FROM events e
			  JOIN vis USING (room_id) JOIN ourjoin j USING (room_id)
			  WHERE e.stream_ordering < j.first_join
			) t WHERE rn <= 10 LIMIT $1`,
	},
	{
		Name: "redacted",
		Why:  "redaction is applied on read; getting it wrong serves deleted content",
		Query: `SELECT redacts FROM redactions WHERE redacts IS NOT NULL
		         ORDER BY random() LIMIT $1`,
	},
	{
		Name: "ban_redacted",
		Why:  "MSC4293 redact-on-ban: no redaction event, no redactions row, stored JSON intact",
		Query: `
			SELECT e.event_id FROM room_ban_redactions b
			JOIN events e ON e.room_id = b.room_id AND e.sender = b.user_id
			WHERE e.type <> 'm.room.create'
			ORDER BY random() LIMIT $1`,
	},
	{
		Name:  "rejected",
		Why:   "/event serves rejected events, unlike the state endpoints",
		Query: `SELECT event_id FROM rejections ORDER BY random() LIMIT $1`,
	},
	{
		Name: "outlier",
		Why:  "the outlier flag is a column, not internal_metadata; reading it wrongly hides nothing here but is easy to get wrong",
		Query: `SELECT event_id FROM events TABLESAMPLE SYSTEM (0.5)
		         WHERE outlier LIMIT $1`,
	},
	{
		Name: "erased",
		Why:  "events from erased users must come back stripped",
		Query: `
			SELECT e.event_id FROM erased_users u
			JOIN events e ON e.sender = u.user_id
			ORDER BY random() LIMIT $1`,
	},
	{
		Name: "rare_room_version",
		Why:  "v1/v2 use a different PDU format; v12 create events carry no room_id",
		Query: `
			SELECT event_id FROM (
			  SELECT e.event_id,
			         row_number() OVER (PARTITION BY r.room_version
			                            ORDER BY e.stream_ordering DESC) AS rn
			  FROM events e JOIN rooms r USING (room_id)
			  WHERE r.room_version IN ('1','2','3','4','7','12')
			) t WHERE rn <= 60 LIMIT $1`,
	},
	{
		Name: "create_events",
		Why:  "m.room.create is special in v12 and is never ban-redacted",
		Query: `
			SELECT event_id FROM events WHERE type = 'm.room.create'
			ORDER BY random() LIMIT $1`,
	},
	{
		Name: "state_events",
		Why:  "state events carry replaces_state and prev_content through the unsigned allowlist",
		Query: `
			SELECT event_id FROM events TABLESAMPLE SYSTEM (0.5)
			WHERE state_key IS NOT NULL LIMIT $1`,
	},
	{
		Name:  "random",
		Why:   "baseline, so the sweep is not only corners",
		Query: `SELECT event_id FROM events TABLESAMPLE SYSTEM (0.2) LIMIT $1`,
	},
}

func main() {
	var (
		dsn         = flag.String("dsn", "host=/var/sockets user=gopro_ro dbname=synapse-db", "read-only PostgreSQL DSN")
		keyPath     = flag.String("key", "", "path to the homeserver signing key (required)")
		socket      = flag.String("socket", "/var/sockets/nginx/av-gopro-worker-1.sock", "gopro-worker unix socket")
		base        = flag.String("base", "", "HTTP base URL instead of the socket")
		serverName  = flag.String("server-name", "aguiarvieira.pt", "our server name; used as request origin and destination")
		perStratum  = flag.Int("per-stratum", 300, "events to sample from each stratum")
		only        = flag.String("strata", "", "comma-separated stratum names; default all")
		interval    = flag.Duration("interval", 2*time.Second, "minimum gap between requests")
		timeout     = flag.Duration("timeout", time.Minute, "per-request timeout")
		queryBudget = flag.Duration("query-timeout", 30*time.Second, "per-stratum SQL budget; a stratum that exceeds it is skipped")
		progress    = flag.String("progress", "", "file recording completed events, so a sweep can resume")
		dryRun      = flag.Bool("dry-run", false, "sample and report counts without sending anything")
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

	selected := map[string]bool{}
	for _, n := range strings.Split(*only, ",") {
		if n = strings.TrimSpace(n); n != "" {
			selected[n] = true
		}
	}

	done := loadProgress(*progress)
	type target struct{ stratum, eventID string }
	var todo []target
	seen := map[string]bool{}

	for _, st := range strata {
		if len(selected) > 0 && !selected[st.Name] {
			continue
		}
		ids, err := sample(ctx, pool, st, *perStratum, *serverName, *queryBudget)
		if err != nil {
			fmt.Printf("  %-18s SKIPPED: %v\n", st.Name, err)
			continue
		}
		added := 0
		for _, id := range ids {
			// An event can belong to several strata; sweep it once, under the
			// first that claimed it, so the totals are not inflated.
			if seen[id] || done[id] {
				continue
			}
			seen[id] = true
			todo = append(todo, target{st.Name, id})
			added++
		}
		fmt.Printf("  %-18s %5d sampled, %4d new   (%s)\n", st.Name, len(ids), added, st.Why)
	}

	fmt.Printf("\n%d events to sweep, %d already done\n", len(todo), len(done))
	if *dryRun || len(todo) == 0 {
		return
	}

	// Shuffled so the request stream is not ordered by stratum. A long run of
	// redacted events in a row would warm caches in a way real traffic never
	// does, and would make a timing anomaly hard to attribute.
	rand.Shuffle(len(todo), func(i, j int) { todo[i], todo[j] = todo[j], todo[i] })

	fmt.Printf("target: %s   interval: %s\n\n", target2(*socket, *base), *interval)
	client := newClient(*socket, *timeout)
	prog := openProgress(*progress)
	defer prog.Close()

	counts := map[string]map[int]int{}
	start := time.Now()
	for i, tg := range todo {
		if ctx.Err() != nil {
			fmt.Println("\ninterrupted")
			break
		}
		status, bytes, err := askEvent(ctx, client, key, *serverName, urlBase(*socket, *base), tg.eventID)
		if err != nil {
			fmt.Printf("[%5d/%5d] %-18s %-46s ERROR %v\n", i+1, len(todo), tg.stratum, short(tg.eventID), err)
		} else {
			if counts[tg.stratum] == nil {
				counts[tg.stratum] = map[int]int{}
			}
			counts[tg.stratum][status]++
			prog.record(tg.eventID)
			if (i+1)%25 == 0 || status != http.StatusOK {
				fmt.Printf("[%5d/%5d] %-18s %-46s %d %s\n", i+1, len(todo), tg.stratum, short(tg.eventID), status, humanBytes(bytes))
			}
		}
		if i+1 < len(todo) {
			select {
			case <-time.After(*interval):
			case <-ctx.Done():
			}
		}
	}

	fmt.Printf("\nswept in %s\n", time.Since(start).Round(time.Second))
	for _, st := range strata {
		if c, ok := counts[st.Name]; ok {
			fmt.Printf("  %-18s %v\n", st.Name, c)
		}
	}
	fmt.Println("results are in the worker's diff log and gopro_shadow_results_total; this program does not judge them")
}

func sample(ctx context.Context, pool *pgxpool.Pool, st stratum, n int, serverName string, budget time.Duration) ([]string, error) {
	qctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	args := []any{n}
	if strings.Contains(st.Query, "$2") {
		args = append(args, serverName)
	}
	rows, err := pool.Query(qctx, st.Query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
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

func askEvent(ctx context.Context, c *http.Client, key *federation.SigningKey, serverName, base, eventID string) (int, int64, error) {
	uri := "/_matrix/federation/v1/event/" + url.PathEscape(eventID)
	sig, err := key.SignJSON(&signableRequest{
		Method: http.MethodGet, URI: uri, Origin: serverName, Destination: serverName,
	})
	if err != nil {
		return 0, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+uri, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", federation.XMatrixAuth{
		Origin: serverName, Destination: serverName, KeyID: key.ID, Signature: sig,
	}.String())
	req.Host = serverName

	resp, err := c.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
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

func target2(socket, base string) string {
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
	if len(s) > 44 {
		return s[:41] + "..."
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
	fmt.Fprintf(os.Stderr, "eventsweep: "+format+"\n", args...)
	os.Exit(1)
}
