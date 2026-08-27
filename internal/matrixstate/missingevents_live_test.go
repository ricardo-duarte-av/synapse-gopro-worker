package matrixstate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"maunium.net/go/mautrix/federation"

	"github.com/daedric/synapse-gopro-worker/internal/pducmp"
)

// TestLiveGetMissingEvents compares our DAG walk against Synapse's on real
// rooms, in the shape real traffic takes: the requester names what it already
// has, so the walk completes rather than truncating.
//
// The distinction is not cosmetic. When the walk stops because it hit the
// limit, which events survive depends on the order the frontier is iterated --
// Synapse iterates a Python set, we iterate a sorted slice -- and the two
// answers are both valid subsets of the reachable events. Measured against the
// live server: 28 of 28 completed walks agreed exactly, while a truncated walk
// differed by one event. So this test deliberately excludes truncated walks,
// and the comparator must reclassify rather than fail them.
func TestLiveGetMissingEvents(t *testing.T) {
	r, s := liveResolver(t)
	key := liveSigningKey(t)
	ctx := context.Background()
	client := &http.Client{Timeout: time.Minute, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv("GOPRO_SYNAPSE_SOCK"))
		}}}

	// For each room: tip = forward extremity, earliest = an ancestor a few
	// hops back, so only a handful of events lie between them.
	rows, err := s.Pool().Query(ctx, `
		SELECT f.room_id, f.event_id FROM event_forward_extremities f
		JOIN rooms USING (room_id) LIMIT 30`)
	if err != nil {
		t.Fatal(err)
	}
	type tgt struct{ room, tip string }
	var targets []tgt
	for rows.Next() {
		var g tgt
		_ = rows.Scan(&g.room, &g.tip)
		targets = append(targets, g)
	}
	rows.Close()

	var agree, differ, notrunc int
	for _, g := range targets {
		// Walk back 4 hops ourselves to find an "earliest".
		chain, err := s.GetMissingEvents(ctx, g.room, nil, []string{g.tip}, 4)
		if err != nil || len(chain) == 0 {
			continue
		}
		earliest := []string{chain[0]}

		raw, _ := json.Marshal(map[string]any{
			"earliest_events": earliest, "latest_events": []string{g.tip}, "limit": 20})
		uri := "/_matrix/federation/v1/get_missing_events/" + url.PathEscape(g.room)
		sig, _ := key.SignJSON(&signableRequest{
			Method: "POST", URI: uri, Origin: ourServer, Destination: ourServer, Content: raw})
		req, _ := http.NewRequest("POST", "http://s"+uri, bytes.NewReader(raw))
		req.Header.Set("Authorization", federation.XMatrixAuth{
			Origin: ourServer, Destination: ourServer, KeyID: key.ID, Signature: sig}.String())
		req.Header.Set("Content-Type", "application/json")
		req.Host = ourServer
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		rb, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			continue
		}
		var syn struct {
			Events []json.RawMessage `json:"events"`
		}
		_ = json.Unmarshal(rb, &syn)

		ours, oerr := r.GetMissingEvents(ctx, ourServer, ourServer, g.room, earliest, []string{g.tip}, 20)
		if oerr != nil {
			t.Errorf("  %s: ours failed %v", g.room[:22], oerr)
			continue
		}
		if len(syn.Events) >= 20 {
			continue // still truncated; not the case under test
		}
		notrunc++

		index := func(raws []json.RawMessage) map[string]bool {
			m := map[string]bool{}
			for _, x := range raws {
				if c, ok := pducmp.Canonical(x); ok {
					m[string(c)] = true
				}
			}
			return m
		}
		sm, om := index(syn.Events), index(ours.Events)
		same := len(sm) == len(om)
		for k := range om {
			if !sm[k] {
				same = false
			}
		}
		if same {
			agree++
		} else {
			differ++
			t.Errorf("  %s synapse=%d ours=%d DIFFER (walk completed)", g.room[:22], len(syn.Events), len(ours.Events))
		}
	}
	t.Logf("  completed walks: %d, agree %d, differ %d", notrunc, agree, differ)
}
