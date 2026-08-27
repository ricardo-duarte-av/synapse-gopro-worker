package matrixstate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/federation"
)

// signableRequest mirrors mautrix's unexported struct of the same name. The
// signature covers exactly these fields, so the shape is part of the protocol
// rather than an implementation detail we are reaching into.
type signableRequest struct {
	Method      string          `json:"method"`
	URI         string          `json:"uri"`
	Origin      string          `json:"origin"`
	Destination string          `json:"destination"`
	Content     json.RawMessage `json:"content,omitempty"`
}

func liveSigningKey(t *testing.T) *federation.SigningKey {
	t.Helper()
	path := os.Getenv("GOPRO_SIGNING_KEY")
	if path == "" {
		t.Skip("GOPRO_SIGNING_KEY not set; skipping signed federation test")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("cannot read signing key: %v", err)
	}
	// A Synapse signing_key file may hold several keys, one per line: the
	// first is the active one and the rest are retired but still published.
	// Parsing the whole file at once fails on the line count rather than on
	// anything meaningful.
	var key *federation.SigningKey
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, err := federation.ParseSynapseKey(line)
		if err != nil {
			t.Fatalf("parse signing key: %v", err)
		}
		key = k
		break
	}
	if key == nil {
		t.Skip("signing key file is empty")
	}
	return key
}

// askSynapseForState makes a signed federation request and digests the
// response as it streams, never holding the body.
func askSynapseForState(t *testing.T, key *federation.SigningKey, base, roomID, eventID string) (StateResult, int) {
	t.Helper()

	// Built once and used for both the signature and the request: the
	// signature covers the URI byte-for-byte, so any re-encoding between the
	// two turns every request into a 401.
	uri := "/_matrix/federation/v1/state/" + url.PathEscape(roomID) +
		"?event_id=" + url.QueryEscape(eventID)

	auth, err := (&signableRequest{
		Method:      http.MethodGet,
		URI:         uri,
		Origin:      ourServer,
		Destination: ourServer,
	}).sign(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, base+uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", auth)
	req.Host = ourServer

	resp, err := (&http.Client{Timeout: 3 * time.Minute}).Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		t.Logf("    upstream %d: %s", resp.StatusCode, body)
		return StateResult{}, resp.StatusCode
	}
	res, err := DigestStateResponse(resp.Body)
	if err != nil {
		t.Fatalf("digest Synapse response: %v", err)
	}
	return res, resp.StatusCode
}

func (r *signableRequest) sign(key *federation.SigningKey) (string, error) {
	sig, err := key.SignJSON(r)
	if err != nil {
		return "", err
	}
	return federation.XMatrixAuth{
		Origin:      r.Origin,
		Destination: r.Destination,
		KeyID:       key.ID,
		Signature:   sig,
	}.String(), nil
}

// TestLiveStateAgainstSynapse is the promotion gate for /state.
//
// It asks the live server for real rooms and compares digests. The rooms are
// chosen to cover the two things most likely to be wrong: sheer size, and the
// depth filter that /state applies and /event does not.
//
// Nothing here is a committed fixture. The depth-invalid events are attack
// artefacts in a room that will eventually be purged, so this test finds its
// own targets from the database and skips what it cannot find.
func TestLiveStateAgainstSynapse(t *testing.T) {
	r, s := liveResolver(t)
	key := liveSigningKey(t)
	base := os.Getenv("GOPRO_LIVE_BASE")
	if base == "" {
		base = "https://" + ourServer
	}
	ctx := context.Background()

	// One forward extremity per room, for the rooms worth testing: the two
	// largest by state size, the room holding depth-invalid events, and a
	// spread of ordinary ones.
	rows, err := s.Pool().Query(ctx, `
		WITH target AS (
			(SELECT room_id, 'depth-invalid' AS why FROM events
			   WHERE depth > 9007199254740991 GROUP BY room_id)
			UNION
			(SELECT room_id, 'largest' FROM current_state_events
			   GROUP BY room_id ORDER BY count(*) DESC LIMIT 3)
			UNION
			(SELECT room_id, 'ordinary' FROM current_state_events
			   GROUP BY room_id HAVING count(*) BETWEEN 5 AND 200 LIMIT 6)
		)
		SELECT t.room_id, t.why, min(f.event_id)
		FROM target t
		JOIN event_forward_extremities f USING (room_id)
		JOIN rooms USING (room_id)
		GROUP BY t.room_id, t.why`)
	if err != nil {
		t.Fatal(err)
	}
	type target struct{ room, why, event string }
	var targets []target
	for rows.Next() {
		var tg target
		if err := rows.Scan(&tg.room, &tg.why, &tg.event); err != nil {
			t.Fatal(err)
		}
		targets = append(targets, tg)
	}
	rows.Close()
	if len(targets) == 0 {
		t.Skip("no targets found")
	}

	var agreed, disagreed int
	for _, tg := range targets {
		t.Run(tg.why+" "+tg.room, func(t *testing.T) {
			upstream, status := askSynapseForState(t, key, base, tg.room, tg.event)
			if status != http.StatusOK {
				t.Skipf("Synapse answered %d; nothing to compare", status)
			}

			start := time.Now()
			ours, err := r.State(ctx, io.Discard, ourServer, tg.room, tg.event)
			if err != nil {
				t.Fatalf("native /state: %v", err)
			}
			elapsed := time.Since(start)

			t.Logf("    pdus %d/%d  auth_chain %d/%d  bytes %s  ours %s",
				ours.PDUs, upstream.PDUs, ours.AuthChain, upstream.AuthChain,
				humanBytes(ours.Bytes), elapsed.Round(time.Millisecond))

			if !ours.Agrees(upstream) {
				disagreed++
				t.Errorf("digests disagree\n  pdus       ours=%d synapse=%d  %x / %x\n"+
					"  auth_chain ours=%d synapse=%d  %x / %x",
					ours.PDUs, upstream.PDUs, ours.PDUDigest[:8], upstream.PDUDigest[:8],
					ours.AuthChain, upstream.AuthChain, ours.AuthChainDigest[:8], upstream.AuthChainDigest[:8])
				return
			}
			agreed++
		})
	}
	t.Logf("  agreed on %d of %d rooms", agreed, agreed+disagreed)
}

func humanBytes(n int64) string {
	switch {
	case n > 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n > 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}
