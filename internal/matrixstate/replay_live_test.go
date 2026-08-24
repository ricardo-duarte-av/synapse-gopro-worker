package matrixstate

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
)

// TestLiveReplayDiffLog re-runs recorded mismatches against the current code.
//
// The diff log stores Synapse's response alongside the request that produced
// it, which makes it a regression corpus of real disagreements. Replaying it
// answers the only question that matters after a fix: do the requests that
// previously disagreed now agree?
//
// Set GOPRO_DIFF_LOG to the diffs.jsonl to replay.
func TestLiveReplayDiffLog(t *testing.T) {
	path := os.Getenv("GOPRO_DIFF_LOG")
	if path == "" {
		t.Skip("GOPRO_DIFF_LOG not set; skipping diff log replay")
	}
	r, _ := liveResolver(t)
	ctx := context.Background()

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	type record struct {
		Kind        string          `json:"kind"`
		Endpoint    string          `json:"endpoint"`
		Origin      string          `json:"origin"`
		EventID     string          `json:"event_id"`
		SynapseBody json.RawMessage `json:"synapse_body"`
	}

	var replayed, nowAgree, stillDiffer, skipped int
	firstDiffs := 0

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var rec record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue
		}
		if rec.Endpoint != "event" || rec.Kind != "body_mismatch" || len(rec.SynapseBody) == 0 {
			skipped++
			continue
		}

		resp, err := r.Event(ctx, rec.Origin, ourServer, rec.EventID)
		if err != nil {
			skipped++
			continue
		}
		nativeBody, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		replayed++

		if pdusAgree(t, rec.SynapseBody, nativeBody) {
			nowAgree++
			continue
		}
		stillDiffer++
		if firstDiffs < 3 {
			firstDiffs++
			t.Errorf("still differs: %s\n synapse: %s\n native:  %s",
				rec.EventID, firstPDU(rec.SynapseBody), firstPDU(nativeBody))
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	t.Logf("replayed %d recorded mismatches: %d now agree, %d still differ (%d skipped)",
		replayed, nowAgree, stillDiffer, skipped)

	if replayed == 0 {
		t.Skip("no replayable /event mismatches in the log")
	}
	if stillDiffer > 0 {
		t.Errorf("%d of %d recorded mismatches still differ", stillDiffer, replayed)
	}
}

// pdusAgree compares the PDUs of two transaction bodies, ignoring the
// wall-clock-dependent age and origin_server_ts values while still requiring
// both sides to agree on whether age is present.
func pdusAgree(t *testing.T, synapseBody, nativeBody []byte) bool {
	t.Helper()
	syn := normaliseTransaction(synapseBody)
	nat := normaliseTransaction(nativeBody)
	a, _ := json.Marshal(syn)
	b, _ := json.Marshal(nat)
	return string(a) == string(b)
}

func normaliseTransaction(body []byte) []any {
	var tx struct {
		PDUs []map[string]any `json:"pdus"`
	}
	if err := json.Unmarshal(body, &tx); err != nil {
		return nil
	}
	out := make([]any, 0, len(tx.PDUs))
	for _, pdu := range tx.PDUs {
		if unsigned, ok := pdu["unsigned"].(map[string]any); ok {
			if _, has := unsigned["age"]; has {
				unsigned["age"] = "<age>"
			}
		}
		out = append(out, pdu)
	}
	return out
}

func firstPDU(body []byte) string {
	var tx struct {
		PDUs []json.RawMessage `json:"pdus"`
	}
	if err := json.Unmarshal(body, &tx); err != nil || len(tx.PDUs) == 0 {
		return string(body)
	}
	s := string(tx.PDUs[0])
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}
