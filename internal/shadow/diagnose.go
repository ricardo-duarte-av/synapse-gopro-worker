package shadow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"

	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
	"github.com/tidwall/gjson"
)

// StateReplayer reproduces one side of a /state answer into w.
//
// Both sides are replayed rather than remembered. A /state response reaches
// about 97MB here, so the comparison that found the mismatch could not hold
// either body, and neither can this.
type StateReplayer func(ctx context.Context, req Request, w io.Writer) error

// EventIdentity is the little that can be said about a PDU without holding it.
//
// Deliberately not the event ID: room version 3 and later carry none in the
// response -- the ID is the reference hash -- so naming events by ID would
// work on some rooms and silently not others. Type and state key are what a
// person needs to go and look, and they are present in every room version.
type EventIdentity struct {
	Type           string `json:"type"`
	StateKey       string `json:"state_key,omitempty"`
	Sender         string `json:"sender"`
	OriginServerTS int64  `json:"origin_server_ts,omitempty"`
	Depth          int64  `json:"depth,omitempty"`
}

// ArrayDiagnosis names the events behind one array's disagreement.
type ArrayDiagnosis struct {
	// Counts are exact even when the samples below are truncated.
	NativeOnly  int `json:"native_only"`
	SynapseOnly int `json:"synapse_only"`

	NativeSamples  []EventIdentity `json:"native_samples,omitempty"`
	SynapseSamples []EventIdentity `json:"synapse_samples,omitempty"`
}

// StateDiagnosis is the second pass over a /state digest mismatch.
type StateDiagnosis struct {
	PDUs      ArrayDiagnosis `json:"pdus"`
	AuthChain ArrayDiagnosis `json:"auth_chain"`
}

// Empty reports that both arrays agreed after all, which would mean the two
// passes saw different answers rather than that the mismatch was imagined.
func (d StateDiagnosis) Empty() bool {
	return d.PDUs.NativeOnly == 0 && d.PDUs.SynapseOnly == 0 &&
		d.AuthChain.NativeOnly == 0 && d.AuthChain.SynapseOnly == 0
}

// defaultDiagnosisSamples bounds how many events of each kind are named.
//
// A mismatch that spans thousands of events is a systematic fault, and the
// first handful identifies it as surely as all of them would. The counts are
// always exact; only the naming is capped.
const defaultDiagnosisSamples = 10

// DiagnoseStateMismatch names the events behind a /state digest disagreement.
//
// The digest that detects a mismatch cannot explain it: it is a sum of
// per-event hashes, so it says only that two multisets differ. This runs
// afterwards, and only on a mismatch, to turn that into a list of events.
//
// Two passes, because one would not be bounded. The first hashes every PDU on
// both sides and cancels matching hashes against each other, leaving only the
// residue -- which is the disagreement, and is normally tiny. The second pass
// re-reads both sides and captures an identity for the events whose hash is in
// that residue. Holding an identity for every event instead would be one pass
// but tens of megabytes on the largest rooms, which is the memory problem this
// endpoint exists to avoid.
//
// Cost is four streamed responses, paid only when something is already wrong.
func DiagnoseStateMismatch(ctx context.Context, req Request, native, synapse StateReplayer, samples int) (*StateDiagnosis, error) {
	if native == nil || synapse == nil {
		return nil, fmt.Errorf("shadow: /state diagnosis needs both replayers")
	}
	if samples <= 0 {
		samples = defaultDiagnosisSamples
	}

	// Pass 1: hash both sides, cancelling as we go.
	//
	// Deleting on zero is what keeps this to the size of the difference once
	// both sides have been read, rather than the size of the response.
	residue := map[string]map[[32]byte]int{
		matrixstate.ArrayPDUs:      {},
		matrixstate.ArrayAuthChain: {},
	}
	tally := func(sign int) matrixstate.PDUVisitor {
		return func(array string, _ int, canonical []byte, _ json.RawMessage) error {
			m, ok := residue[array]
			if !ok {
				return nil
			}
			h := sha256.Sum256(canonical)
			m[h] += sign
			if m[h] == 0 {
				delete(m, h)
			}
			return nil
		}
	}

	if err := replay(ctx, req, native, tally(+1)); err != nil {
		return nil, fmt.Errorf("replay native: %w", err)
	}
	if err := replay(ctx, req, synapse, tally(-1)); err != nil {
		return nil, fmt.Errorf("replay synapse: %w", err)
	}

	var d StateDiagnosis
	for array, m := range residue {
		a := arrayFor(&d, array)
		for _, n := range m {
			if n > 0 {
				a.NativeOnly += n
			} else {
				a.SynapseOnly += -n
			}
		}
	}
	if d.Empty() {
		// Both sides agreed this time. Reported rather than swallowed: it means
		// the answer is not stable, which is a different and worse problem than
		// a reproducible difference.
		return &d, nil
	}

	// Pass 2: name the residue. A working copy is decremented as events are
	// captured so an event duplicated in one array is not reported twice for a
	// single occurrence.
	collect := func(sign int, pick func(*ArrayDiagnosis) *[]EventIdentity) matrixstate.PDUVisitor {
		remaining := map[string]map[[32]byte]int{}
		for array, m := range residue {
			c := make(map[[32]byte]int, len(m))
			for h, n := range m {
				if n*sign > 0 {
					c[h] = n * sign
				}
			}
			remaining[array] = c
		}
		return func(array string, _ int, canonical []byte, raw json.RawMessage) error {
			m, ok := remaining[array]
			if !ok {
				return nil
			}
			h := sha256.Sum256(canonical)
			if m[h] <= 0 {
				return nil
			}
			m[h]--
			out := pick(arrayFor(&d, array))
			if len(*out) < samples {
				*out = append(*out, identify(raw))
			}
			return nil
		}
	}

	if err := replay(ctx, req, native, collect(+1, func(a *ArrayDiagnosis) *[]EventIdentity { return &a.NativeSamples })); err != nil {
		return &d, fmt.Errorf("identify native: %w", err)
	}
	if err := replay(ctx, req, synapse, collect(-1, func(a *ArrayDiagnosis) *[]EventIdentity { return &a.SynapseSamples })); err != nil {
		return &d, fmt.Errorf("identify synapse: %w", err)
	}
	return &d, nil
}

// arrayFor maps an array name onto the diagnosis field it fills.
func arrayFor(d *StateDiagnosis, array string) *ArrayDiagnosis {
	if array == matrixstate.ArrayAuthChain {
		return &d.AuthChain
	}
	return &d.PDUs
}

// replay streams one side through the scanner, without buffering it.
//
// The pipe is always drained, including after the scanner stops early. Leaving
// a replayer blocked writing into a pipe nobody reads is how a diagnosis would
// come to hold up the goroutine it runs on for as long as the writer is
// willing to wait.
func replay(ctx context.Context, req Request, side StateReplayer, visit matrixstate.PDUVisitor) error {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		defer func() {
			_, _ = io.Copy(io.Discard, pr)
			_ = pr.Close()
		}()
		_, _, err := matrixstate.ScanStateResponse(pr, visit)
		done <- err
	}()

	writeErr := side(ctx, req, pw)
	_ = pw.Close()
	scanErr := <-done

	if scanErr != nil {
		return scanErr
	}
	return writeErr
}

// identify extracts what can be said about a PDU from the bytes at hand.
func identify(raw json.RawMessage) EventIdentity {
	return EventIdentity{
		Type:           gjson.GetBytes(raw, "type").Str,
		StateKey:       gjson.GetBytes(raw, "state_key").Str,
		Sender:         gjson.GetBytes(raw, "sender").Str,
		OriginServerTS: gjson.GetBytes(raw, "origin_server_ts").Int(),
		Depth:          gjson.GetBytes(raw, "depth").Int(),
	}
}
