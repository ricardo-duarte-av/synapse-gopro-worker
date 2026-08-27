package matrixstate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// canonicalJSON integer bounds, from Synapse's events/utils.py. A PDU whose
// depth falls outside them is dropped from /state responses entirely.
const (
	canonicalJSONMinInt = -(1 << 53) + 1
	canonicalJSONMaxInt = (1 << 53) - 1
)

// StateResult summarises a streamed /state response.
//
// There is deliberately no body here. The largest room on this deployment
// resolves to about 97MB of event JSON, and holding that per request is the
// thing that makes Synapse's /state cost ~53 seconds and drives its worker
// past half a gigabyte. Callers get a digest instead, which is enough to
// compare two responses without either side ever being resident.
type StateResult struct {
	// PDUs and AuthChain count what was actually written, after the depth
	// filter dropped anything Synapse would drop.
	PDUs      int
	AuthChain int
	Bytes     int64
	// Digest identifies the response contents independently of ordering. See
	// digestAccumulator.
	Digest [32]byte
}

// digestAccumulator builds an order-independent digest of a set of PDUs.
//
// Each event contributes sha256(event_id || 0 || body), and the contributions
// are summed modulo 2^256. Summing rather than XOR matters: XOR cancels
// duplicates, so a response containing the same event twice would digest
// identically to one omitting it entirely, and "the two sides disagree about
// duplicates" is exactly the sort of bug a comparator exists to catch.
//
// The cost of the ordering-independence is that a mismatch says only *that*
// the sides differ, not which event. That is the right trade here: /state
// receives 48 requests a day, so a second pass to diagnose a rare mismatch is
// affordable, whereas holding per-event hashes for a 145,000-event room across
// 16 concurrent comparisons is exactly the memory problem being avoided.
type digestAccumulator struct {
	sum   big.Int
	count int
}

func (d *digestAccumulator) add(eventID string, body []byte) {
	h := sha256.New()
	h.Write([]byte(eventID))
	h.Write([]byte{0})
	h.Write(body)
	var v big.Int
	v.SetBytes(h.Sum(nil))
	d.sum.Add(&d.sum, &v)
	d.count++
}

func (d *digestAccumulator) digest() [32]byte {
	var mod big.Int
	mod.Lsh(big.NewInt(1), 256)
	var r big.Int
	r.Mod(&d.sum, &mod)

	var out [32]byte
	b := r.Bytes()
	copy(out[32-len(b):], b)
	return out
}

// depthInCanonicalRange reports whether a serialised PDU's depth is within the
// range canonical JSON allows.
//
// Synapse drops out-of-range PDUs from /state (serialize_and_filter_pdus ->
// filter_pdus_for_valid_depth) but *not* from /event, which uses
// _transaction_dict_from_pdus and applies no such filter. The asymmetry is
// real and reachable: this deployment holds 31 such events, 24 of them
// m.room.member state events appearing in 1,918 state_groups_state rows.
//
// A PDU with no depth field at all is kept, matching Synapse's `"depth" in
// pdu` guard.
func depthInCanonicalRange(body []byte) bool {
	var probe struct {
		Depth json.RawMessage `json:"depth"`
	}
	if err := json.Unmarshal(body, &probe); err != nil || len(probe.Depth) == 0 {
		// Synapse reads depth off an already-parsed dict, so a body we cannot
		// parse is not a case it can encounter, and its `"depth" in pdu` guard
		// keeps a PDU that has no depth at all. Keeping it defers the decision
		// to the comparator rather than silently dropping an event.
		return true
	}

	// Deliberately checked on the raw token rather than by decoding into a
	// json.Number, which accepts a quoted string and would silently treat
	// "12" as 12. Synapse compares int to str there and raises TypeError, so
	// it neither keeps nor drops: it fails the whole request. We keep the PDU
	// and let the comparator report the difference, because inventing a
	// filtering rule Synapse does not have would hide the malformed event
	// instead of surfacing it.
	if probe.Depth[0] == '"' {
		return true
	}

	n, err := strconv.ParseInt(string(probe.Depth), 10, 64)
	if err != nil {
		// A depth outside int64 is outside the canonical range by definition;
		// a fractional one is not an integer and cannot be in range either.
		return false
	}
	return n >= canonicalJSONMinInt && n <= canonicalJSONMaxInt
}

// State answers /state for a remote server, streaming the response to w.
//
// The response is written as it is built, so peak memory is one batch of
// events rather than the whole room. w may be io.Discard when only the digest
// is wanted, which is how shadow comparison comes to be possible for an
// endpoint whose responses cannot be captured.
//
// Mirrors Synapse's _on_context_state_request_compute_with_events:
//
//	event_ids  = get_state_ids_for_pdu(room_id, event_id)
//	pdus       = get_events_as_list(event_ids)
//	auth_chain = get_auth_chain(room_id, [pdu.event_id for pdu in pdus])
//
// Two details there are easy to get wrong, and both differ from /state_ids:
//
//   - The auth chain is computed over the events actually *found*, not over
//     every state ID. An ID we do not hold is dropped by get_events_as_list
//     and so never contributes to the chain.
//   - It is computed before serialisation, so events later dropped by the
//     depth filter still contribute to it.
func (r *Resolver) State(ctx context.Context, w io.Writer, origin, roomID, eventID string) (StateResult, error) {
	var res StateResult

	if err := r.checkAccess(ctx, origin, roomID); err != nil {
		return res, err
	}

	stateIDs, err := r.stateIDsForEvent(ctx, roomID, eventID)
	if err != nil {
		return res, err
	}

	cw := &countingWriter{w: w}
	var digest digestAccumulator

	if _, err := io.WriteString(cw, `{"pdus":[`); err != nil {
		return res, err
	}

	// Collected before the depth filter, because the auth chain is computed
	// from what was found rather than from what is served.
	found := make([]string, 0, len(stateIDs))

	n, err := r.streamEvents(ctx, cw, &digest, stateIDs, &found)
	if err != nil {
		return res, err
	}
	res.PDUs = n

	if _, err := io.WriteString(cw, `],"auth_chain":[`); err != nil {
		return res, err
	}

	authIDs, usedFallback, err := r.db.GetAuthChainIDsWithFallback(ctx, roomID, found)
	if err != nil {
		return res, fmt.Errorf("auth chain: %w", err)
	}
	if usedFallback {
		authChainFallbacks.Inc()
	}

	n, err = r.streamEvents(ctx, cw, &digest, authIDs, nil)
	if err != nil {
		return res, err
	}
	res.AuthChain = n

	if _, err := io.WriteString(cw, `]}`); err != nil {
		return res, err
	}

	res.Bytes = cw.n
	res.Digest = digest.digest()
	return res, nil
}

// streamEvents writes the serialised PDUs for ids as a JSON array body,
// without the enclosing brackets, and reports how many it wrote.
//
// When found is non-nil, every event we hold is appended to it before the
// depth filter is applied.
func (r *Resolver) streamEvents(ctx context.Context, w io.Writer, digest *digestAccumulator, ids []string, found *[]string) (int, error) {
	written := 0
	err := r.db.GetEventsBatched(ctx, ids, store.DefaultEventBatch, func(events map[string]*store.Event) error {
		// Iterate the requested IDs rather than the map, so output order
		// follows the input and does not vary with Go's map iteration. The
		// comparator treats these as sets, but a stable order makes a captured
		// response reproducible.
		for _, id := range ids {
			ev, ok := events[id]
			if !ok {
				continue
			}
			if found != nil {
				*found = append(*found, id)
			}

			body, err := r.serialisePDU(ev)
			if err != nil {
				return err
			}
			if !depthInCanonicalRange(body) {
				continue
			}

			if written > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			if _, err := w.Write(body); err != nil {
				return err
			}
			digest.add(id, body)
			written++
		}
		return nil
	})
	return written, err
}

// countingWriter tracks how much was written without holding any of it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// serialisePDU renders one stored event as /state serialises it.
//
// Deliberately not shared with /event, which differs in two ways that both
// matter:
//
//   - No history-visibility filtering. /state and /state_ids do not apply it
//     at all; only /event and backfill do.
//   - No age_ts -> age conversion, because serialize_and_filter_pdus passes
//     time_now=None. See applyPDUJSONRulesAt.
//
// A redacted event is still served redacted: redaction is how a user deletes a
// message, and it is applied on read regardless of endpoint.
func (r *Resolver) serialisePDU(ev *store.Event) ([]byte, error) {
	body := ev.JSON
	if ev.IsRedacted() {
		redacted, err := redactEvent(ev)
		if err != nil {
			return nil, fmt.Errorf("redact %s: %w", ev.EventID, err)
		}
		body = redacted
	}
	out, err := applyPDUJSONRulesAt(body, nil)
	if err != nil {
		return nil, fmt.Errorf("prepare pdu %s: %w", ev.EventID, err)
	}
	return out, nil
}
