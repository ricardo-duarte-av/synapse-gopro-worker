package matrixstate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"

	"github.com/daedric/synapse-gopro-worker/internal/pducmp"
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
	// PDUDigest and AuthChainDigest identify each array's contents
	// independently of ordering. See digestAccumulator.
	//
	// Kept separate on purpose. One digest spanning both arrays would match
	// even if an event were placed in the wrong one, and "we put it in
	// auth_chain and Synapse put it in pdus" is exactly the sort of
	// disagreement worth catching -- the two arrays are built by different
	// queries and only the first is filtered by state resolution.
	PDUDigest       [32]byte
	AuthChainDigest [32]byte
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

// add folds in one PDU, which must already be in canonical form.
//
// The event ID is deliberately not part of the contribution. Synapse's
// response carries no event_id for room version 3 and later -- the ID is the
// event's reference hash -- so a digest keyed on it could not be computed from
// the far side without hashing every event again. Nothing is lost by leaving
// it out: the ID is a function of the body, so identical bodies have identical
// IDs.
func (d *digestAccumulator) add(canonical []byte) {
	h := sha256.New()
	h.Write(canonical)
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
	var pduDigest, authDigest digestAccumulator

	if _, err := io.WriteString(cw, `{"pdus":[`); err != nil {
		return res, err
	}

	// Collected before the depth filter, because the auth chain is computed
	// from what was found rather than from what is served.
	found := make([]string, 0, len(stateIDs))

	n, err := r.streamEvents(ctx, cw, &pduDigest, stateIDs, &found)
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

	n, err = r.streamEvents(ctx, cw, &authDigest, authIDs, nil)
	if err != nil {
		return res, err
	}
	res.AuthChain = n

	if _, err := io.WriteString(cw, `]}`); err != nil {
		return res, err
	}

	res.Bytes = cw.n
	res.PDUDigest = pduDigest.digest()
	res.AuthChainDigest = authDigest.digest()
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

			// Digested in canonical form, but written to the client verbatim.
			// The stored JSON is spliced rather than re-encoded on purpose:
			// 14,654 events here carry an escaped NUL, and a response is not
			// the place to discover that a round trip altered one.
			canonical, ok := pducmp.Canonical(body)
			if !ok {
				return fmt.Errorf("canonicalise %s: unparseable event JSON", id)
			}
			if emitsCachePollution(body) {
				statePrevContentEmitted.Inc()
			}

			if written > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			if _, err := w.Write(body); err != nil {
				return err
			}
			digest.add(canonical)
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

// emitsCachePollution reports that a PDU we are about to serve carries
// prev_content or prev_sender.
//
// Canonical drops those from both sides, because a digest cannot apply a
// tolerance asymmetrically the way Equal does. That loses the one direction
// worth knowing about: Synapse emitting them is its cached EventBase leaking
// and is not our concern, but us emitting them would be our bug.
//
// It is recoverable with no comparison at all -- we can look at what we
// produced. get_persisted_pdu does not load these fields, so if we emit one it
// came from stored JSON and wants explaining.
//
// Checked on the parsed unsigned object rather than by scanning the body for
// the field names, which would count any message whose text happens to mention
// them.
func emitsCachePollution(body []byte) bool {
	var ev struct {
		Unsigned map[string]json.RawMessage `json:"unsigned"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return false
	}
	for _, field := range []string{"prev_content", "prev_sender"} {
		if _, ok := ev.Unsigned[field]; ok {
			return true
		}
	}
	return false
}

// DigestStateResponse computes a StateResult from a /state response body.
//
// This is the far half of the comparison. Shadow mode cannot capture a /state
// response -- the largest here is about 97MB, and raising capture_mb to hold
// one would mean up to a gigabyte across 16 concurrent comparisons, recreating
// the problem the streaming resolver exists to avoid. So Synapse's answer is
// streamed and folded into the same digest instead, and the two sides are
// compared as 32 bytes.
//
// Memory is bounded by the largest single PDU, not by the response.
//
// The depth filter is deliberately not applied here: Synapse has already
// applied it to what it sent, and applying it again would hide precisely the
// disagreement this comparison exists to find.
func DigestStateResponse(r io.Reader) (StateResult, error) {
	var res StateResult
	dec := json.NewDecoder(r)

	tok, err := dec.Token()
	if err != nil {
		return res, fmt.Errorf("state response: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return res, fmt.Errorf("state response: expected object, got %v", tok)
	}

	var pduDigest, authDigest digestAccumulator
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return res, fmt.Errorf("state response: %w", err)
		}
		key, _ := keyTok.(string)

		switch key {
		case "pdus":
			n, err := digestArray(dec, &pduDigest)
			if err != nil {
				return res, fmt.Errorf("state response pdus: %w", err)
			}
			res.PDUs = n
		case "auth_chain":
			n, err := digestArray(dec, &authDigest)
			if err != nil {
				return res, fmt.Errorf("state response auth_chain: %w", err)
			}
			res.AuthChain = n
		default:
			// An unknown field is skipped rather than refused: Synapse may add
			// one, and a comparison that fell over on it would be worse than
			// one that ignores it. Decoding into RawMessage discards the value
			// without buffering the rest of the response.
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return res, fmt.Errorf("state response: skip %q: %w", key, err)
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		return res, fmt.Errorf("state response: %w", err)
	}

	res.PDUDigest = pduDigest.digest()
	res.AuthChainDigest = authDigest.digest()
	return res, nil
}

// Agrees reports whether two /state answers are the same, in both arrays.
func (s StateResult) Agrees(other StateResult) bool {
	return s.PDUDigest == other.PDUDigest &&
		s.AuthChainDigest == other.AuthChainDigest
}

// digestArray folds one array of PDUs into the digest and reports how many it
// held.
func digestArray(dec *json.Decoder, digest *digestAccumulator) (int, error) {
	tok, err := dec.Token()
	if err != nil {
		return 0, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return 0, fmt.Errorf("expected array, got %v", tok)
	}

	n := 0
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return n, err
		}
		canonical, ok := pducmp.Canonical(raw)
		if !ok {
			return n, fmt.Errorf("unparseable pdu at index %d", n)
		}
		digest.add(canonical)
		n++
	}
	if _, err := dec.Token(); err != nil {
		return n, err
	}
	return n, nil
}
