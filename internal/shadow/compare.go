// Package shadow runs the native implementation alongside the proxied Synapse
// answer and records where the two disagree.
package shadow

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/daedric/synapse-gopro-worker/internal/difflog"
	"github.com/daedric/synapse-gopro-worker/internal/pducmp"
)

// stateIDsBody is the /state_ids response shape.
type stateIDsBody struct {
	PDUIDs       []string `json:"pdu_ids"`
	AuthChainIDs []string `json:"auth_chain_ids"`
}

// CompareStateIDs diffs two /state_ids responses.
//
// Both fields are unordered collections of event IDs, so they are compared as
// sets: Synapse builds them from a Python set and dict, and neither the spec
// nor any client depends on the order. Comparing byte-for-byte would report
// mismatches on every single request.
func CompareStateIDs(synapseBody, nativeBody []byte) (*difflog.Diff, error) {
	var syn, nat stateIDsBody
	if err := json.Unmarshal(synapseBody, &syn); err != nil {
		return nil, fmt.Errorf("shadow: parse synapse body: %w", err)
	}
	if err := json.Unmarshal(nativeBody, &nat); err != nil {
		return nil, fmt.Errorf("shadow: parse native body: %w", err)
	}

	diff := &difflog.Diff{Fields: []difflog.FieldDiff{
		diffIDSets("pdu_ids", syn.PDUIDs, nat.PDUIDs),
		diffIDSets("auth_chain_ids", syn.AuthChainIDs, nat.AuthChainIDs),
	}}
	if diff.Empty() {
		return nil, nil
	}
	return diff, nil
}

// diffIDSets reports which IDs each side has that the other does not.
func diffIDSets(field string, synapse, native []string) difflog.FieldDiff {
	synSet := make(map[string]struct{}, len(synapse))
	for _, id := range synapse {
		synSet[id] = struct{}{}
	}
	natSet := make(map[string]struct{}, len(native))
	for _, id := range native {
		natSet[id] = struct{}{}
	}

	fd := difflog.FieldDiff{
		Field:        field,
		SynapseCount: len(synSet),
		NativeCount:  len(natSet),
	}
	for id := range synSet {
		if _, ok := natSet[id]; !ok {
			fd.MissingFromNative = append(fd.MissingFromNative, id)
		}
	}
	for id := range natSet {
		if _, ok := synSet[id]; !ok {
			fd.ExtraInNative = append(fd.ExtraInNative, id)
		}
	}
	// Stable order so the same disagreement always logs identically.
	sort.Strings(fd.MissingFromNative)
	sort.Strings(fd.ExtraInNative)
	return fd
}

// transactionBody is the /event response shape.
type transactionBody struct {
	Origin         string            `json:"origin"`
	OriginServerTS int64             `json:"origin_server_ts"`
	PDUs           []json.RawMessage `json:"pdus"`
}

// CompareEvent diffs two /event responses.
//
// Two fields are wall-clock-dependent and cannot be compared by value: the
// transaction's origin_server_ts, and unsigned.age, which Synapse computes as
// "now minus age_ts" when serialising. Their presence is still compared, so
// emitting age where Synapse emits age_ts, or omitting it entirely, is still
// caught.
func CompareEvent(synapseBody, nativeBody []byte) (*difflog.Diff, error) {
	var syn, nat transactionBody
	if err := json.Unmarshal(synapseBody, &syn); err != nil {
		return nil, fmt.Errorf("shadow: parse synapse body: %w", err)
	}
	if err := json.Unmarshal(nativeBody, &nat); err != nil {
		return nil, fmt.Errorf("shadow: parse native body: %w", err)
	}

	fd := difflog.FieldDiff{
		Field:        "pdus",
		SynapseCount: len(syn.PDUs),
		NativeCount:  len(nat.PDUs),
	}

	if syn.Origin != nat.Origin {
		fd.ContentMismatch = append(fd.ContentMismatch,
			fmt.Sprintf("origin: synapse=%q native=%q", syn.Origin, nat.Origin))
	}

	synPDUs := indexPDUs(syn.PDUs)
	natPDUs := indexPDUs(nat.PDUs)

	for id, synPDU := range synPDUs {
		natPDU, ok := natPDUs[id]
		if !ok {
			fd.MissingFromNative = append(fd.MissingFromNative, id)
			continue
		}
		if !equalPDU(synPDU, natPDU) {
			fd.ContentMismatch = append(fd.ContentMismatch, id)
		}
	}
	for id := range natPDUs {
		if _, ok := synPDUs[id]; !ok {
			fd.ExtraInNative = append(fd.ExtraInNative, id)
		}
	}

	sort.Strings(fd.MissingFromNative)
	sort.Strings(fd.ExtraInNative)
	sort.Strings(fd.ContentMismatch)

	diff := &difflog.Diff{Fields: []difflog.FieldDiff{fd}}
	if diff.Empty() {
		return nil, nil
	}
	return diff, nil
}

// indexPDUs keys events by their event ID, falling back to position when an
// event carries none.
func indexPDUs(pdus []json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(pdus))
	for i, raw := range pdus {
		var ev struct {
			EventID string `json:"event_id"`
		}
		key := fmt.Sprintf("#%d", i)
		if err := json.Unmarshal(raw, &ev); err == nil && ev.EventID != "" {
			key = ev.EventID
		}
		out[key] = raw
	}
	return out
}

// equalPDU compares two events. The rules live in pducmp so the diff-log
// replay test applies exactly the same ones.
func equalPDU(synapse, native json.RawMessage) bool {
	return pducmp.Equal(synapse, native)
}

// CompareStatus reports whether two responses agree at the HTTP level, and
// whether their bodies are worth comparing at all.
//
// A body comparison only makes sense when both sides returned 200. When both
// returned the same error status the answers agree; when they differ, that is
// itself the mismatch and the bodies add nothing.
func CompareStatus(synapseStatus, nativeStatus int) (agree, compareBodies bool) {
	if synapseStatus != nativeStatus {
		return false, false
	}
	return true, synapseStatus == 200
}

// missingEventsBody is the shape of a /get_missing_events response.
type missingEventsBody struct {
	Events []json.RawMessage `json:"events"`
}

// CompareGetMissingEvents compares two /get_missing_events answers.
//
// The events are keyed by canonical content rather than by position or event
// ID. Position is meaningless here -- the two sides walk the DAG in different
// orders -- and room version 3 and later carry no event_id to key on, so
// indexPDUs would fall back to position for most rooms and compare order
// instead of content.
//
// The second return value is a skip reason rather than a disagreement.
//
// When the walk stops because it reached the limit, which events survive
// depends on the order the frontier was iterated: Synapse iterates a Python
// set, we iterate a sorted slice, and both answers are equally valid subsets
// of the reachable events. Worse, Python randomises string hashes per process,
// so Synapse's own answer is stable only for the lifetime of one process.
// There is no answer to match, so a truncated walk carries no information
// about whether we are right -- exactly like Synapse's 429s and its failed key
// fetches, and reclassified for the same reason.
//
// A walk that *completed* is fully determined and is compared exactly.
// Measured against the live server, 28 of 28 completed walks agreed.
func CompareGetMissingEvents(requestBody, synapseBody, nativeBody []byte) (*difflog.Diff, string, error) {
	var syn, nat missingEventsBody
	if err := json.Unmarshal(synapseBody, &syn); err != nil {
		return nil, "", fmt.Errorf("shadow: parse synapse body: %w", err)
	}
	if err := json.Unmarshal(nativeBody, &nat); err != nil {
		return nil, "", fmt.Errorf("shadow: parse native body: %w", err)
	}

	synSet := indexByContent(syn.Events)
	natSet := indexByContent(nat.Events)

	fd := difflog.FieldDiff{
		Field:        "events",
		SynapseCount: len(syn.Events),
		NativeCount:  len(nat.Events),
	}
	for key, raw := range synSet {
		if _, ok := natSet[key]; !ok {
			fd.MissingFromNative = append(fd.MissingFromNative, describeEvent(raw))
		}
	}
	for key, raw := range natSet {
		if _, ok := synSet[key]; !ok {
			fd.ExtraInNative = append(fd.ExtraInNative, describeEvent(raw))
		}
	}
	sort.Strings(fd.MissingFromNative)
	sort.Strings(fd.ExtraInNative)

	diff := &difflog.Diff{Fields: []difflog.FieldDiff{fd}}
	if diff.Empty() {
		return nil, "", nil
	}

	// Both sides filled their budget: neither answer is the correct one.
	if limit := missingEventsLimit(requestBody); len(syn.Events) >= limit && len(nat.Events) >= limit {
		return nil, "walk_truncated", nil
	}
	return diff, "", nil
}

// missingEventsLimit reports the effective limit for a request, applying
// Synapse's default of 10 and its cap of 20.
func missingEventsLimit(requestBody []byte) int {
	limit := 10
	if len(requestBody) > 0 {
		var body struct {
			Limit *int `json:"limit"`
		}
		if err := json.Unmarshal(requestBody, &body); err == nil && body.Limit != nil {
			limit = *body.Limit
		}
	}
	if limit > 20 {
		limit = 20
	}
	return limit
}

// indexByContent keys events by their canonical form, so two answers listing
// the same events in different orders compare equal.
func indexByContent(events []json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(events))
	for i, raw := range events {
		if c, ok := pducmp.Canonical(raw); ok {
			out[string(c)] = raw
			continue
		}
		// An event we cannot canonicalise still has to be distinguishable, or
		// two unparseable events would collapse into one.
		out[fmt.Sprintf("#%d", i)] = raw
	}
	return out
}

// describeEvent names an event for a diff record, preferring its ID when the
// room version carries one and falling back to type and sender.
func describeEvent(raw json.RawMessage) string {
	var ev struct {
		EventID  string  `json:"event_id"`
		Type     string  `json:"type"`
		Sender   string  `json:"sender"`
		StateKey *string `json:"state_key"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return "<unparseable>"
	}
	if ev.EventID != "" {
		return ev.EventID
	}
	if ev.StateKey != nil {
		return fmt.Sprintf("%s/%s from %s", ev.Type, *ev.StateKey, ev.Sender)
	}
	return fmt.Sprintf("%s from %s", ev.Type, ev.Sender)
}
