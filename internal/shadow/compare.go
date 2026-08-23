// Package shadow runs the native implementation alongside the proxied Synapse
// answer and records where the two disagree.
package shadow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/daedric/synapse-gopro-worker/internal/difflog"
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

// equalPDU compares two events, ignoring the value of the age field while still
// requiring both sides to agree on whether it is present.
func equalPDU(a, b json.RawMessage) bool {
	na, okA := normalisePDU(a)
	nb, okB := normalisePDU(b)
	if !okA || !okB {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(na, nb)
}

// normalisePDU decodes an event and replaces the wall-clock-dependent age with
// a marker, so presence is compared but the value is not.
func normalisePDU(raw json.RawMessage) (map[string]any, bool) {
	var ev map[string]any
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, false
	}
	if unsigned, ok := ev["unsigned"].(map[string]any); ok {
		if _, has := unsigned["age"]; has {
			unsigned["age"] = "<age>"
		}
	}
	return ev, true
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
