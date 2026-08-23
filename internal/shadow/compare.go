// Package shadow runs the native implementation alongside the proxied Synapse
// answer and records where the two disagree.
package shadow

import (
	"encoding/json"
	"fmt"
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
