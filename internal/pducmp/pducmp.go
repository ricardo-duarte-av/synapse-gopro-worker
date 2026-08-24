// Package pducmp decides whether two serialisations of the same event agree.
//
// It exists so the shadow comparator and the diff-log replay test share one
// definition of "agree". They previously each carried their own copy, which
// drifted twice: a fix to one left the other reporting differences that the
// live comparator tolerated, and vice versa.
package pducmp

import (
	"bytes"
	"encoding/json"
	"reflect"
)

// Equal compares an event as Synapse served it against ours.
//
// The comparison is deliberately asymmetric: the direction of a difference is
// evidence about whose bug it is.
func Equal(synapse, native json.RawMessage) bool {
	ns, okS := normalise(synapse)
	nn, okN := normalise(native)
	if !okS || !okN {
		return bytes.Equal(synapse, native)
	}
	dropSynapseCachePollution(ns, nn)
	return reflect.DeepEqual(ns, nn)
}

// normalise decodes an event and replaces the wall-clock-dependent age with a
// marker, so its presence is compared but its value is not. Synapse derives
// age as "now minus age_ts" at serialisation time, so the two sides compute it
// milliseconds apart and can never match by value.
func normalise(raw json.RawMessage) (map[string]any, bool) {
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

// synapseCachePollutedFields are unsigned fields Synapse sometimes emits on
// /event through no decision of its own.
//
// get_persisted_pdu loads the event with get_prev_content defaulting to false,
// so federation should never see these. But events_worker computes them for
// client-facing reads and writes them into the *shared cached* EventBase --
// "This mutates the cached event, but that's fine", says the comment there. A
// later federation read of that same cached event then serves them. Whether
// they appear depends on whether a local client happened to read the event
// first, which we cannot observe and should not imitate: they are not covered
// by the event signature and the spec does not ask for them here.
var synapseCachePollutedFields = []string{"prev_content", "prev_sender"}

// dropSynapseCachePollution ignores those fields in the one direction Synapse's
// cache can produce -- present upstream, absent in ours. Emitting them where
// Synapse does not would still be our bug, and is still reported.
func dropSynapseCachePollution(synapse, native map[string]any) {
	synUnsigned, ok := synapse["unsigned"].(map[string]any)
	if !ok {
		return
	}
	natUnsigned, _ := native["unsigned"].(map[string]any)
	for _, field := range synapseCachePollutedFields {
		if _, inSynapse := synUnsigned[field]; !inSynapse {
			continue
		}
		if _, inNative := natUnsigned[field]; inNative {
			continue
		}
		delete(synUnsigned, field)
	}
}
