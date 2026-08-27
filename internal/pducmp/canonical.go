package pducmp

import (
	"encoding/json"
)

// Canonical renders one event in the form a digest may be taken over.
//
// Equal is pairwise: it can drop a field from Synapse's side *because* ours
// lacks it. A digest has no such luxury. Each side is folded independently and
// neither knows what the other holds, so every tolerance has to be applied to
// both sides identically or the two digests disagree on events that Equal
// would have accepted.
//
// That costs one thing worth naming. Equal reports prev_content and prev_sender
// as our bug when we emit them and Synapse does not; a symmetric drop cannot
// tell the two directions apart. The detection is recovered elsewhere by
// counting our own emissions, which needs no comparison against Synapse at
// all: if we ever emit those fields, that is a finding on its own.
//
// Key order is normalised by re-encoding from a map, and integers keep their
// literal text because normalise decodes with UseNumber. Both matter here:
// our responses splice stored JSON while Synapse re-serialises from a dict, so
// the two differ in key order on essentially every event.
func Canonical(raw json.RawMessage) ([]byte, bool) {
	ev, ok := normalise(raw)
	if !ok {
		return nil, false
	}
	dropUncomparableUnsigned(ev)
	out, err := json.Marshal(ev)
	if err != nil {
		return nil, false
	}
	return out, true
}

// dropUncomparableUnsigned removes the unsigned fields a digest cannot compare
// asymmetrically.
//
// Only synapseCachePollutedFields are dropped, and they are dropped from
// whichever side carries them. age is already reduced to a marker by normalise
// rather than removed, so its presence is still compared -- and /state never
// emits it at all, because serialize_and_filter_pdus passes time_now=None.
func dropUncomparableUnsigned(ev map[string]any) {
	unsigned, ok := ev["unsigned"].(map[string]any)
	if !ok {
		return
	}
	for _, field := range synapseCachePollutedFields {
		delete(unsigned, field)
	}
}
