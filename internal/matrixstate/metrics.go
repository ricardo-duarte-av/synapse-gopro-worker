package matrixstate

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// authChainFallbacks counts auth chains computed by walking event_auth because
// the chain cover index did not describe the events.
//
// The fallback is correct but much slower. There is deliberately no room_id
// label: this server has over a thousand rooms, and a per-room label would make
// the series count grow without bound. The room is logged instead.
var authChainFallbacks = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "gopro",
	Name:      "auth_chain_index_fallback_total",
	Help:      "Auth chains computed by recursive walk because the chain cover index was incomplete.",
})

// statePrevContentEmitted counts PDUs we served on /state carrying
// prev_content or prev_sender.
//
// The digest drops those fields symmetrically, so the comparator can no longer
// tell "Synapse emitted them" from "we did". This recovers the half that
// matters: get_persisted_pdu does not load them, so if we emit them the field
// came from stored JSON and is worth explaining. Expected to stay at zero.
var statePrevContentEmitted = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gopro_state_prev_content_emitted_total",
	Help: "PDUs served on /state carrying prev_content or prev_sender, which get_persisted_pdu would not load.",
})
