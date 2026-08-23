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
