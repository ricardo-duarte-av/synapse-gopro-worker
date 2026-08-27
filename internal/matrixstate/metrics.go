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
// It does NOT count defects, and an earlier version of this comment claimed it
// would stay at zero. That was wrong. Both fields are in Synapse's unsigned
// allowlist, remote servers send them, and Synapse stores them verbatim: 9,300
// events across 158 rooms here carry one. Synapse serves them from stored JSON
// exactly as we do, so both sides agree and the count is ordinary traffic.
//
// What it actually measures is how often the digest is comparing blind.
// Canonical drops these fields from both sides, because a digest cannot apply
// a tolerance asymmetrically the way Equal does -- so for every PDU counted
// here, a disagreement about those two fields would not have been caught.
// That blind spot turns out to be engaged frequently rather than rarely, which
// is worth knowing when reading a clean /state match rate.
var statePrevContentEmitted = promauto.NewCounter(prometheus.CounterOpts{
	Name: "gopro_state_prev_content_emitted_total",
	Help: "PDUs served on /state carrying prev_content or prev_sender, for which the digest comparison is blind to those fields.",
})
