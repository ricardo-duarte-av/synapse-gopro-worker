package matrixstate

import (
	"context"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// stateIDsComputeTimeout bounds one shared /state_ids computation.
//
// It is deliberately longer than the 5s serving budget and independent of it.
// The computation no longer belongs to any one request: each caller waits on
// it under its own deadline and falls back alone, while the work carries on
// and lands in the caches for whoever asks next. What this bounds is only a
// computation that would never finish.
const stateIDsComputeTimeout = 60 * time.Second

// stateIDsFlight collapses concurrent /state_ids computations for the same
// event into one.
//
// Measured 2026-10-16 13:30 UTC: 327 servers asked for state in one room
// within minutes, one event up to 129 times. The caches only help once an
// answer has finished, so every request arriving before then cold-computed
// the same state map and auth chain -- ~2,500 computations for ~209 distinct
// answers, all queued on a 16-connection pool. Queue waits ran out the serving
// budget and ~170 requests fell back to Synapse, which then took a median 18s
// to answer them.
//
// Only the origin-independent part is shared. The access check (membership,
// ACL) depends on who is asking and runs per request, before joining.
type stateIDsFlight struct {
	group   singleflight.Group
	timeout time.Duration
}

// do returns the answer for key, computing it with fn unless an identical
// computation is already running, in which case it waits for that one.
//
// The computation runs on a context detached from every caller. Otherwise the
// first server to hang up -- and Tuwunel-style servers hang up constantly --
// would cancel the work everyone else is waiting on.
func (f *stateIDsFlight) do(ctx context.Context, key string, fn func(context.Context) (*StateIDsResponse, error)) (*StateIDsResponse, error) {
	computed := false
	ch := f.group.DoChan(key, func() (v any, err error) {
		computed = true
		// singleflight re-panics a panicking fn on a fresh goroutine, which
		// no recover can reach and which takes the whole process down. The
		// request path recovers panics into a fallback; keep that true here.
		defer func() {
			if p := recover(); p != nil {
				v, err = nil, fmt.Errorf("state_ids: shared computation panicked: %v", p)
			}
		}()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.timeout)
		defer cancel()
		return fn(cctx)
	})

	select {
	case res := <-ch:
		// Reading computed is safe: fn returned before the result was sent.
		if computed {
			stateIDsFlights.WithLabelValues("computed").Inc()
		} else {
			stateIDsFlights.WithLabelValues("joined").Inc()
			cache.MarkJoined(ctx)
		}
		if res.Err != nil {
			return nil, res.Err
		}
		return cloneStateIDs(res.Val.(*StateIDsResponse)), nil
	case <-ctx.Done():
		stateIDsFlights.WithLabelValues("abandoned").Inc()
		return nil, fmt.Errorf("state_ids: waiting for shared computation: %w", ctx.Err())
	}
}

// cloneStateIDs gives each caller its own slices.
//
// Today every caller only marshals the response, but a shared value that a
// caller can edit is exactly how the cache was corrupted twice (§6.1, §6.3).
// The strings themselves are immutable and stay shared, so this costs two
// slice-header copies.
func cloneStateIDs(r *StateIDsResponse) *StateIDsResponse {
	return &StateIDsResponse{
		PDUIDs:       slices.Clone(r.PDUIDs),
		AuthChainIDs: slices.Clone(r.AuthChainIDs),
	}
}
