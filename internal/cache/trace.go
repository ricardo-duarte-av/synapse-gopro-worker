package cache

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Trace records, for one request, which cacheable lookups were served from
// cache and which went to the database.
//
// It counts outcomes rather than LRU probes. A filtered-state lookup that
// misses its own cache but finds the whole map resident was still answered
// from memory, and counting the probe would report it as a miss.
//
// Only cacheable data is traced. Membership, server ACLs and erasure are never
// cached (§5) and are read from the database on every request, so "hit" means
// every *cacheable* lookup was served from memory -- not that the request made
// no queries at all.
type Trace struct {
	mu     sync.Mutex
	counts map[string]*[2]int // name -> {hits, lookups}
	joined bool
}

type traceKey struct{}

// WithTrace returns a context carrying a fresh trace.
func WithTrace(ctx context.Context) (context.Context, *Trace) {
	t := &Trace{counts: map[string]*[2]int{}}
	return context.WithValue(ctx, traceKey{}, t), t
}

// Record notes one cacheable lookup on the request's trace, if it has one.
//
// Safe from any goroutine. A shared /state_ids computation outlives the
// request that started it and may still record after that request has logged;
// those late records are simply never read.
func Record(ctx context.Context, name string, hit bool) {
	RecordN(ctx, name, boolToInt(hit), 1)
}

// RecordN notes several lookups against one cache at once.
func RecordN(ctx context.Context, name string, hits, lookups int) {
	t, _ := ctx.Value(traceKey{}).(*Trace)
	if t == nil || lookups == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.counts[name]
	if c == nil {
		c = &[2]int{}
		t.counts[name] = c
	}
	c[0] += hits
	c[1] += lookups
}

// MarkJoined notes that the answer came from another request's in-flight
// computation, so this request made no lookups of its own.
func MarkJoined(ctx context.Context) {
	if t, _ := ctx.Value(traceKey{}).(*Trace); t != nil {
		t.mu.Lock()
		t.joined = true
		t.mu.Unlock()
	}
}

// Summary is "hit" when every cacheable lookup was served from memory, "miss"
// when none was, "partial" in between, and "joined" when the answer came from
// another request's computation. Empty when nothing cacheable was looked up.
func (t *Trace) Summary() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.joined {
		return "joined"
	}
	var hits, lookups int
	for _, c := range t.counts {
		hits += c[0]
		lookups += c[1]
	}
	switch {
	case lookups == 0:
		return ""
	case hits == lookups:
		return "hit"
	case hits == 0:
		return "miss"
	default:
		return "partial"
	}
}

// Detail lists each cache as name=hits/lookups, sorted by name, e.g.
// "auth_chains=0/1 events=3/3 state_groups=1/1".
func (t *Trace) Detail() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	names := make([]string, 0, len(t.counts))
	for n := range t.counts {
		names = append(names, n)
	}
	slices.Sort(names)
	parts := make([]string, len(names))
	for i, n := range names {
		c := t.counts[n]
		parts[i] = fmt.Sprintf("%s=%d/%d", n, c[0], c[1])
	}
	return strings.Join(parts, " ")
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
