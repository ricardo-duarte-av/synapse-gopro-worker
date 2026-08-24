// Package cache provides a size-aware LRU cache for immutable Matrix data.
//
// Only data that can never change is cached here. A state group's contents are
// fixed once written, an event's JSON never changes, and an auth chain is a
// function of immutable events. That means entries need no invalidation at all,
// which is what makes caching safe without consuming Synapse's replication
// stream.
//
// Eviction is by total byte size rather than entry count. A resolved state map
// ranges from a few kilobytes for a small room to nearly a hundred megabytes
// for the largest, so a count-based bound would either waste memory or fail to
// bound it.
package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Sizer reports the approximate memory footprint of a cached value.
type Sizer[V any] func(V) int64

// LRU is a size-bounded, least-recently-used cache safe for concurrent use.
type LRU[K comparable, V any] struct {
	name     string
	maxBytes int64
	sizeOf   Sizer[V]

	// armed gates the whole cache at runtime. It exists so caching can be
	// suspended the moment we lose the invalidation signal that makes it safe,
	// without tearing down the cache or touching any call site.
	armed atomic.Bool

	mu      sync.Mutex
	entries map[K]*list.Element
	order   *list.List // front is most recently used
	bytes   int64

	hits, misses, evictions uint64
}

type entry[K comparable, V any] struct {
	key   K
	value V
	size  int64
}

// NewLRU builds a cache bounded to maxBytes. A maxBytes of zero disables the
// cache entirely: Get always misses and Add does nothing, so a cache can be
// turned off in configuration without changing any call site.
func NewLRU[K comparable, V any](name string, maxBytes int64, sizeOf Sizer[V]) *LRU[K, V] {
	c := &LRU[K, V]{
		name:     name,
		maxBytes: maxBytes,
		sizeOf:   sizeOf,
		entries:  make(map[K]*list.Element),
		order:    list.New(),
	}
	// Armed by default, so a deployment without an invalidation source behaves
	// exactly as before. Anything that can lose that signal must disarm.
	c.armed.Store(true)
	return c
}

// SetArmed turns the cache on or off at runtime. Disarming also empties it:
// entries admitted before the invalidation signal was lost cannot be trusted,
// and keeping them would only make re-arming dangerous.
func (c *LRU[K, V]) SetArmed(armed bool) {
	if c == nil {
		return
	}
	c.armed.Store(armed)
	if !armed {
		c.Purge()
	}
}

// Armed reports whether the cache is currently permitted to serve.
func (c *LRU[K, V]) Armed() bool { return c != nil && c.armed.Load() }

// Enabled reports whether the cache stores anything right now. Both conditions
// matter: a cache sized to zero is off permanently, and a disarmed cache is off
// until its invalidation source comes back.
func (c *LRU[K, V]) Enabled() bool {
	return c != nil && c.maxBytes > 0 && c.armed.Load()
}

// Get returns a cached value.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	var zero V
	if !c.Enabled() {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.entries[key]
	if !ok {
		c.misses++
		return zero, false
	}
	c.order.MoveToFront(el)
	c.hits++
	return el.Value.(*entry[K, V]).value, true
}

// Add stores a value, evicting least-recently-used entries until the cache is
// within its size bound.
func (c *LRU[K, V]) Add(key K, value V) {
	if !c.Enabled() {
		return
	}
	size := c.sizeOf(value)

	c.mu.Lock()
	defer c.mu.Unlock()

	// An entry larger than the whole cache is not stored: admitting it would
	// evict everything else and then be evicted itself by the next write.
	if size > c.maxBytes {
		return
	}

	if el, ok := c.entries[key]; ok {
		e := el.Value.(*entry[K, V])
		c.bytes += size - e.size
		e.value, e.size = value, size
		c.order.MoveToFront(el)
		c.evictLocked()
		return
	}

	el := c.order.PushFront(&entry[K, V]{key: key, value: value, size: size})
	c.entries[key] = el
	c.bytes += size
	c.evictLocked()
}

func (c *LRU[K, V]) evictLocked() {
	for c.bytes > c.maxBytes {
		el := c.order.Back()
		if el == nil {
			return
		}
		e := el.Value.(*entry[K, V])
		c.order.Remove(el)
		delete(c.entries, e.key)
		c.bytes -= e.size
		c.evictions++
	}
}

// Stats is a snapshot of cache activity.
type Stats struct {
	Name                    string
	Entries                 int
	Bytes, MaxBytes         int64
	Hits, Misses, Evictions uint64
}

// HitRate returns the fraction of lookups served from cache, or 0 if none.
func (s Stats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Stats returns a snapshot.
func (c *LRU[K, V]) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Name:      c.name,
		Entries:   len(c.entries),
		Bytes:     c.bytes,
		MaxBytes:  c.maxBytes,
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}

// Purge empties the cache.
func (c *LRU[K, V]) Purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[K]*list.Element)
	c.order.Init()
	c.bytes = 0
}

// Remove drops a single entry. Removing something absent is not an error: an
// invalidation for a key we never cached is the common case.
func (c *LRU[K, V]) Remove(key K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return
	}
	c.bytes -= el.Value.(*entry[K, V]).size
	c.order.Remove(el)
	delete(c.entries, key)
}
