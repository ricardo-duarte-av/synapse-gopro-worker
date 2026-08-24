package cache

import (
	"fmt"
	"sync"
	"testing"
)

// bytes sizes a string value by its length, so tests can reason in exact bytes.
func bytes(s string) int64 { return int64(len(s)) }

func TestGetAndAdd(t *testing.T) {
	c := NewLRU[string, string]("test", 100, bytes)
	if _, ok := c.Get("a"); ok {
		t.Error("empty cache returned a value")
	}
	c.Add("a", "hello")
	got, ok := c.Get("a")
	if !ok || got != "hello" {
		t.Errorf("Get = %q, %v; want hello, true", got, ok)
	}

	s := c.Stats()
	if s.Hits != 1 || s.Misses != 1 {
		t.Errorf("hits/misses = %d/%d, want 1/1", s.Hits, s.Misses)
	}
	if s.Bytes != 5 {
		t.Errorf("Bytes = %d, want 5", s.Bytes)
	}
}

func TestEvictsBySize(t *testing.T) {
	// Eviction is by bytes, not entry count: a resolved state map ranges over
	// four orders of magnitude, so a count bound would not bound memory.
	c := NewLRU[string, string]("test", 10, bytes)
	c.Add("a", "12345")
	c.Add("b", "12345")
	if s := c.Stats(); s.Bytes != 10 || s.Entries != 2 {
		t.Fatalf("bytes/entries = %d/%d, want 10/2", s.Bytes, s.Entries)
	}

	// This pushes past the bound and must evict the oldest.
	c.Add("c", "12345")
	if _, ok := c.Get("a"); ok {
		t.Error("least recently used entry was not evicted")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("newest entry is missing")
	}
	if s := c.Stats(); s.Bytes > 10 {
		t.Errorf("Bytes = %d, exceeds the bound", s.Bytes)
	}
}

func TestGetPromotes(t *testing.T) {
	c := NewLRU[string, string]("test", 10, bytes)
	c.Add("a", "12345")
	c.Add("b", "12345")
	// Touching "a" must make "b" the eviction candidate instead.
	c.Get("a")
	c.Add("c", "12345")

	if _, ok := c.Get("a"); !ok {
		t.Error("recently used entry was evicted")
	}
	if _, ok := c.Get("b"); ok {
		t.Error("least recently used entry survived")
	}
}

func TestOversizedValueIsNotStored(t *testing.T) {
	// Admitting an entry bigger than the cache would evict everything else and
	// then be evicted itself by the next write, so it is refused outright.
	c := NewLRU[string, string]("test", 10, bytes)
	c.Add("small", "123")
	c.Add("huge", "12345678901234567890")

	if _, ok := c.Get("huge"); ok {
		t.Error("an oversized value was stored")
	}
	if _, ok := c.Get("small"); !ok {
		t.Error("an oversized write evicted a valid entry")
	}
}

func TestReplaceAdjustsSize(t *testing.T) {
	c := NewLRU[string, string]("test", 100, bytes)
	c.Add("a", "12345")
	c.Add("a", "1")
	s := c.Stats()
	if s.Bytes != 1 {
		t.Errorf("Bytes = %d after replacing with a smaller value, want 1", s.Bytes)
	}
	if s.Entries != 1 {
		t.Errorf("Entries = %d, want 1", s.Entries)
	}
	if got, _ := c.Get("a"); got != "1" {
		t.Errorf("Get = %q, want the replacement", got)
	}
}

func TestDisabledCache(t *testing.T) {
	// A zero bound turns the cache off without changing any call site.
	c := NewLRU[string, string]("test", 0, bytes)
	if c.Enabled() {
		t.Error("Enabled = true for a zero-sized cache")
	}
	c.Add("a", "hello")
	if _, ok := c.Get("a"); ok {
		t.Error("a disabled cache returned a value")
	}
}

func TestNilCacheIsSafe(t *testing.T) {
	var c *LRU[string, string]
	if c.Enabled() {
		t.Error("nil cache reports enabled")
	}
	if _, ok := c.Get("a"); ok {
		t.Error("nil cache returned a value")
	}
	c.Add("a", "b")
	c.Purge()
	if s := c.Stats(); s.Entries != 0 {
		t.Errorf("nil cache stats = %+v", s)
	}
}

func TestHitRate(t *testing.T) {
	if got := (Stats{}).HitRate(); got != 0 {
		t.Errorf("HitRate with no lookups = %v, want 0", got)
	}
	if got := (Stats{Hits: 3, Misses: 1}).HitRate(); got != 0.75 {
		t.Errorf("HitRate = %v, want 0.75", got)
	}
}

func TestPurge(t *testing.T) {
	c := NewLRU[string, string]("test", 100, bytes)
	c.Add("a", "12345")
	c.Purge()
	if s := c.Stats(); s.Entries != 0 || s.Bytes != 0 {
		t.Errorf("after Purge: entries=%d bytes=%d, want 0/0", s.Entries, s.Bytes)
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	c := NewLRU[int, string]("test", 1000, bytes)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 500 {
				k := (g*500 + i) % 100
				if _, ok := c.Get(k); !ok {
					c.Add(k, fmt.Sprintf("value-%d", k))
				}
			}
		}(g)
	}
	wg.Wait()
	if s := c.Stats(); s.Bytes > 1000 {
		t.Errorf("Bytes = %d, exceeds the bound after concurrent use", s.Bytes)
	}
}
