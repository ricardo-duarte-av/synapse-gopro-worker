package replication

import (
	"io"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// fakeCaches records what the subscriber did, so tests can assert on the thing
// that actually matters: whether the caches were left trusted.
type fakeCaches struct {
	mu      sync.Mutex
	armed   []bool
	purges  int
	dropped []string
}

func (f *fakeCaches) SetCachesArmed(a bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = append(f.armed, a)
}
func (f *fakeCaches) PurgeCaches() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purges++
}
func (f *fakeCaches) DropEvent(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped = append(f.dropped, id)
}

func newTestSubscriber(t *testing.T) (*Subscriber, *fakeCaches) {
	t.Helper()
	f := &fakeCaches{}
	s, err := New(Config{Enabled: true, Address: "/tmp/x", Channel: "example.org"},
		f, zerolog.New(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	return s, f
}

func TestPurgeHistoryEmptiesCaches(t *testing.T) {
	// The signal that would have prevented serving a deleted event.
	s, f := newTestSubscriber(t)
	s.handle(`RDATA caches av-event-persister-1 1234 ["ph_cache_fake",["!room:example.org"],1787612697762]`)
	if f.purges != 1 {
		t.Errorf("purges = %d, want 1 for ph_cache_fake", f.purges)
	}
}

func TestDeleteRoomEmptiesCaches(t *testing.T) {
	s, f := newTestSubscriber(t)
	s.handle(`RDATA caches master 1 ["dr_cache_fake",["!room:example.org"],1]`)
	if f.purges != 1 {
		t.Errorf("purges = %d, want 1 for dr_cache_fake", f.purges)
	}
}

func TestPerEventInvalidationIsTargeted(t *testing.T) {
	// An ordinary invalidation must not cost the whole cache.
	s, f := newTestSubscriber(t)
	s.handle(`RDATA caches master 1 ["_get_state_group_for_event",["$abc"],1]`)
	s.handle(`RDATA caches master 2 ["have_seen_event",["!r:example.org","$def"],1]`)
	if f.purges != 0 {
		t.Errorf("purges = %d, want 0 for per-event invalidations", f.purges)
	}
	want := []string{"$abc", "$def"}
	if len(f.dropped) != 2 || f.dropped[0] != want[0] || f.dropped[1] != want[1] {
		t.Errorf("dropped = %v, want %v", f.dropped, want)
	}
}

func TestNullKeysMeansInvalidateAll(t *testing.T) {
	s, f := newTestSubscriber(t)
	s.handle(`RDATA caches master 1 ["_get_state_group_for_event",null,1]`)
	if f.purges != 1 {
		t.Errorf("purges = %d, want 1 when keys is null", f.purges)
	}
}

func TestUnrelatedTrafficIsIgnored(t *testing.T) {
	// The channel carries presence, receipts and typing; none of it can affect
	// an immutable cache, and reacting to it would flush constantly.
	s, f := newTestSubscriber(t)
	for _, p := range []string{
		`RDATA presence master 1 ["@a:example.org","online"]`,
		`RDATA receipts master 2 ["!r:example.org","m.read"]`,
		`RDATA caches master 3 ["cs_cache_fake",["!r:example.org","@a:example.org"],1]`,
		`RDATA caches master 4 ["get_destination_retry_timings",["matrix.org"],1]`,
		`REMOTE_SERVER_UP matrix.org`,
		`POSITION caches master 1 2`,
	} {
		s.handle(p)
	}
	if f.purges != 0 {
		t.Errorf("purges = %d, want 0 for unrelated traffic", f.purges)
	}
	if len(f.dropped) != 0 {
		t.Errorf("dropped = %v, want none", f.dropped)
	}
}

func TestBatchTokenIsAccepted(t *testing.T) {
	// Synapse sends "batch" instead of a number for all but the last row of a
	// batch. Treating that as malformed would flush the cache on every batch.
	s, f := newTestSubscriber(t)
	s.handle(`RDATA caches master batch ["_get_state_group_for_event",["$abc"],1]`)
	if f.purges != 0 {
		t.Errorf("a batched row was treated as malformed (purges = %d)", f.purges)
	}
	if len(f.dropped) != 1 || f.dropped[0] != "$abc" {
		t.Errorf("dropped = %v, want [$abc]", f.dropped)
	}
}

func TestMalformedRowEmptiesCaches(t *testing.T) {
	// A row we cannot parse may have been the purge that makes us wrong, so the
	// only safe reading is that we missed an invalidation.
	for _, payload := range []string{
		`RDATA caches master 1 {not json}`,
		`RDATA caches master 1 ["ph_cache_fake"]`,
		`RDATA caches master`,
		`RDATA caches master 1 [123,["k"],1]`,
		`RDATA caches master 1 ["_get_state_group_for_event",[],1]`,
	} {
		s, f := newTestSubscriber(t)
		s.handle(payload)
		if f.purges == 0 {
			t.Errorf("malformed row %q did not empty the caches", payload)
		}
	}
}

func TestConfigRequiresChannel(t *testing.T) {
	// One Redis can carry several homeservers' streams, so a wrong or missing
	// channel would look healthy and never invalidate anything.
	if _, err := New(Config{Enabled: true, Address: "/tmp/x"}, &fakeCaches{}, zerolog.New(io.Discard)); err == nil {
		t.Error("a missing channel was accepted")
	}
	if _, err := New(Config{Enabled: true, Channel: "example.org"}, &fakeCaches{}, zerolog.New(io.Discard)); err == nil {
		t.Error("a missing address was accepted")
	}
}
