package store

import (
	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// caches holds the in-process caches for immutable data.
//
// Everything here is safe to cache forever because it cannot change. A state
// group's contents are fixed once written; an event's stored JSON does not
// change; an auth chain is a function of immutable events. Mutable data —
// membership, server ACLs, partial-state status, erasure — is deliberately
// absent, because caching it would need invalidation we have no way to receive.
type caches struct {
	stateGroups     *cache.LRU[int64, map[StateKey]string]
	events          *cache.LRU[string, *Event]
	eventStateGroup *cache.LRU[string, int64]
	authChains      *cache.LRU[[32]byte, []string]
}

func newCaches(s cache.Settings) *caches {
	s = s.WithDefaults()
	return &caches{
		stateGroups: cache.NewLRU[int64, map[StateKey]string](
			"state_groups", cache.MB(s.StateGroupsMB), sizeOfStateMap),
		events: cache.NewLRU[string, *Event](
			"events", cache.MB(s.EventsMB), sizeOfEvent),
		eventStateGroup: cache.NewLRU[string, int64](
			"event_state_groups", cache.MB(s.EventStateGroupsMB), func(int64) int64 { return 64 }),
		authChains: cache.NewLRU[[32]byte, []string](
			"auth_chains", cache.MB(s.AuthChainsMB), sizeOfIDs),
	}
}

// sizeOfStateMap approximates a resolved state map's footprint. Keys and values
// are event IDs and type strings; the per-entry constant covers map overhead.
func sizeOfStateMap(m map[StateKey]string) int64 {
	var n int64
	for k, v := range m {
		n += int64(len(k.Type) + len(k.StateKey) + len(v) + 64)
	}
	return n
}

func sizeOfEvent(e *Event) int64 {
	if e == nil {
		return 0
	}
	return int64(len(e.JSON) + len(e.InternalMetadata) + len(e.EventID) +
		len(e.RoomID) + len(e.Type) + len(e.RoomVersion) + 128)
}

func sizeOfIDs(ids []string) int64 {
	var n int64
	for _, id := range ids {
		n += int64(len(id) + 16)
	}
	return n
}

// CacheStats returns a snapshot of every cache, for metrics.
func (s *Store) CacheStats() []cache.Stats {
	if s == nil || s.caches == nil {
		return nil
	}
	return []cache.Stats{
		s.caches.stateGroups.Stats(),
		s.caches.events.Stats(),
		s.caches.eventStateGroup.Stats(),
		s.caches.authChains.Stats(),
	}
}
