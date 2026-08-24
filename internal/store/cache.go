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

// SetCachesArmed turns every cache on or off together.
//
// Caching stored events, state groups and auth chains is only safe while we are
// certain nothing has been deleted underneath us. Synapse's data is immutable
// in the sense that a state group's contents never change — but rooms get
// purged and events get deleted, and an entry admitted before we lost the
// invalidation signal may describe something that no longer exists. Disarming
// empties every cache, so re-arming always starts from the database.
func (s *Store) SetCachesArmed(armed bool) {
	if s == nil || s.caches == nil {
		return
	}
	s.caches.stateGroups.SetArmed(armed)
	s.caches.events.SetArmed(armed)
	s.caches.eventStateGroup.SetArmed(armed)
	s.caches.authChains.SetArmed(armed)
}

// PurgeCaches empties every cache without disarming it. Used when Synapse tells
// us it has deleted events, which our caches cannot express selectively: they
// are keyed by state group, event ID and auth-chain hash, none of which can be
// mapped back to a room.
func (s *Store) PurgeCaches() {
	if s == nil || s.caches == nil {
		return
	}
	s.caches.stateGroups.Purge()
	s.caches.events.Purge()
	s.caches.eventStateGroup.Purge()
	s.caches.authChains.Purge()
}

// DropEvent removes one event from the caches that key on an event ID. Used for
// the targeted invalidations Synapse streams per event, so an ordinary
// invalidation does not cost us the whole cache.
func (s *Store) DropEvent(eventID string) {
	if s == nil || s.caches == nil {
		return
	}
	s.caches.events.Remove(eventID)
	s.caches.eventStateGroup.Remove(eventID)
}

// CachesArmed reports whether the caches are currently permitted to serve.
func (s *Store) CachesArmed() bool {
	return s != nil && s.caches != nil && s.caches.events.Armed()
}
