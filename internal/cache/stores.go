package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// Settings bound each cache, in megabytes. Zero disables that cache.
type Settings struct {
	// StateGroupsMB bounds resolved state maps. This is the one that matters:
	// resolving state at an event is by far the most expensive read, and state
	// groups never change once written.
	StateGroupsMB int `yaml:"state_groups_mb"`
	// EventsMB bounds stored event JSON.
	EventsMB int `yaml:"events_mb"`
	// EventStateGroupsMB bounds the event-to-state-group mapping, which is tiny
	// per entry but saves a round trip on every request.
	EventStateGroupsMB int `yaml:"event_state_groups_mb"`
	// AuthChainsMB bounds computed auth chains.
	AuthChainsMB int `yaml:"auth_chains_mb"`
}

// DefaultSettings are deliberately modest, sized against a server whose largest
// room resolves to roughly 100MB of state.
func DefaultSettings() Settings {
	return Settings{
		StateGroupsMB:      512,
		EventsMB:           128,
		EventStateGroupsMB: 32,
		AuthChainsMB:       64,
	}
}

// WithDefaults fills unset fields.
func (s Settings) WithDefaults() Settings {
	d := DefaultSettings()
	if s.StateGroupsMB == 0 {
		s.StateGroupsMB = d.StateGroupsMB
	}
	if s.EventsMB == 0 {
		s.EventsMB = d.EventsMB
	}
	if s.EventStateGroupsMB == 0 {
		s.EventStateGroupsMB = d.EventStateGroupsMB
	}
	if s.AuthChainsMB == 0 {
		s.AuthChainsMB = d.AuthChainsMB
	}
	return s
}

// Validate reports whether the settings are usable. A negative value disables
// that cache, so only nonsensical combinations are rejected.
func (s Settings) Validate() error {
	for name, v := range map[string]int{
		"state_groups_mb":       s.StateGroupsMB,
		"events_mb":             s.EventsMB,
		"event_state_groups_mb": s.EventStateGroupsMB,
		"auth_chains_mb":        s.AuthChainsMB,
	} {
		if v < -1 {
			return fmt.Errorf("database.cache.%s must be -1 (disabled), 0 (default) or positive", name)
		}
	}
	return nil
}

// MB converts megabytes to bytes, treating a negative value as disabled.
func MB(n int) int64 {
	if n < 0 {
		return 0
	}
	return int64(n) << 20
}

// AuthChainKey derives a stable key for the auth chain of a set of event IDs.
//
// The chain is a pure function of its inputs, and the inputs are an unordered
// set, so the key is a hash over the sorted IDs. Hashing rather than
// concatenating matters because the input can run to a hundred thousand IDs.
func AuthChainKey(roomID string, eventIDs []string) [32]byte {
	sorted := make([]string, len(eventIDs))
	copy(sorted, eventIDs)
	sort.Strings(sorted)

	h := sha256.New()
	h.Write([]byte(roomID))
	h.Write([]byte{0})
	var lenBuf [8]byte
	for _, id := range sorted {
		// Length-prefix each ID so that different splits cannot collide.
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(id)))
		h.Write(lenBuf[:])
		h.Write([]byte(id))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// AnyEnabled reports whether any cache is sized above zero.
func (s Settings) AnyEnabled() bool {
	return s.StateGroupsMB > 0 || s.EventsMB > 0 ||
		s.EventStateGroupsMB > 0 || s.AuthChainsMB > 0
}
