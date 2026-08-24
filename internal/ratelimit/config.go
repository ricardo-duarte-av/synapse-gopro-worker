// Package ratelimit implements Synapse's per-origin federation rate limiting.
//
// The configuration deliberately mirrors Synapse's rc_federation block field
// for field, so an operator can copy their existing settings across without
// translating anything.
package ratelimit

import "fmt"

// FederationSettings mirrors Synapse's rc_federation.
//
// The defaults are Synapse's own, so an absent block behaves the same way
// Synapse would with no rc_federation configured.
type FederationSettings struct {
	// WindowSize is the period, in milliseconds, over which requests from one
	// server are counted.
	WindowSize int `yaml:"window_size"`
	// SleepLimit is how many requests a server may make within the window
	// before further ones are deliberately delayed.
	SleepLimit int `yaml:"sleep_limit"`
	// SleepDelay is how long, in milliseconds, a delayed request waits.
	SleepDelay int `yaml:"sleep_delay"`
	// RejectLimit is how many requests from one server may be waiting (asleep
	// or queued) before further ones are rejected with 429.
	RejectLimit int `yaml:"reject_limit"`
	// Concurrent is how many requests from one server are processed at once.
	Concurrent int `yaml:"concurrent"`
}

// DefaultFederationSettings matches Synapse's defaults.
func DefaultFederationSettings() FederationSettings {
	return FederationSettings{
		WindowSize:  1000,
		SleepLimit:  10,
		SleepDelay:  500,
		RejectLimit: 50,
		Concurrent:  3,
	}
}

// withDefaults fills unset fields, so a partial block behaves like Synapse's,
// which applies its defaults per field rather than all or nothing.
func (s FederationSettings) withDefaults() FederationSettings {
	d := DefaultFederationSettings()
	if s.WindowSize == 0 {
		s.WindowSize = d.WindowSize
	}
	if s.SleepLimit == 0 {
		s.SleepLimit = d.SleepLimit
	}
	if s.SleepDelay == 0 {
		s.SleepDelay = d.SleepDelay
	}
	if s.RejectLimit == 0 {
		s.RejectLimit = d.RejectLimit
	}
	if s.Concurrent == 0 {
		s.Concurrent = d.Concurrent
	}
	return s
}

// Validate reports whether the settings are usable.
func (s FederationSettings) Validate() error {
	switch {
	case s.WindowSize < 0:
		return fmt.Errorf("rc_federation: window_size must not be negative")
	case s.SleepLimit < 0:
		return fmt.Errorf("rc_federation: sleep_limit must not be negative")
	case s.SleepDelay < 0:
		return fmt.Errorf("rc_federation: sleep_delay must not be negative")
	case s.RejectLimit < 0:
		return fmt.Errorf("rc_federation: reject_limit must not be negative")
	case s.Concurrent < 0:
		return fmt.Errorf("rc_federation: concurrent must not be negative")
	}
	return nil
}

// RetryAfterMS is the value Synapse reports when it rejects a request:
// window_size divided by sleep_limit.
func (s FederationSettings) RetryAfterMS() int {
	s = s.withDefaults()
	if s.SleepLimit == 0 {
		return s.WindowSize
	}
	return s.WindowSize / s.SleepLimit
}
