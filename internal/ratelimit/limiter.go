package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrLimitExceeded is returned when a server has too many requests waiting.
var ErrLimitExceeded = errors.New("rate limit exceeded")

// Clock is injectable so the limiter can be tested without real delays.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Limiter applies Synapse's per-origin federation rate limiting.
//
// The algorithm is ported from Synapse's FederationRateLimiter rather than
// approximated, because the point is to answer exactly where Synapse would
// answer and throttle exactly where it throttles. It has three stages:
//
//   - reject: if too many of a server's requests are already waiting, refuse
//     immediately with 429;
//   - sleep: if the server has exceeded its request count for the window,
//     delay this request;
//   - queue: allow only a fixed number of a server's requests to run at once.
type Limiter struct {
	settings FederationSettings
	clock    Clock

	mu    sync.Mutex
	hosts map[string]*hostLimiter
}

// New builds a Limiter. A zero-valued setting takes Synapse's default.
func New(settings FederationSettings) *Limiter {
	return NewWithClock(settings, realClock{})
}

// NewWithClock builds a Limiter with an injectable clock.
func NewWithClock(settings FederationSettings, clock Clock) *Limiter {
	return &Limiter{
		settings: settings.withDefaults(),
		clock:    clock,
		hosts:    make(map[string]*hostLimiter),
	}
}

// Settings returns the effective settings, with defaults applied.
func (l *Limiter) Settings() FederationSettings { return l.settings }

// hostLimiter is the per-origin state.
type hostLimiter struct {
	mu sync.Mutex
	// requestTimes are the arrival times still inside the window.
	requestTimes []time.Time
	// sleeping and queued count requests waiting to run; together they decide
	// whether to reject.
	sleeping int
	queued   int
	// processing counts requests currently running.
	processing int
	// waiters are queued requests, released in arrival order.
	waiters []chan struct{}
	// lastUsed drives cleanup of servers we have not heard from.
	lastUsed time.Time
}

// Acquire waits until a request from host may proceed.
//
// It returns a release function that must be called when the request finishes.
// If the server has too much already waiting it returns ErrLimitExceeded
// without waiting at all.
func (l *Limiter) Acquire(ctx context.Context, host string) (func(), error) {
	// A limit of zero disables that stage, matching Synapse treating the
	// feature as off.
	if l.settings.RejectLimit == 0 && l.settings.Concurrent == 0 && l.settings.SleepLimit == 0 {
		return func() {}, nil
	}

	h := l.hostLimiter(host)
	now := l.clock.Now()

	h.mu.Lock()
	h.lastUsed = now
	h.prune(now, time.Duration(l.settings.WindowSize)*time.Millisecond)

	// Reject before recording the request, as Synapse does, so a rejected
	// request does not itself count towards the window.
	if h.sleeping+h.queued > l.settings.RejectLimit {
		h.mu.Unlock()
		return nil, ErrLimitExceeded
	}

	h.requestTimes = append(h.requestTimes, now)
	shouldSleep := len(h.requestTimes) > l.settings.SleepLimit
	if shouldSleep {
		h.sleeping++
	}
	h.mu.Unlock()

	if shouldSleep {
		err := l.clock.Sleep(ctx, time.Duration(l.settings.SleepDelay)*time.Millisecond)
		h.mu.Lock()
		h.sleeping--
		h.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}

	// Queue behind any of this server's requests already running.
	h.mu.Lock()
	if h.processing < l.settings.Concurrent {
		h.processing++
		h.mu.Unlock()
		return l.releaseFunc(h), nil
	}

	wait := make(chan struct{})
	h.waiters = append(h.waiters, wait)
	h.queued++
	h.mu.Unlock()

	select {
	case <-wait:
		// The releasing request handed its slot over, so processing is already
		// accounted for.
		return l.releaseFunc(h), nil
	case <-ctx.Done():
		h.abandon(wait)
		return nil, ctx.Err()
	}
}

// releaseFunc returns a function that frees the slot exactly once.
func (l *Limiter) releaseFunc(h *hostLimiter) func() {
	var once sync.Once
	return func() {
		once.Do(func() { h.release() })
	}
}

// release frees a processing slot, handing it directly to the next waiter so
// the slot cannot be taken by a request that arrived later.
func (h *hostLimiter) release() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.waiters) > 0 {
		next := h.waiters[0]
		h.waiters = h.waiters[1:]
		h.queued--
		// processing stays as it is: the slot moves straight to the waiter.
		close(next)
		return
	}
	h.processing--
}

// abandon removes a waiter whose request was cancelled before it started.
func (h *hostLimiter) abandon(wait chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, w := range h.waiters {
		if w == wait {
			h.waiters = append(h.waiters[:i], h.waiters[i+1:]...)
			h.queued--
			return
		}
	}
	// Not found: the slot was handed over between the context being cancelled
	// and this lock being taken, so it must be given back.
	select {
	case <-wait:
		if len(h.waiters) > 0 {
			next := h.waiters[0]
			h.waiters = h.waiters[1:]
			h.queued--
			close(next)
		} else {
			h.processing--
		}
	default:
	}
}

// prune drops arrival times that have fallen out of the window.
func (h *hostLimiter) prune(now time.Time, window time.Duration) {
	cutoff := now.Add(-window)
	kept := h.requestTimes[:0]
	for _, t := range h.requestTimes {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	h.requestTimes = kept
}

func (l *Limiter) hostLimiter(host string) *hostLimiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.hosts[host]
	if !ok {
		h = &hostLimiter{lastUsed: l.clock.Now()}
		l.hosts[host] = h
	}
	return h
}

// Cleanup discards state for servers idle for longer than maxIdle, so a server
// that contacts us once does not occupy memory forever. It reports how many
// were removed.
func (l *Limiter) Cleanup(maxIdle time.Duration) int {
	cutoff := l.clock.Now().Add(-maxIdle)
	l.mu.Lock()
	defer l.mu.Unlock()

	var removed int
	for host, h := range l.hosts {
		h.mu.Lock()
		idle := h.lastUsed.Before(cutoff) &&
			h.processing == 0 && h.queued == 0 && h.sleeping == 0
		h.mu.Unlock()
		if idle {
			delete(l.hosts, host)
			removed++
		}
	}
	return removed
}

// Hosts reports how many servers currently have state, for metrics.
func (l *Limiter) Hosts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.hosts)
}
