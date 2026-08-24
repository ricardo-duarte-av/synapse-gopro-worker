package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock advances only when told to, so the sleep stage can be tested
// without real delays.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	slept  []time.Duration
	block  bool
	waking chan struct{}
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1000, 0), waking: make(chan struct{})}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.slept = append(c.slept, d)
	block := c.block
	c.mu.Unlock()
	if !block {
		return nil
	}
	select {
	case <-c.waking:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) sleepCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.slept)
}

func TestDefaultsMatchSynapse(t *testing.T) {
	// Synapse's own defaults, so an absent rc_federation behaves identically.
	d := DefaultFederationSettings()
	if d.WindowSize != 1000 || d.SleepLimit != 10 || d.SleepDelay != 500 ||
		d.RejectLimit != 50 || d.Concurrent != 3 {
		t.Errorf("defaults = %+v, do not match Synapse", d)
	}
}

func TestPartialSettingsFillPerField(t *testing.T) {
	// Synapse applies defaults field by field, so overriding one must not zero
	// the others.
	s := FederationSettings{Concurrent: 1}.withDefaults()
	if s.Concurrent != 1 {
		t.Errorf("Concurrent = %d, want the override", s.Concurrent)
	}
	if s.WindowSize != 1000 || s.SleepLimit != 10 {
		t.Errorf("other fields lost their defaults: %+v", s)
	}
}

func TestRetryAfterMatchesSynapse(t *testing.T) {
	// Synapse reports window_size / sleep_limit.
	s := FederationSettings{WindowSize: 1000, SleepLimit: 4}
	if got := s.RetryAfterMS(); got != 250 {
		t.Errorf("RetryAfterMS = %d, want 250", got)
	}
}

func TestConcurrentLimit(t *testing.T) {
	// Only `concurrent` requests from one server run at once; the rest queue.
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 100, SleepDelay: 0, RejectLimit: 100, Concurrent: 2,
	}, newFakeClock())
	ctx := context.Background()

	r1, _, err := l.Acquire(ctx, "a.example")
	if err != nil {
		t.Fatal(err)
	}
	r2, _, err := l.Acquire(ctx, "a.example")
	if err != nil {
		t.Fatal(err)
	}

	// A third must block until one of the first two finishes.
	started := make(chan struct{})
	go func() {
		r3, _, err := l.Acquire(ctx, "a.example")
		if err == nil {
			close(started)
			r3()
		}
	}()

	select {
	case <-started:
		t.Fatal("a third concurrent request was admitted")
	case <-time.After(100 * time.Millisecond):
	}

	r1()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the queued request was not released when a slot freed")
	}
	r2()
}

func TestLimitIsPerHost(t *testing.T) {
	// One noisy server must not throttle another.
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 100, SleepDelay: 0, RejectLimit: 100, Concurrent: 1,
	}, newFakeClock())
	ctx := context.Background()

	r1, _, err := l.Acquire(ctx, "a.example")
	if err != nil {
		t.Fatal(err)
	}
	defer r1()

	done := make(chan struct{})
	go func() {
		r2, _, err := l.Acquire(ctx, "b.example")
		if err == nil {
			r2()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a second server was blocked by the first server's limit")
	}
}

func TestRejectsWhenTooManyWaiting(t *testing.T) {
	// With concurrency 1 and a reject limit of 2, requests beyond the queue
	// depth are refused outright rather than queued indefinitely.
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 100, SleepDelay: 0, RejectLimit: 2, Concurrent: 1,
	}, newFakeClock())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Occupy the single processing slot.
	r1, _, err := l.Acquire(ctx, "a.example")
	if err != nil {
		t.Fatal(err)
	}
	defer r1()

	// Every further request either queues or is rejected. Acquire blocks while
	// queued, so each attempt needs its own goroutine.
	const attempts = 10
	var rejected, admitted atomic.Int32
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _, err := l.Acquire(ctx, "a.example")
			switch {
			case err == ErrLimitExceeded:
				rejected.Add(1)
			case err == nil:
				admitted.Add(1)
				r()
			}
		}()
	}

	// Give them time to queue or be rejected, then release so the queued ones
	// can drain and the goroutines finish.
	time.Sleep(300 * time.Millisecond)
	got := rejected.Load()
	cancel()
	wg.Wait()

	if got == 0 {
		t.Errorf("no requests were rejected despite %d attempts against a reject limit of 2", attempts)
	}
	t.Logf("of %d attempts: %d rejected, %d admitted", attempts, got, admitted.Load())
}

func TestSleepsPastTheWindowLimit(t *testing.T) {
	clock := newFakeClock()
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 2, SleepDelay: 500, RejectLimit: 100, Concurrent: 10,
	}, clock)
	ctx := context.Background()

	// The first two requests within the window are not delayed.
	for range 2 {
		r, _, err := l.Acquire(ctx, "a.example")
		if err != nil {
			t.Fatal(err)
		}
		r()
	}
	if got := clock.sleepCount(); got != 0 {
		t.Errorf("slept %d times within the limit, want 0", got)
	}

	// The third exceeds it and is delayed.
	r, _, err := l.Acquire(ctx, "a.example")
	if err != nil {
		t.Fatal(err)
	}
	r()
	if got := clock.sleepCount(); got != 1 {
		t.Errorf("slept %d times past the limit, want 1", got)
	}
	if clock.slept[0] != 500*time.Millisecond {
		t.Errorf("slept %v, want the configured sleep_delay", clock.slept[0])
	}
}

func TestWindowExpiryStopsSleeping(t *testing.T) {
	// Once the window passes, the count resets and requests stop being delayed.
	clock := newFakeClock()
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 1, SleepDelay: 10, RejectLimit: 100, Concurrent: 10,
	}, clock)
	ctx := context.Background()

	for range 3 {
		r, _, _ := l.Acquire(ctx, "a.example")
		r()
	}
	before := clock.sleepCount()
	if before == 0 {
		t.Fatal("expected some requests to be delayed")
	}

	clock.advance(2 * time.Second)
	r, _, err := l.Acquire(ctx, "a.example")
	if err != nil {
		t.Fatal(err)
	}
	r()
	if clock.sleepCount() != before {
		t.Error("a request after the window expired was still delayed")
	}
}

func TestCancelledRequestReleasesItsQueueSlot(t *testing.T) {
	// A remote server that hangs up while queued must not leak a slot, or the
	// limiter would wedge shut for that server.
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 100, SleepDelay: 0, RejectLimit: 100, Concurrent: 1,
	}, newFakeClock())

	r1, _, err := l.Acquire(context.Background(), "a.example")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, err := l.Acquire(ctx, "a.example")
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("a cancelled request reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not unblock the queued request")
	}

	r1()

	// The limiter must still admit new work.
	done := make(chan struct{})
	go func() {
		r, _, err := l.Acquire(context.Background(), "a.example")
		if err == nil {
			r()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("limiter wedged after a cancelled request")
	}
}

func TestCleanupRemovesIdleHosts(t *testing.T) {
	clock := newFakeClock()
	l := NewWithClock(DefaultFederationSettings(), clock)
	r, _, _ := l.Acquire(context.Background(), "a.example")
	r()
	if l.Hosts() != 1 {
		t.Fatalf("Hosts = %d, want 1", l.Hosts())
	}

	// Still recent: kept.
	if got := l.Cleanup(time.Hour); got != 0 {
		t.Errorf("cleaned %d hosts that were still recent", got)
	}
	clock.advance(2 * time.Hour)
	if got := l.Cleanup(time.Hour); got != 1 {
		t.Errorf("cleaned %d idle hosts, want 1", got)
	}
	if l.Hosts() != 0 {
		t.Errorf("Hosts = %d after cleanup, want 0", l.Hosts())
	}
}

func TestCleanupKeepsBusyHosts(t *testing.T) {
	// A host with a request in flight must never be discarded, or its
	// accounting would be lost.
	clock := newFakeClock()
	l := NewWithClock(DefaultFederationSettings(), clock)
	r, _, _ := l.Acquire(context.Background(), "a.example")
	defer r()

	clock.advance(2 * time.Hour)
	if got := l.Cleanup(time.Hour); got != 0 {
		t.Errorf("cleaned %d hosts with work in flight, want 0", got)
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	l := NewWithClock(FederationSettings{
		WindowSize: 1000, SleepLimit: 1000, SleepDelay: 0, RejectLimit: 1000, Concurrent: 4,
	}, newFakeClock())
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			host := []string{"a.example", "b.example"}[g%2]
			for range 50 {
				r, _, err := l.Acquire(ctx, host)
				if err != nil {
					continue
				}
				r()
			}
		}(g)
	}
	wg.Wait()
}
