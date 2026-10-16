package matrixstate

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

func testFlight() *stateIDsFlight {
	return &stateIDsFlight{timeout: 10 * time.Second}
}

// Concurrent requests for one key must compute once and all get the answer.
func TestStateIDsFlightCollapsesConcurrentRequests(t *testing.T) {
	f := testFlight()
	var calls atomic.Int32
	release := make(chan struct{})
	fn := func(ctx context.Context) (*StateIDsResponse, error) {
		calls.Add(1)
		<-release
		return &StateIDsResponse{PDUIDs: []string{"$a", "$b"}, AuthChainIDs: []string{"$c"}}, nil
	}

	const n = 50
	var wg sync.WaitGroup
	results := make([]*StateIDsResponse, n)
	errs := make([]error, n)
	started := make(chan struct{}, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			results[i], errs[i] = f.do(context.Background(), "k", fn)
		}()
	}
	for range n {
		<-started
	}
	// Give every goroutine time to join before the computation finishes.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("computed %d times, want 1", got)
	}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if len(results[i].PDUIDs) != 2 || len(results[i].AuthChainIDs) != 1 {
			t.Fatalf("caller %d got %+v", i, results[i])
		}
	}

	// Each caller owns its slices: editing one answer must not reach another.
	results[0].PDUIDs[0] = "$mutated"
	if results[1].PDUIDs[0] != "$a" {
		t.Fatal("callers share a slice; one caller's edit leaked into another's answer")
	}
}

// The first caller hanging up must not cancel the work others wait on.
func TestStateIDsFlightSurvivesLeaderCancel(t *testing.T) {
	f := testFlight()
	release := make(chan struct{})
	var sawCancel atomic.Bool
	fn := func(ctx context.Context) (*StateIDsResponse, error) {
		<-release
		if ctx.Err() != nil {
			sawCancel.Store(true)
			return nil, ctx.Err()
		}
		return &StateIDsResponse{PDUIDs: []string{"$a"}, AuthChainIDs: []string{}}, nil
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := f.do(leaderCtx, "k", fn)
		leaderErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	followerRes := make(chan *StateIDsResponse, 1)
	followerErr := make(chan error, 1)
	go func() {
		r, err := f.do(context.Background(), "k", fn)
		followerRes <- r
		followerErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: got %v, want context.Canceled", err)
	}
	close(release)

	if err := <-followerErr; err != nil {
		t.Fatalf("follower: %v", err)
	}
	if r := <-followerRes; len(r.PDUIDs) != 1 {
		t.Fatalf("follower got %+v", r)
	}
	if sawCancel.Load() {
		t.Fatal("the shared computation saw the leader's cancellation")
	}
}

// A caller whose deadline passes stops waiting promptly, and reports its own
// deadline so the canary classifies it as a timeout rather than client_gone.
func TestStateIDsFlightWaiterDeadline(t *testing.T) {
	f := testFlight()
	release := make(chan struct{})
	defer close(release)
	fn := func(ctx context.Context) (*StateIDsResponse, error) {
		<-release
		return &StateIDsResponse{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.do(ctx, "k", fn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("waiter did not return promptly at its deadline")
	}
}

// A panic in the shared computation must come back as an error. singleflight
// would otherwise re-panic it on a goroutine nothing can recover.
func TestStateIDsFlightPanicIsAnError(t *testing.T) {
	f := testFlight()
	_, err := f.do(context.Background(), "k", func(context.Context) (*StateIDsResponse, error) {
		panic("boom")
	})
	if err == nil {
		t.Fatal("panic was not turned into an error")
	}
}

// Errors are shared like answers, and nothing is remembered afterwards: a
// later request computes afresh.
func TestStateIDsFlightDoesNotMemoise(t *testing.T) {
	f := testFlight()
	var calls atomic.Int32
	fn := func(context.Context) (*StateIDsResponse, error) {
		if calls.Add(1) == 1 {
			return nil, errEventNotFound("$x")
		}
		return &StateIDsResponse{PDUIDs: []string{}, AuthChainIDs: []string{}}, nil
	}
	if _, err := f.do(context.Background(), "k", fn); err == nil {
		t.Fatal("first call: want the computation's error")
	}
	r, err := f.do(context.Background(), "k", fn)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	// Empty sets must still serialise as arrays after cloning, never null.
	body, _ := json.Marshal(r)
	if string(body) != `{"pdu_ids":[],"auth_chain_ids":[]}` {
		t.Fatalf("got %s", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("computed %d times, want 2", calls.Load())
	}
}

// A request that waited on another's computation says so in its trace, and
// the leader's trace -- reached through the detached context -- records the
// computation's own lookups.
func TestStateIDsFlightMarksJoined(t *testing.T) {
	f := testFlight()
	release := make(chan struct{})
	fn := func(ctx context.Context) (*StateIDsResponse, error) {
		cache.Record(ctx, "state_groups", false)
		<-release
		return &StateIDsResponse{}, nil
	}
	leaderCtx, leader := cache.WithTrace(context.Background())
	followerCtx, follower := cache.WithTrace(context.Background())

	done := make(chan struct{})
	go func() { _, _ = f.do(leaderCtx, "k", fn); close(done) }()
	time.Sleep(20 * time.Millisecond)
	go func() { time.Sleep(20 * time.Millisecond); close(release) }()
	if _, err := f.do(followerCtx, "k", fn); err != nil {
		t.Fatal(err)
	}
	<-done

	if got := follower.Summary(); got != "joined" {
		t.Errorf("follower: %q, want joined", got)
	}
	if got := leader.Summary(); got != "miss" {
		t.Errorf("leader: %q, want miss -- the computation's lookups belong to it", got)
	}
}
