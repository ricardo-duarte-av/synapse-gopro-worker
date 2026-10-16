package matrixstate

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestLiveStateIDsCoalesces checks the wiring rather than the mechanism:
// concurrent /state_ids requests for one event, through the real StateIDs,
// must compute once and return identical answers. The unit tests prove the
// flight works; this proves StateIDs actually uses it.
func TestLiveStateIDsCoalesces(t *testing.T) {
	r, s := liveResolver(t)
	ctx := context.Background()

	// The latest extremity of the room with the most current state: the
	// largest computation available, so the requests reliably overlap.
	var roomID, eventID string
	err := s.Pool().QueryRow(ctx, `
		SELECT f.room_id, f.event_id
		FROM event_forward_extremities AS f
		JOIN (SELECT room_id, count(*) AS n FROM current_state_events GROUP BY room_id) AS c
		  USING (room_id)
		JOIN events AS e USING (event_id)
		WHERE NOT e.outlier
		ORDER BY c.n DESC, e.stream_ordering DESC
		LIMIT 1`).Scan(&roomID, &eventID)
	if err != nil {
		t.Fatal(err)
	}

	computed := testutil.ToFloat64(stateIDsFlights.WithLabelValues("computed"))
	joined := testutil.ToFloat64(stateIDsFlights.WithLabelValues("joined"))

	const n = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	answers := make([]*StateIDsResponse, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			answers[i], errs[i] = r.StateIDs(ctx, ourServer, roomID, eventID)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if got := testutil.ToFloat64(stateIDsFlights.WithLabelValues("computed")) - computed; got >= n {
		t.Fatalf("%d concurrent requests computed %v times; nothing was shared", n, got)
	}
	if got := testutil.ToFloat64(stateIDsFlights.WithLabelValues("joined")) - joined; got == 0 {
		t.Fatal("no request joined an in-flight computation")
	}
	want := answers[0]
	slices.Sort(want.PDUIDs)
	slices.Sort(want.AuthChainIDs)
	for i, a := range answers[1:] {
		slices.Sort(a.PDUIDs)
		slices.Sort(a.AuthChainIDs)
		if !slices.Equal(a.PDUIDs, want.PDUIDs) || !slices.Equal(a.AuthChainIDs, want.AuthChainIDs) {
			t.Fatalf("request %d disagrees with request 0", i+1)
		}
	}
	t.Logf("%s: %d state, %d auth chain; computed %v, joined %v",
		roomID, len(want.PDUIDs), len(want.AuthChainIDs),
		testutil.ToFloat64(stateIDsFlights.WithLabelValues("computed"))-computed,
		testutil.ToFloat64(stateIDsFlights.WithLabelValues("joined"))-joined)
}
