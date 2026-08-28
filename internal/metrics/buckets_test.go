package metrics

import (
	"sort"
	"testing"
)

// A sleep of exactly sleep_delay must not be reported as something else.
//
// The previous buckets jumped .1 -> .25, so a 100ms sleep_delay landed
// mid-bucket and histogram_quantile interpolated it to ~234ms. The raw counts
// showed every sleep in that one bucket, nothing sleeping twice -- the
// quantile was describing the bucket, not the wait.
//
// A boundary just above each common setting puts a single sleep at the bottom
// of its bucket, where interpolation has almost nothing to distort.
func TestLimiterBucketsBracketCommonSleepDelays(t *testing.T) {
	if !sort.Float64sAreSorted(limiterWaitBuckets) {
		t.Fatal("buckets must be ascending")
	}
	for _, delay := range []float64{.05, .1, .25, .5} {
		// A real sleep overshoots: time.Sleep(100ms) returns a little after
		// 100ms, so the observed wait is just *above* sleep_delay and lands in
		// the bucket after the boundary, not the one ending at it. Testing the
		// bucket containing exactly `delay` measures a value that never occurs
		// -- which is how this test first failed against correct buckets.
		observed := delay * 1.001

		var lower, upper float64
		for _, b := range limiterWaitBuckets {
			if b < observed {
				lower = b
			}
			if b >= observed {
				upper = b
				break
			}
		}
		// The bucket containing `delay` must be tight around it, so a value at
		// `delay` is not interpolated far from the truth.
		if width := upper - lower; width > delay*0.2 {
			t.Errorf("a sleep of %.4fs (sleep_delay %.3f) falls in bucket (%.4f, %.4f], "+
				"width %.4f: too wide, a single sleep will be misreported",
				observed, delay, lower, upper, width)
		}
	}
}

// Two sleeps must be distinguishable from one, or a request delayed twice
// looks the same as a request delayed once.
func TestLimiterBucketsSeparateOneSleepFromTwo(t *testing.T) {
	for _, delay := range []float64{.05, .1, .25} {
		bucketOf := func(v float64) float64 {
			for _, b := range limiterWaitBuckets {
				if v <= b {
					return b
				}
			}
			return -1
		}
		if one, two := bucketOf(delay), bucketOf(2*delay); one == two {
			t.Errorf("one sleep and two sleeps of %.3fs share bucket %.4f", delay, one)
		}
	}
}
