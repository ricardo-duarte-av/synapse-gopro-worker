package cache

import (
	"context"
	"testing"
)

func TestTraceSummary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record func(ctx context.Context)
		want   string
		detail string
	}{
		{"nothing looked up", func(context.Context) {}, "", ""},
		{"all hits", func(ctx context.Context) {
			Record(ctx, "state_groups", true)
			RecordN(ctx, "events", 3, 3)
		}, "hit", "events=3/3 state_groups=1/1"},
		{"all misses", func(ctx context.Context) {
			Record(ctx, "auth_chains", false)
		}, "miss", "auth_chains=0/1"},
		{"mixed", func(ctx context.Context) {
			Record(ctx, "state_groups", true)
			Record(ctx, "auth_chains", false)
		}, "partial", "auth_chains=0/1 state_groups=1/1"},
		{"partial within one cache", func(ctx context.Context) {
			RecordN(ctx, "events", 2, 3)
		}, "partial", "events=2/3"},
		{"joined wins", func(ctx context.Context) {
			Record(ctx, "state_groups", false)
			MarkJoined(ctx)
		}, "joined", "state_groups=0/1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, tr := WithTrace(context.Background())
			tc.record(ctx)
			if got := tr.Summary(); got != tc.want {
				t.Errorf("Summary = %q, want %q", got, tc.want)
			}
			if got := tr.Detail(); got != tc.detail {
				t.Errorf("Detail = %q, want %q", got, tc.detail)
			}
		})
	}
}

// Recording without a trace, and reading a nil one, must be harmless: the
// store records on every lookup, including ones no request is tracing.
func TestTraceAbsent(t *testing.T) {
	Record(context.Background(), "events", true)
	MarkJoined(context.Background())
	var tr *Trace
	if tr.Summary() != "" || tr.Detail() != "" {
		t.Fatal("nil trace reported something")
	}
}
