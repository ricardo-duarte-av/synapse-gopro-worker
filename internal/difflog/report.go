package difflog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// Report renders the persisted statistics for an operator deciding whether an
// endpoint has earned promotion out of shadow mode.
func Report(dir string, out io.Writer) error {
	stats, err := loadStats(filepath.Join(dir, statsName))
	if err != nil {
		return err
	}
	if stats.Since.IsZero() {
		return fmt.Errorf("no shadow statistics recorded in %s", dir)
	}

	elapsed := time.Since(stats.Since)
	fmt.Fprintf(out, "Shadow comparison since %s (%s, %d restarts)\n\n",
		stats.Since.Format(time.RFC3339), roundDuration(elapsed), stats.Restarts)

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ENDPOINT\tCOMPARED\tMATCHED\tMISMATCHED\tMATCH RATE\tLAST MISMATCH")

	names := make([]string, 0, len(stats.Endpoints))
	for name := range stats.Endpoints {
		names = append(names, name)
	}
	sort.Strings(names)

	if len(names) == 0 {
		if err := tw.Flush(); err != nil {
			return err
		}
		fmt.Fprintln(out, "(no comparisons recorded yet - is any endpoint in shadow or canary mode?)")
		return nil
	}

	for _, name := range names {
		e := stats.Endpoints[name]
		last := "never"
		if e.LastMismatch != nil {
			last = fmt.Sprintf("%s ago", roundDuration(time.Since(*e.LastMismatch)))
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%.4f%%\t%s\n",
			name, e.Compared, e.Matched, e.Mismatched, e.MatchRate()*100, last)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintf(out, "\nRecords logged: %d\n", stats.Logged)

	// These two are the reasons a clean-looking report might be a lie, so they
	// are called out rather than buried in the table.
	if stats.Dropped > 0 {
		fmt.Fprintf(out, "WARNING: %d records were dropped (queue full). The log is incomplete.\n", stats.Dropped)
	}
	if stats.WriteErrors > 0 {
		fmt.Fprintf(out, "WARNING: %d write errors. Check disk space and permissions.\n", stats.WriteErrors)
	}

	fmt.Fprintln(out)
	for _, name := range names {
		fmt.Fprintf(out, "%s: %s\n", name, verdict(stats.Endpoints[name], elapsed))
	}
	return nil
}

// promotionWindow is the soak period an endpoint must complete without any
// disagreement before it is a candidate for canary traffic.
const promotionWindow = 7 * 24 * time.Hour

func verdict(e *EndpointStats, elapsed time.Duration) string {
	switch {
	case e.Compared == 0:
		return "no comparisons yet"
	case e.Mismatched > 0 && e.LastMismatch != nil:
		since := time.Since(*e.LastMismatch)
		if since < promotionWindow {
			return fmt.Sprintf("NOT READY - %d mismatches, most recent %s ago; the clock restarts when they stop",
				e.Mismatched, roundDuration(since))
		}
		return fmt.Sprintf("ready - %d historical mismatches but none for %s",
			e.Mismatched, roundDuration(since))
	case elapsed < promotionWindow:
		return fmt.Sprintf("clean so far - %s of the %s soak elapsed",
			roundDuration(elapsed), roundDuration(promotionWindow))
	default:
		return fmt.Sprintf("ready - %d comparisons, no mismatches", e.Compared)
	}
}

func roundDuration(d time.Duration) string {
	switch {
	case d >= 24*time.Hour:
		return fmt.Sprintf("%.1fd", d.Hours()/24)
	case d >= time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	case d >= time.Minute:
		return fmt.Sprintf("%.0fm", d.Minutes())
	default:
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
}

// ReportToStdout is a convenience for the -diffstats flag.
func ReportToStdout(dir string) error {
	var sb strings.Builder
	if err := Report(dir, &sb); err != nil {
		return err
	}
	_, err := io.WriteString(os.Stdout, sb.String())
	return err
}
