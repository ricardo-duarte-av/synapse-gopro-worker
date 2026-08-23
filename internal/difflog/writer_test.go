package difflog

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testRecord(endpoint string) *Record {
	return &Record{
		Kind:     KindBody,
		Endpoint: endpoint,
		Origin:   "remote.example",
		URI:      "/_matrix/federation/v1/state_ids/%21r%3Aex.com?event_id=%24e",
		RoomID:   "!r:ex.com",
		Diff: &Diff{Fields: []FieldDiff{{
			Field:             "pdu_ids",
			MissingFromNative: []string{"$a"},
			SynapseCount:      2,
			NativeCount:       1,
		}}},
	}
}

func readRecords(t *testing.T, path string) []Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		defer zr.Close()
		r = zr
	}

	var out []Record
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("invalid JSON line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWriterPersistsRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		rec := testRecord("state_ids")
		rec.EventID = fmt.Sprintf("$e%d", i)
		if !w.Log(rec) {
			t.Fatal("Log returned false on an empty queue")
		}
		w.Observe("state_ids", false)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recs := readRecords(t, filepath.Join(dir, activeName))
	if len(recs) != 3 {
		t.Fatalf("wrote %d records, want 3", len(recs))
	}
	if recs[0].Endpoint != "state_ids" || recs[0].RoomID != "!r:ex.com" {
		t.Errorf("record not round-tripped: %+v", recs[0])
	}
	if recs[2].EventID != "$e2" {
		t.Errorf("EventID = %q, want $e2", recs[2].EventID)
	}
	if recs[0].Time.IsZero() {
		t.Error("Time was not stamped")
	}
	if len(recs[0].Diff.Fields) != 1 || recs[0].Diff.Fields[0].Field != "pdu_ids" {
		t.Errorf("diff not round-tripped: %+v", recs[0].Diff)
	}
}

// TestWriterSurvivesRestart is the point of the stats file: a shadow run spans
// weeks and many restarts, and the evidence has to accumulate across them.
func TestWriterSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	w1, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		w1.Observe("state_ids", true)
	}
	w1.Observe("state_ids", false)
	w1.Log(testRecord("state_ids"))
	if err := w1.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	got := w2.Snapshot()
	if got.Compared != 11 {
		t.Errorf("Compared = %d, want 11 carried over from the previous run", got.Compared)
	}
	if got.Matched != 10 || got.Mismatched != 1 {
		t.Errorf("Matched/Mismatched = %d/%d, want 10/1", got.Matched, got.Mismatched)
	}
	if got.Restarts != 2 {
		t.Errorf("Restarts = %d, want 2", got.Restarts)
	}
	if got.Endpoints["state_ids"].LastMismatch == nil {
		t.Error("LastMismatch was not preserved")
	}

	// New records must append to the existing file, not truncate it.
	w2.Log(testRecord("state_ids"))
	w2.Observe("state_ids", false)
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if recs := readRecords(t, filepath.Join(dir, activeName)); len(recs) != 2 {
		t.Errorf("after restart the log holds %d records, want 2 (append, not truncate)", len(recs))
	}
}

func TestWriterRotates(t *testing.T) {
	dir := t.TempDir()
	// A tiny cap so a handful of records forces several rotations.
	w, err := Open(Options{Dir: dir, MaxFileBytes: 512, MaxFiles: 3, Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		rec := testRecord("state")
		rec.EventID = fmt.Sprintf("$event%d", i)
		w.Log(rec)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if got := w.Snapshot().Rotations; got == 0 {
		t.Fatal("no rotation happened despite exceeding the size cap")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var rotated []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), activeName+".") {
			rotated = append(rotated, e.Name())
		}
	}
	// MaxFiles bounds the history; without this the disk fills over weeks.
	if len(rotated) > 3 {
		t.Errorf("kept %d rotated files (%v), want at most MaxFiles=3", len(rotated), rotated)
	}
	if len(rotated) == 0 {
		t.Fatal("no rotated files were kept")
	}
	for _, name := range rotated {
		if !strings.HasSuffix(name, ".gz") {
			t.Errorf("rotated file %q is not compressed", name)
		}
		// Rotated files must remain readable, or the history is worthless.
		if recs := readRecords(t, filepath.Join(dir, name)); len(recs) == 0 {
			t.Errorf("rotated file %q has no readable records", name)
		}
	}
}

func TestLogNeverBlocks(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, QueueSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Far more records than the queue can hold. The contract is that Log
	// returns promptly regardless: a stalled federation request is worse than
	// a missing log line.
	done := make(chan int, 1)
	go func() {
		queued := 0
		for range 10000 {
			if w.Log(testRecord("event")) {
				queued++
			}
		}
		done <- queued
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Log blocked when the queue was full")
	}

	// Dropping must be visible, since a lossy log invalidates the gate.
	if w.Snapshot().Dropped == 0 {
		t.Error("Dropped = 0, want the overflow to be counted")
	}
}

func TestLogIsConcurrencySafe(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxFileBytes: 4096, MaxFiles: 5, Compress: true})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 50 {
				rec := testRecord("state")
				rec.EventID = fmt.Sprintf("$g%d-%d", g, i)
				w.Log(rec)
				w.Observe("state", i%2 == 0)
			}
		}(g)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	s := w.Snapshot()
	if s.Compared != 400 {
		t.Errorf("Compared = %d, want 400", s.Compared)
	}
	if s.Matched+s.Mismatched != s.Compared {
		t.Errorf("Matched+Mismatched = %d, want %d", s.Matched+s.Mismatched, s.Compared)
	}
}

func TestTruncation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, BodyLimit: 64, ListLimit: 3})
	if err != nil {
		t.Fatal(err)
	}

	rec := testRecord("state")
	rec.SynapseBody = json.RawMessage(`{"pdus":[` + strings.Repeat(`"$aaaaaaaa",`, 100) + `"$z"]}`)
	rec.NativeBody = json.RawMessage(`{"pdus":[]}`)
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = fmt.Sprintf("$id%d", i)
	}
	rec.Diff.Fields[0].MissingFromNative = ids

	w.Log(rec)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	recs := readRecords(t, filepath.Join(dir, activeName))
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	got := recs[0]
	if !got.BodiesTruncated {
		t.Error("BodiesTruncated = false, want true")
	}
	// The small body must be left alone.
	if string(got.NativeBody) != `{"pdus":[]}` {
		t.Errorf("NativeBody = %s, want the original", got.NativeBody)
	}
	if n := len(got.Diff.Fields[0].MissingFromNative); n != 3 {
		t.Errorf("MissingFromNative kept %d ids, want ListLimit=3", n)
	}
	if !got.Diff.Fields[0].ListsTruncated {
		t.Error("ListsTruncated = false, want true")
	}
}

func TestOmitBodies(t *testing.T) {
	// A negative BodyLimit is the privacy-conscious setting: log that a
	// mismatch happened and its shape, but never the room content.
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, BodyLimit: -1})
	if err != nil {
		t.Fatal(err)
	}
	rec := testRecord("event")
	rec.SynapseBody = json.RawMessage(`{"secret":"data"}`)
	rec.NativeBody = json.RawMessage(`{"secret":"data"}`)
	w.Log(rec)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, activeName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret") {
		t.Errorf("body was logged despite BodyLimit < 0: %s", data)
	}
}

func TestStatsFileIsValidAndAtomic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, StatsEvery: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.Observe("state", true)
	time.Sleep(100 * time.Millisecond)

	data, err := os.ReadFile(filepath.Join(dir, statsName))
	if err != nil {
		t.Fatalf("stats file was not checkpointed: %v", err)
	}
	var s Stats
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("stats file is not valid JSON: %v", err)
	}
	if s.Compared != 1 {
		t.Errorf("Compared = %d, want 1", s.Compared)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// No temp files must be left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("left a temp file behind: %s", e.Name())
		}
	}
}

func TestCorruptStatsDoesNotPreventStartup(t *testing.T) {
	// The worker must start even if the stats file was truncated by a crash;
	// the diff log itself is the authoritative record.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, statsName), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed on a corrupt stats file: %v", err)
	}
	defer w.Close()
	if got := w.Snapshot().Compared; got != 0 {
		t.Errorf("Compared = %d, want 0", got)
	}
}

func TestMatchRate(t *testing.T) {
	var e *EndpointStats
	if got := e.MatchRate(); got != 1 {
		t.Errorf("nil MatchRate = %v, want 1", got)
	}
	e = &EndpointStats{Compared: 4, Matched: 3}
	if got := e.MatchRate(); got != 0.75 {
		t.Errorf("MatchRate = %v, want 0.75", got)
	}
	s := &Stats{Compared: 0}
	if got := s.MatchRate(); got != 1 {
		t.Errorf("empty MatchRate = %v, want 1", got)
	}
}

func TestDiffEmpty(t *testing.T) {
	if !(*Diff)(nil).Empty() {
		t.Error("nil diff should be empty")
	}
	if !(&Diff{}).Empty() {
		t.Error("diff with no fields should be empty")
	}
	d := &Diff{Fields: []FieldDiff{{Field: "pdu_ids", SynapseCount: 2, NativeCount: 2}}}
	if !d.Empty() {
		t.Error("field with equal counts and no differences should be empty")
	}
	d = &Diff{Fields: []FieldDiff{{Field: "pdu_ids", ExtraInNative: []string{"$a"}}}}
	if d.Empty() {
		t.Error("field with an extra id should not be empty")
	}
}

func TestNilWriterIsSafe(t *testing.T) {
	// Diff logging is off while every endpoint is in proxy mode, so callers
	// hold a nil *Writer and must not need to check for it.
	var w *Writer
	if w.Log(testRecord("state")) {
		t.Error("Log on a nil Writer returned true")
	}
	w.Observe("state", true)
	if got := w.Snapshot().Compared; got != 0 {
		t.Errorf("Snapshot().Compared = %d, want 0", got)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close on a nil Writer = %v, want nil", err)
	}
}

// TestLogDuringCloseDoesNotPanic covers a shutdown race that would otherwise
// crash the worker: Log sending on the queue at the moment Close closes it.
// A send on a closed channel panics, and shutdown routinely catches in-flight
// shadow comparisons.
func TestLogDuringCloseDoesNotPanic(t *testing.T) {
	for range 20 {
		w, err := Open(Options{Dir: t.TempDir(), QueueSize: 4})
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 200 {
					// Must not panic, whether or not the writer is closed.
					w.Log(testRecord("state"))
				}
			}()
		}

		go func() {
			time.Sleep(time.Millisecond)
			_ = w.Close()
		}()

		wg.Wait()
		_ = w.Close()
	}
}
