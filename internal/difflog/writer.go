package difflog

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults chosen so a long shadow run cannot fill a disk: at most
// MaxFiles * MaxFileBytes of uncompressed-equivalent data, and rotated files
// are gzipped, which typically shrinks JSON by 10x or more.
const (
	DefaultMaxFileBytes = 64 << 20 // 64 MiB
	DefaultMaxFiles     = 10
	DefaultQueueSize    = 1024
	DefaultBodyLimit    = 256 << 10 // 256 KiB per body
	DefaultListLimit    = 100       // event IDs per diff list
	DefaultStatsEvery   = 30 * time.Second
)

// Options configures a Writer.
type Options struct {
	// Dir is the directory holding diffs.jsonl, its rotated siblings and
	// stats.json.
	Dir string

	// MaxFileBytes is the size at which the active file is rotated.
	MaxFileBytes int64
	// MaxFiles is how many rotated files to keep, oldest deleted first.
	MaxFiles int
	// Compress gzips rotated files. On by default.
	Compress bool

	// QueueSize bounds the handoff to the writer goroutine. When full,
	// records are dropped rather than blocking a federation request.
	QueueSize int

	// BodyLimit caps each retained response body. Zero uses the default;
	// negative omits bodies entirely.
	BodyLimit int
	// ListLimit caps how many event IDs are listed per diff field.
	ListLimit int

	// StatsEvery is how often cumulative counters are checkpointed to disk.
	StatsEvery time.Duration

	// Now is injectable for tests.
	Now func() time.Time
}

func (o *Options) applyDefaults() {
	if o.MaxFileBytes <= 0 {
		o.MaxFileBytes = DefaultMaxFileBytes
	}
	if o.MaxFiles <= 0 {
		o.MaxFiles = DefaultMaxFiles
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	if o.BodyLimit == 0 {
		o.BodyLimit = DefaultBodyLimit
	}
	if o.ListLimit <= 0 {
		o.ListLimit = DefaultListLimit
	}
	if o.StatsEvery <= 0 {
		o.StatsEvery = DefaultStatsEvery
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

const (
	activeName = "diffs.jsonl"
	statsName  = "stats.json"
)

// Writer persists diff records to a rotating JSON Lines file.
//
// Log is safe for concurrent use and never blocks: it hands the record to a
// background goroutine and drops it if the queue is full, since a stalled
// federation request is worse than a missing log line.
type Writer struct {
	opts Options

	queue  chan *Record
	done   chan struct{}
	closed atomic.Bool

	mu   sync.Mutex
	file *os.File
	size int64

	stats  *Stats
	statMu sync.Mutex

	dropped atomic.Uint64
	// errs counts write failures, surfaced via Stats so a full or read-only
	// disk does not fail silently.
	errs atomic.Uint64
}

// Open prepares a Writer, creating the directory and resuming the existing
// stats and active log file if present.
func Open(opts Options) (*Writer, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("difflog: Dir is required")
	}
	opts.applyDefaults()

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("difflog: create dir: %w", err)
	}

	w := &Writer{
		opts:  opts,
		queue: make(chan *Record, opts.QueueSize),
		done:  make(chan struct{}),
	}

	stats, err := loadStats(filepath.Join(opts.Dir, statsName))
	if err != nil {
		return nil, err
	}
	if stats.Since.IsZero() {
		stats.Since = opts.Now().UTC()
	}
	stats.Restarts++
	w.stats = stats

	if err := w.openActive(); err != nil {
		return nil, err
	}

	go w.loop()
	return w, nil
}

// openActive opens the active log file for appending. The caller must not hold
// w.mu.
func (w *Writer) openActive() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.openActiveLocked()
}

func (w *Writer) openActiveLocked() error {
	path := filepath.Join(w.opts.Dir, activeName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("difflog: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("difflog: stat %s: %w", path, err)
	}
	w.file, w.size = f, info.Size()
	return nil
}

// Log queues a record. It never blocks and is safe for concurrent use. A nil
// Writer is a no-op, so callers need no nil checks when diff logging is off.
//
// It reports whether the record was queued; a false result means the queue was
// full and the record was dropped, which is counted in Stats.
func (w *Writer) Log(rec *Record) bool {
	if w == nil || w.closed.Load() || rec == nil {
		return false
	}
	if rec.Time.IsZero() {
		rec.Time = w.opts.Now().UTC()
	}
	w.truncate(rec)

	select {
	case w.queue <- rec:
		return true
	default:
		w.dropped.Add(1)
		return false
	}
}

// truncate applies the body and list caps so one pathological /state response
// cannot dominate the log.
func (w *Writer) truncate(rec *Record) {
	if w.opts.BodyLimit < 0 {
		rec.SynapseBody, rec.NativeBody = nil, nil
	} else {
		limit := w.opts.BodyLimit
		if len(rec.SynapseBody) > limit {
			rec.SynapseBody = json.RawMessage(clip(rec.SynapseBody, limit))
			rec.BodiesTruncated = true
		}
		if len(rec.NativeBody) > limit {
			rec.NativeBody = json.RawMessage(clip(rec.NativeBody, limit))
			rec.BodiesTruncated = true
		}
	}

	if rec.Diff == nil {
		return
	}
	for i := range rec.Diff.Fields {
		f := &rec.Diff.Fields[i]
		for _, list := range []*[]string{&f.MissingFromNative, &f.ExtraInNative, &f.ContentMismatch} {
			if len(*list) > w.opts.ListLimit {
				*list = (*list)[:w.opts.ListLimit]
				f.ListsTruncated = true
			}
		}
	}
}

// clip cuts a body to n bytes and marks it as a JSON string so the record
// remains valid JSON Lines even when the payload is truncated mid-structure.
func clip(b []byte, n int) []byte {
	quoted, err := json.Marshal(string(b[:n]) + "…[truncated]")
	if err != nil {
		return []byte(`"[truncated]"`)
	}
	return quoted
}

// Observe records the outcome of a shadow comparison for the cumulative
// counters, whether or not it matched. Call it for every compared request.
func (w *Writer) Observe(endpoint string, matched bool) {
	if w == nil {
		return
	}
	w.statMu.Lock()
	defer w.statMu.Unlock()
	w.stats.observe(endpoint, matched)
}

func (w *Writer) loop() {
	defer close(w.done)

	ticker := time.NewTicker(w.opts.StatsEvery)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-w.queue:
			if !ok {
				w.flushStats()
				return
			}
			w.write(rec)
		case <-ticker.C:
			w.flushStats()
		}
	}
}

func (w *Writer) write(rec *Record) {
	line, err := json.Marshal(rec)
	if err != nil {
		w.errs.Add(1)
		return
	}
	line = append(line, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		w.errs.Add(1)
		return
	}
	if w.size+int64(len(line)) > w.opts.MaxFileBytes && w.size > 0 {
		if err := w.rotateLocked(); err != nil {
			w.errs.Add(1)
			return
		}
	}
	n, err := w.file.Write(line)
	w.size += int64(n)
	if err != nil {
		w.errs.Add(1)
		return
	}
	// Mismatches are rare by design, so syncing each one is affordable and
	// means a crash never loses the evidence that prompted it.
	if err := w.file.Sync(); err != nil {
		w.errs.Add(1)
	}

	w.statMu.Lock()
	w.stats.Logged++
	w.statMu.Unlock()
}

// rotateLocked closes the active file, shifts the numbered history along and
// reopens an empty active file. w.mu must be held.
func (w *Writer) rotateLocked() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	ext := ""
	if w.opts.Compress {
		ext = ".gz"
	}
	// Shift diffs.jsonl.N -> diffs.jsonl.N+1, dropping the oldest.
	for i := w.opts.MaxFiles - 1; i >= 1; i-- {
		from := filepath.Join(w.opts.Dir, fmt.Sprintf("%s.%d%s", activeName, i, ext))
		to := filepath.Join(w.opts.Dir, fmt.Sprintf("%s.%d%s", activeName, i+1, ext))
		if i+1 > w.opts.MaxFiles {
			_ = os.Remove(from)
			continue
		}
		_ = os.Rename(from, to)
	}

	active := filepath.Join(w.opts.Dir, activeName)
	first := filepath.Join(w.opts.Dir, fmt.Sprintf("%s.1%s", activeName, ext))
	if w.opts.Compress {
		if err := gzipFile(active, first); err != nil {
			return err
		}
		if err := os.Remove(active); err != nil {
			return err
		}
	} else if err := os.Rename(active, first); err != nil {
		return err
	}

	w.statMu.Lock()
	w.stats.Rotations++
	w.statMu.Unlock()

	return w.openActiveLocked()
}

func gzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	zw := gzip.NewWriter(out)
	if _, err := io.Copy(zw, in); err != nil {
		_ = zw.Close()
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return out.Sync()
}

func (w *Writer) flushStats() {
	w.statMu.Lock()
	snapshot := w.stats.clone()
	w.statMu.Unlock()

	snapshot.Dropped = w.dropped.Load()
	snapshot.WriteErrors = w.errs.Load()
	snapshot.Updated = w.opts.Now().UTC()

	if err := saveStats(filepath.Join(w.opts.Dir, statsName), snapshot); err != nil {
		w.errs.Add(1)
	}
}

// Snapshot returns the current cumulative statistics.
func (w *Writer) Snapshot() Stats {
	if w == nil {
		return Stats{}
	}
	w.statMu.Lock()
	s := w.stats.clone()
	w.statMu.Unlock()
	s.Dropped = w.dropped.Load()
	s.WriteErrors = w.errs.Load()
	return *s
}

// Close drains the queue, checkpoints the stats and closes the file.
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	if w.closed.Swap(true) {
		return nil
	}
	close(w.queue)
	<-w.done

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// Files lists the log files currently on disk, newest first, for operator
// tooling.
func (w *Writer) Files() ([]string, error) {
	entries, err := os.ReadDir(w.opts.Dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), activeName) {
			names = append(names, filepath.Join(w.opts.Dir, e.Name()))
		}
	}
	sort.Strings(names)
	return names, nil
}
