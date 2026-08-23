package difflog

import (
	"github.com/daedric/synapse-gopro-worker/internal/config"
)

// FromConfig translates the YAML configuration into writer Options.
func FromConfig(c config.DiffLog) Options {
	o := Options{
		Dir:       c.Dir,
		MaxFiles:  c.MaxFiles,
		Compress:  c.CompressRotated(),
		QueueSize: c.QueueSize,
		ListLimit: c.ListLimit,
	}
	if c.MaxFileMB > 0 {
		o.MaxFileBytes = int64(c.MaxFileMB) << 20
	}
	switch {
	case c.BodyKB < 0:
		// Preserved as negative so bodies are omitted entirely.
		o.BodyLimit = -1
	case c.BodyKB > 0:
		o.BodyLimit = c.BodyKB << 10
	}
	o.StatsEvery = DefaultStatsEvery
	return o
}

// OpenFromConfig opens a Writer if diff logging is enabled, returning nil
// otherwise. A nil *Writer is safe to use: Log and Observe are no-ops.
func OpenFromConfig(c config.DiffLog) (*Writer, error) {
	if !c.Enabled() {
		return nil, nil
	}
	return Open(FromConfig(c))
}
