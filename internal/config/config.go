// Package config loads the gopro-worker configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Mode selects how a given endpoint is served.
type Mode struct {
	// Kind is one of "proxy", "shadow", "canary", "native".
	Kind string
	// CanaryPercent is the percentage of traffic served natively when Kind is
	// "canary". Ignored otherwise.
	CanaryPercent int
}

const (
	ModeProxy  = "proxy"
	ModeShadow = "shadow"
	ModeCanary = "canary"
	ModeNative = "native"
)

// UnmarshalYAML accepts either a bare mode name ("proxy") or a canary
// specification ("canary:5").
func (m *Mode) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return err
	}
	return m.parse(raw)
}

func (m *Mode) parse(raw string) error {
	kind, pct, hasPct := strings.Cut(raw, ":")
	kind = strings.TrimSpace(kind)

	switch kind {
	case ModeProxy, ModeShadow, ModeNative:
		if hasPct {
			return fmt.Errorf("mode %q does not take a percentage", kind)
		}
		m.Kind = kind
		return nil
	case ModeCanary:
		if !hasPct {
			return fmt.Errorf("mode %q requires a percentage, e.g. %q", kind, "canary:5")
		}
		n, err := strconv.Atoi(strings.TrimSpace(pct))
		if err != nil {
			return fmt.Errorf("invalid canary percentage %q: %w", pct, err)
		}
		if n < 0 || n > 100 {
			return fmt.Errorf("canary percentage must be 0-100, got %d", n)
		}
		m.Kind, m.CanaryPercent = kind, n
		return nil
	default:
		return fmt.Errorf("unknown mode %q", raw)
	}
}

func (m Mode) String() string {
	if m.Kind == ModeCanary {
		return fmt.Sprintf("canary:%d", m.CanaryPercent)
	}
	return m.Kind
}

// Config is the top-level worker configuration.
type Config struct {
	// ServerName is this homeserver's name, e.g. "example.com". It is the
	// expected destination of incoming X-Matrix authenticated requests.
	ServerName string `yaml:"server_name"`

	Listen    Listen    `yaml:"listen"`
	Upstream  Upstream  `yaml:"upstream"`
	Metrics   Metrics   `yaml:"metrics"`
	Endpoints Endpoints `yaml:"endpoints"`
	Log       Log       `yaml:"log"`
	DiffLog   DiffLog   `yaml:"diff_log"`
	Database  Database  `yaml:"database"`
}

// Database describes read-only access to Synapse's PostgreSQL database.
//
// The worker only reads, and is expected to connect as a role with SELECT only
// and default_transaction_read_only set. See deploy/readonly-role.sql.
type Database struct {
	// DSN is a libpq connection string. Prefer the unix socket, which needs no
	// password and no network path:
	//   host=/var/sockets user=gopro_ro dbname=synapse-db
	// Empty disables database access, which is correct while every endpoint is
	// in proxy mode.
	DSN string `yaml:"dsn"`
	// MaxConns bounds the connection pool. Zero uses pgx's default of the
	// greater of 4 and NumCPU.
	MaxConns int `yaml:"max_conns"`
	// ConnectTimeoutSeconds bounds the initial connection. Zero uses 10.
	ConnectTimeoutSeconds int `yaml:"connect_timeout_seconds"`
}

// Enabled reports whether database access is configured.
func (d Database) Enabled() bool { return d.DSN != "" }

// DiffLog configures where shadow-mode disagreements are recorded.
//
// A shadow run lasts weeks, so the defaults bound disk use: rotated files are
// gzipped and capped in number, and bodies are clipped.
type DiffLog struct {
	// Dir holds diffs.jsonl, its rotated siblings and stats.json. Empty
	// disables diff logging, which is the right setting while every endpoint
	// is still in proxy mode.
	Dir string `yaml:"dir"`
	// MaxFileMB is the size at which the active log rotates. Zero uses 64.
	MaxFileMB int `yaml:"max_file_mb"`
	// MaxFiles is how many rotated files to keep. Zero uses 10.
	MaxFiles int `yaml:"max_files"`
	// Compress gzips rotated files. Defaults to true.
	Compress *bool `yaml:"compress"`
	// QueueSize bounds the handoff to the background writer. Records are
	// dropped rather than blocking a federation request. Zero uses 1024.
	QueueSize int `yaml:"queue_size"`
	// BodyKB caps each retained response body. Zero uses 256. Set to -1 to
	// omit bodies entirely, which keeps room content out of the log.
	BodyKB int `yaml:"body_kb"`
	// ListLimit caps how many event IDs are listed per diff field. Zero uses 100.
	ListLimit int `yaml:"list_limit"`
}

// Enabled reports whether diff logging is configured.
func (d DiffLog) Enabled() bool { return d.Dir != "" }

// CompressRotated reports whether rotated files should be gzipped.
func (d DiffLog) CompressRotated() bool { return d.Compress == nil || *d.Compress }

// Listen describes where the worker accepts requests. Exactly one of Socket or
// Addr must be set.
type Listen struct {
	// Socket is a unix socket path, matching the Synapse worker convention.
	Socket string `yaml:"socket"`
	// Addr is a TCP address such as ":8090".
	Addr string `yaml:"addr"`
	// SocketMode is the permission bits applied to Socket, as an octal string.
	// Defaults to "0660" so the nginx group can connect.
	SocketMode string `yaml:"socket_mode"`
}

// Upstream describes the Synapse federation workers to proxy to.
type Upstream struct {
	// Sockets are unix socket paths of Synapse federation readers. Requests are
	// balanced across them.
	Sockets []string `yaml:"sockets"`
	// Addrs are TCP addresses of Synapse federation readers, used when Sockets
	// is empty.
	Addrs []string `yaml:"addrs"`
	// TimeoutSeconds bounds a single upstream request. Zero means 60s.
	TimeoutSeconds int `yaml:"timeout_seconds"`
	// MaxIdleConns caps idle keep-alive connections per upstream. Zero means 32.
	MaxIdleConns int `yaml:"max_idle_conns"`
}

// Metrics configures the Prometheus listener.
type Metrics struct {
	Addr string `yaml:"addr"`
}

// Endpoints selects the serving mode per federation endpoint.
type Endpoints struct {
	Event    Mode `yaml:"event"`
	State    Mode `yaml:"state"`
	StateIDs Mode `yaml:"state_ids"`
}

// Log configures logging.
type Log struct {
	// Level is one of trace, debug, info, warn, error.
	Level string `yaml:"level"`
	// Pretty enables human-readable console output instead of JSON.
	Pretty bool `yaml:"pretty"`
}

// Load reads and validates the configuration at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parse(data)
}

func parse(data []byte) (*Config, error) {
	cfg := &Config{
		Listen:  Listen{SocketMode: "0660"},
		Metrics: Metrics{Addr: ":9200"},
		Log:     Log{Level: "info"},
		Endpoints: Endpoints{
			Event:    Mode{Kind: ModeProxy},
			State:    Mode{Kind: ModeProxy},
			StateIDs: Mode{Kind: ModeProxy},
		},
	}

	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.ServerName == "" {
		return fmt.Errorf("server_name is required")
	}
	if (c.Listen.Socket == "") == (c.Listen.Addr == "") {
		return fmt.Errorf("listen: exactly one of socket or addr must be set")
	}
	if c.Listen.SocketMode == "" {
		c.Listen.SocketMode = "0660"
	}
	if _, err := c.Listen.ParsedSocketMode(); err != nil {
		return err
	}
	if len(c.Upstream.Sockets) == 0 && len(c.Upstream.Addrs) == 0 {
		return fmt.Errorf("upstream: at least one of sockets or addrs must be set")
	}
	if c.Upstream.TimeoutSeconds < 0 {
		return fmt.Errorf("upstream: timeout_seconds must not be negative")
	}
	if c.Upstream.MaxIdleConns < 0 {
		return fmt.Errorf("upstream: max_idle_conns must not be negative")
	}
	if c.DiffLog.MaxFileMB < 0 || c.DiffLog.MaxFiles < 0 || c.DiffLog.QueueSize < 0 || c.DiffLog.ListLimit < 0 {
		return fmt.Errorf("diff_log: sizes must not be negative")
	}
	if c.DiffLog.BodyKB < -1 {
		return fmt.Errorf("diff_log: body_kb must be -1 (omit), 0 (default) or positive")
	}
	if c.Database.MaxConns < 0 || c.Database.ConnectTimeoutSeconds < 0 {
		return fmt.Errorf("database: values must not be negative")
	}

	for name, m := range c.Endpoints.byName() {
		// Shadow and canary compare against Synapse, and a comparison whose
		// disagreements are not recorded is not worth running.
		if (m.Kind == ModeShadow || m.Kind == ModeCanary) && !c.DiffLog.Enabled() {
			return fmt.Errorf("endpoint %s is in %s mode but diff_log.dir is not set", name, m.Kind)
		}
		// Anything beyond proxy needs to compute an answer of its own.
		if m.Kind != ModeProxy && !c.Database.Enabled() {
			return fmt.Errorf("endpoint %s is in %s mode but database.dsn is not set", name, m.Kind)
		}
	}
	return nil
}

// byName maps each endpoint to its configured mode.
func (e Endpoints) byName() map[string]Mode {
	return map[string]Mode{
		"event":     e.Event,
		"state":     e.State,
		"state_ids": e.StateIDs,
	}
}

// NeedsDatabase reports whether any endpoint is configured to compute its own
// answer.
func (c *Config) NeedsDatabase() bool {
	for _, m := range c.Endpoints.byName() {
		if m.Kind != ModeProxy {
			return true
		}
	}
	return false
}

// ParsedSocketMode returns the listen socket permission bits.
func (l Listen) ParsedSocketMode() (os.FileMode, error) {
	mode := l.SocketMode
	if mode == "" {
		mode = "0660"
	}
	n, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("listen: invalid socket_mode %q: %w", l.SocketMode, err)
	}
	return os.FileMode(n), nil
}
