package config

import (
	"strings"
	"testing"
)

const minimal = `
server_name: example.com
listen:
  socket: /var/sockets/nginx/gopro.sock
upstream:
  sockets:
    - /var/sockets/nginx/fed-1.sock
`

func TestParseMinimalAppliesDefaults(t *testing.T) {
	cfg, err := parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "example.com" {
		t.Errorf("ServerName = %q", cfg.ServerName)
	}
	if cfg.Metrics.Addr != ":9200" {
		t.Errorf("Metrics.Addr = %q, want the default :9200", cfg.Metrics.Addr)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q, want the default info", cfg.Log.Level)
	}
	// Proxy is the safe default: a fresh deployment must not serve natively.
	for name, m := range map[string]Mode{
		"event":     cfg.Endpoints.Event,
		"state":     cfg.Endpoints.State,
		"state_ids": cfg.Endpoints.StateIDs,
	} {
		if m.Kind != ModeProxy {
			t.Errorf("endpoint %s defaulted to %q, want proxy", name, m.Kind)
		}
	}
	mode, err := cfg.Listen.ParsedSocketMode()
	if err != nil {
		t.Fatal(err)
	}
	if mode != 0o660 {
		t.Errorf("socket mode = %o, want 660", mode)
	}
}

func TestParseModes(t *testing.T) {
	cfg, err := parse([]byte(minimal + `
diff_log:
  dir: /data/diffs
database:
  dsn: "host=/var/sockets user=gopro_ro dbname=synapse-db"
endpoints:
  event: proxy
  state: shadow
  state_ids: "canary:25"
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoints.State.Kind != ModeShadow {
		t.Errorf("state = %v, want shadow", cfg.Endpoints.State)
	}
	if got := cfg.Endpoints.StateIDs; got.Kind != ModeCanary || got.CanaryPercent != 25 {
		t.Errorf("state_ids = %+v, want canary:25", got)
	}
	if got := cfg.Endpoints.StateIDs.String(); got != "canary:25" {
		t.Errorf("String() = %q, want canary:25", got)
	}
}

func TestDiffLogDefaults(t *testing.T) {
	cfg, err := parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DiffLog.Enabled() {
		t.Error("diff logging should be off by default")
	}
	// Compression defaults on: a shadow run spans weeks and rotated JSON
	// compresses heavily.
	if !cfg.DiffLog.CompressRotated() {
		t.Error("CompressRotated() = false, want true by default")
	}

	off := false
	cfg2, err := parse([]byte(minimal + "diff_log:\n  dir: /data/diffs\n  compress: false\n  body_kb: -1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg2.DiffLog.Enabled() {
		t.Error("Enabled() = false, want true when dir is set")
	}
	if cfg2.DiffLog.CompressRotated() != off {
		t.Error("compress: false was not honoured")
	}
	if cfg2.DiffLog.BodyKB != -1 {
		t.Errorf("BodyKB = %d, want -1", cfg2.DiffLog.BodyKB)
	}
}

func TestNeedsDatabase(t *testing.T) {
	cfg, err := parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NeedsDatabase() {
		t.Error("all-proxy config should not need a database")
	}
	if cfg.Database.Enabled() {
		t.Error("database should be disabled by default")
	}

	cfg2, err := parse([]byte(minimal + `diff_log:
  dir: /d
database:
  dsn: "host=/var/sockets user=gopro_ro dbname=synapse-db"
endpoints:
  state_ids: shadow
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg2.NeedsDatabase() {
		t.Error("a shadow endpoint should need a database")
	}
	if !cfg2.Database.Enabled() {
		t.Error("Enabled() = false, want true when dsn is set")
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"missing server_name", "listen:\n  addr: :1\nupstream:\n  addrs: [a:1]\n", "server_name"},
		{"no listener", "server_name: e.com\nupstream:\n  addrs: [a:1]\n", "exactly one"},
		{"both listeners", "server_name: e.com\nlisten:\n  addr: :1\n  socket: /s\nupstream:\n  addrs: [a:1]\n", "exactly one"},
		{"no upstream", "server_name: e.com\nlisten:\n  addr: :1\n", "at least one"},
		{"unknown mode", minimal + "endpoints:\n  state: turbo\n", "unknown mode"},
		{"canary without percent", minimal + "endpoints:\n  state: canary\n", "requires a percentage"},
		{"canary out of range", minimal + "endpoints:\n  state: \"canary:150\"\n", "0-100"},
		{"percent on non-canary", minimal + "endpoints:\n  state: \"shadow:5\"\n", "does not take a percentage"},
		{"unknown field", minimal + "nonsense: 1\n", "field nonsense not found"},
		{"bad socket mode", "server_name: e.com\nlisten:\n  addr: :1\n  socket_mode: \"99z\"\nupstream:\n  addrs: [a:1]\n", "invalid socket_mode"},
		{"negative timeout", "server_name: e.com\nlisten:\n  addr: :1\nupstream:\n  addrs: [a:1]\n  timeout_seconds: -1\n", "must not be negative"},
		// A comparison whose disagreements go unrecorded is not worth running,
		// so shadow and canary modes require a diff log.
		{"shadow without a diff log", minimal + "endpoints:\n  state: shadow\n", "diff_log.dir is not set"},
		{"canary without a diff log", minimal + "endpoints:\n  event: \"canary:5\"\n", "diff_log.dir is not set"},
		// Anything past proxy has to compute an answer, which needs the database.
		{"shadow without a database", minimal + "diff_log:\n  dir: /d\nendpoints:\n  state: shadow\n", "database.dsn is not set"},
		{"native without a database", minimal + "endpoints:\n  state: native\n", "database.dsn is not set"},
		{"negative max_conns", minimal + "database:\n  dsn: x\n  max_conns: -1\n", "must not be negative"},
		{"negative diff log size", minimal + "diff_log:\n  dir: /d\n  max_files: -1\n", "must not be negative"},
		{"invalid body_kb", minimal + "diff_log:\n  dir: /d\n  body_kb: -5\n", "body_kb must be"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("parse(%q) succeeded, want an error", tc.yaml)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}
