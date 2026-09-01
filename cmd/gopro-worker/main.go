// Command gopro-worker serves the read-only Matrix federation endpoints
// /event, /state and /state_ids.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sort"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/config"
	"github.com/daedric/synapse-gopro-worker/internal/difflog"
	"github.com/daedric/synapse-gopro-worker/internal/fedapi"
	"github.com/daedric/synapse-gopro-worker/internal/fedauth"
	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
	"github.com/daedric/synapse-gopro-worker/internal/metrics"
	"github.com/daedric/synapse-gopro-worker/internal/proxy"
	"github.com/daedric/synapse-gopro-worker/internal/replication"
	"github.com/daedric/synapse-gopro-worker/internal/shadow"
	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// Build information, stamped by the Docker build via -ldflags. The defaults
// apply to a plain "go build".
var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gopro-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "/data/gopro-worker.yaml", "path to the configuration file")
	showVersion := flag.Bool("version", false, "print build information and exit")
	diffStats := flag.String("diffstats", "", "print shadow comparison statistics from the given diff log directory and exit")
	probe := flag.Bool("healthcheck", false, "probe the running worker's /health endpoint and exit; used as the container healthcheck")
	flag.Parse()

	if *diffStats != "" {
		return difflog.ReportToStdout(*diffStats)
	}

	if *showVersion {
		fmt.Printf("gopro-worker %s (commit %s, built %s, %s)\n",
			tag, commit, buildTime, runtime.Version())
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	if *probe {
		return healthcheck(cfg)
	}

	log, err := newLogger(cfg.Log)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p, err := proxy.New(cfg.Upstream, log)
	if err != nil {
		return err
	}

	// A nil Writer is a valid no-op, which is what proxy-only mode gets.
	diffs, err := difflog.OpenFromConfig(cfg.DiffLog)
	if err != nil {
		return err
	}
	defer func() {
		if err := diffs.Close(); err != nil {
			log.Err(err).Msg("Failed to close the diff log")
		}
	}()
	if err := difflog.Register(prometheus.DefaultRegisterer, diffs); err != nil {
		return fmt.Errorf("register diff log metrics: %w", err)
	}

	// Opened whenever configured, even while every endpoint is still proxied,
	// so a broken DSN or an unreachable socket surfaces at startup rather than
	// on the first request served natively.
	db, err := openDatabase(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer db.Close()

	listener, err := listen(cfg.Listen)
	if err != nil {
		return err
	}

	// Endpoints are read from the config rather than listed here. The
	// hand-written version silently omitted get_missing_events the moment it
	// was added, which is the one line an operator reads to confirm what the
	// worker is actually doing.
	startup := log.Info().
		Str("server_name", cfg.ServerName).
		Strs("upstreams", p.Backends())
	for _, name := range slices.Sorted(maps.Keys(cfg.Endpoints.ByName())) {
		startup = startup.Str(name, cfg.Endpoints.ByName()[name].String())
	}
	startup.
		Str("diff_log", cfg.DiffLog.Dir).
		Str("version", tag).
		Msg("Starting gopro-worker")

	if diffs != nil {
		s := diffs.Snapshot()
		log.Info().
			Uint64("compared", s.Compared).
			Uint64("mismatched", s.Mismatched).
			Int("restarts", s.Restarts).
			Time("since", s.Since).
			Msg("Resumed shadow comparison statistics")
	}

	// The resolver and verifier are built once and shared between the shadow
	// comparator and the request path, so a canary answer and the answer we
	// compared in shadow mode come from exactly the same objects -- including
	// the same warm caches and the same key cache.
	var (
		resolver *matrixstate.Resolver
		verifier *fedauth.Verifier
	)
	if db != nil && cfg.NeedsDatabase() {
		resolver = matrixstate.NewResolver(db)
		verifier = fedauth.New(cfg.ServerName, fedauth.Options{
			KeyRefetchDelay: time.Duration(cfg.Auth.KeyRefetchMinutes) * time.Minute,
			Timeout:         time.Duration(cfg.Auth.TimeoutSeconds) * time.Second,
			Notaries:        cfg.Auth.TrustedKeyServers,
			DB:              db,
			Log:             log,
		})
	}

	// Only built when a database is available and something is being compared.
	var runner *shadow.Runner
	if db != nil && cfg.NeedsDatabase() {
		runner = shadow.NewRunner(
			resolver,
			cfg.ServerName,
			verifier,
			diffs,
			log,
			shadow.Options{
				Timeout:     time.Duration(cfg.Shadow.TimeoutSeconds) * time.Second,
				Concurrency: cfg.Shadow.Concurrency,
				VerifyWait:  time.Duration(cfg.Shadow.VerifyWaitSeconds) * time.Second,
			},
		)
		log.Info().
			Int("concurrency", cfg.Shadow.Concurrency).
			Int("timeout_seconds", cfg.Shadow.TimeoutSeconds).
			Msg("Shadow comparison enabled")
	}

	var handlerOpts []fedapi.Option
	if resolver != nil && verifier != nil {
		// Canary and native modes need both. Passing them unconditionally is
		// harmless: without a mode that serves natively, nothing calls them.
		nativeTimeout := time.Duration(cfg.NativeTimeoutSeconds) * time.Second
		if nativeTimeout <= 0 {
			nativeTimeout = 5 * time.Second
		}
		// The verification fetch runs after the response with nobody waiting,
		// so it gets shadow's generous budget rather than the serving one.
		verifyTimeout := time.Duration(cfg.Shadow.TimeoutSeconds) * time.Second
		if verifyTimeout <= 0 {
			verifyTimeout = 30 * time.Second
		}
		handlerOpts = append(handlerOpts, fedapi.WithNative(resolver, verifier, nativeTimeout, verifyTimeout),
			fedapi.WithStreamTimeout(time.Duration(cfg.StreamTimeoutSeconds)*time.Second))
	}
	handler := fedapi.New(cfg, p, runner, log, handlerOpts...)

	// Enable the /state mismatch diagnosis, which needs both sides replayed.
	//
	// Wired here rather than in the Runner's constructor because only this
	// scope holds both a streaming resolver and a proxy, and the Runner is
	// deliberately ignorant of HTTP. Without it a /state mismatch still
	// reports counts and digests; with it, the events are named.
	if runner != nil && resolver != nil {
		runner.SetStateReplayers(
			func(ctx context.Context, req shadow.Request, w io.Writer) error {
				_, err := resolver.State(ctx, w, req.Origin, req.RoomID, req.EventID)
				return err
			},
			func(ctx context.Context, hr *http.Request, w io.Writer) error {
				res := p.ForwardStreaming(proxy.Discard{}, hr, w)
				if res.SinkErr != nil {
					return res.SinkErr
				}
				if res.Err != nil {
					return res.Err
				}
				if res.Status != http.StatusOK {
					// An error body is not an answer, and scanning one would
					// report every event as missing from Synapse's side.
					return fmt.Errorf("upstream answered %d", res.Status)
				}
				return nil
			},
		)
	}

	// Report which endpoints the limiter actually governs. It applies only
	// where we answer, so saying "active" while everything is proxied or
	// shadowed would describe behaviour that is not happening.
	rc := handler.Limiter().Settings()
	var limited []string
	for name, m := range cfg.Endpoints.ByName() {
		if m.ServesNatively() {
			limited = append(limited, name)
		}
	}
	sort.Strings(limited)

	ev := log.Info().
		Int("window_size", rc.WindowSize).
		Int("sleep_limit", rc.SleepLimit).
		Int("sleep_delay", rc.SleepDelay).
		Int("reject_limit", rc.RejectLimit).
		Int("concurrent", rc.Concurrent)
	if len(limited) == 0 {
		ev.Msg("Federation rate limiting configured but inactive: no endpoint serves natively yet, so Synapse's own limiter governs every request")
	} else {
		ev.Strs("endpoints", limited).Msg("Federation rate limiting active")
	}

	metrics.RegisterRateLimitHosts(handler.Limiter().Hosts)

	// Discard state for servers we have not heard from, so a one-off contact
	// does not occupy memory indefinitely.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n := handler.Limiter().Cleanup(time.Hour); n > 0 {
					log.Debug().Int("removed", n).Msg("Cleaned up idle rate limit state")
				}
			}
		}
	}()

	srv := &http.Server{
		Handler: handler,
		// Federation clients are remote servers over the open internet; keep
		// header reads bounded but allow slow large-state responses to drain.
		ReadHeaderTimeout: 20 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          nil,
	}

	metricsSrv := &http.Server{
		Addr:              cfg.Metrics.Addr,
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errs := make(chan error, 2)
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("federation listener: %w", err)
		}
	}()
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- fmt.Errorf("metrics listener: %w", err)
		}
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		log.Info().Msg("Shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	return srv.Shutdown(shutdownCtx)
}

// openDatabase connects to Synapse's database if configured, returning a nil
// Store otherwise. A nil Store is safe to Close.
func openDatabase(ctx context.Context, cfg *config.Config, log zerolog.Logger) (*store.Store, error) {
	if !cfg.Database.Enabled() {
		if cfg.NeedsDatabase() {
			// Validation should have caught this; belt and braces.
			return nil, fmt.Errorf("database.dsn is required for the configured endpoint modes")
		}
		log.Info().Msg("No database configured; serving every endpoint by proxy")
		return nil, nil
	}

	timeout := time.Duration(cfg.Database.ConnectTimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db, err := store.Open(connectCtx, store.Config{
		DSN:            cfg.Database.DSN,
		MaxConns:       int32(cfg.Database.MaxConns),
		ConnectTimeout: timeout,
		Cache:          cfg.Database.Cache,
	})
	if err != nil {
		return nil, err
	}

	cacheSettings := cfg.Database.Cache.WithDefaults()
	log.Info().
		Int("state_groups_mb", cacheSettings.StateGroupsMB).
		Int("events_mb", cacheSettings.EventsMB).
		Int("event_state_groups_mb", cacheSettings.EventStateGroupsMB).
		Int("auth_chains_mb", cacheSettings.AuthChainsMB).
		Msg("Caching immutable data in process")
	metrics.RegisterCaches(db.CacheStats)

	startReplication(ctx, cfg, db, log)

	readOnly, err := db.IsReadOnly(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("verify read-only access: %w", err)
	}
	// Loud, because a writable role means a bug in this worker could damage a
	// production Synapse database.
	if !readOnly {
		log.Warn().Msg("Database role is NOT read-only; see deploy/readonly-role.sql")
	}
	log.Info().Bool("read_only", readOnly).Msg("Connected to Synapse database")
	return db, nil
}

func newLogger(cfg config.Log) (zerolog.Logger, error) {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return zerolog.Nop(), fmt.Errorf("log: %w", err)
	}
	var w = os.Stdout
	logger := zerolog.New(w).Level(level).With().Timestamp().Logger()
	if cfg.Pretty {
		logger = logger.Output(zerolog.ConsoleWriter{
			Out: w,
			// Time only: the date is in the container log's own timestamps and
			// every line here is from the same day as the one above it.
			TimeFormat: "15:04:05",
			// The fields an operator scans for first, in the order they are
			// usually wanted. Anything not listed still prints, after these.
			FieldsOrder: []string{
				"endpoint", "mode", "status", "origin", "room_id", "event_id",
				"total_ms", "upstream_ms", "bytes", "reason", "kind",
			},
		})
	}
	return logger, nil
}

// listen opens the configured listener. For unix sockets it removes a stale
// socket left by an unclean shutdown and applies the configured permissions so
// nginx can connect.
func listen(cfg config.Listen) (net.Listener, error) {
	if cfg.Addr != "" {
		return net.Listen("tcp", cfg.Addr)
	}

	if err := os.Remove(cfg.Socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket %s: %w", cfg.Socket, err)
	}
	l, err := net.Listen("unix", cfg.Socket)
	if err != nil {
		return nil, err
	}
	mode, err := cfg.ParsedSocketMode()
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := os.Chmod(cfg.Socket, mode); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}
	return l, nil
}

// startReplication consumes Synapse's cache-invalidation stream, which is what
// makes caching safe against events being deleted.
//
// When it is not configured the caches still run, but nothing tells them a
// purge has happened — so say so loudly rather than let it pass as a default.
func startReplication(ctx context.Context, cfg *config.Config, db *store.Store, log zerolog.Logger) {
	if !cfg.Replication.Enabled {
		if cfg.Database.Cache.WithDefaults().AnyEnabled() {
			log.Warn().Msg("Caching without replication: nothing will invalidate a cached " +
				"event that Synapse later deletes, until this process restarts")
		}
		return
	}
	sub, err := replication.New(cfg.Replication, db, log)
	if err != nil {
		// Not fatal, but the caches must not run unsupervised.
		db.SetCachesArmed(false)
		log.Error().Err(err).Msg("Replication misconfigured; caches disabled")
		return
	}
	go sub.Run(ctx)
}
