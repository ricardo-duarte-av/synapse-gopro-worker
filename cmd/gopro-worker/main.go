// Command gopro-worker serves the read-only Matrix federation endpoints
// /event, /state and /state_ids.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
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

	log.Info().
		Str("server_name", cfg.ServerName).
		Strs("upstreams", p.Backends()).
		Str("event", cfg.Endpoints.Event.String()).
		Str("state", cfg.Endpoints.State.String()).
		Str("state_ids", cfg.Endpoints.StateIDs.String()).
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

	// Only built when a database is available and something is being compared.
	var runner *shadow.Runner
	if db != nil && cfg.NeedsDatabase() {
		runner = shadow.NewRunner(
			matrixstate.NewResolver(db),
			cfg.ServerName,
			fedauth.New(cfg.ServerName, fedauth.Options{
				KeyRefetchDelay: time.Duration(cfg.Auth.KeyRefetchMinutes) * time.Minute,
				Timeout:         time.Duration(cfg.Auth.TimeoutSeconds) * time.Second,
				Notaries:        cfg.Auth.TrustedKeyServers,
				DB:              db,
				Log:             log,
			}),
			diffs,
			log,
			shadow.Options{
				Timeout:     time.Duration(cfg.Shadow.TimeoutSeconds) * time.Second,
				Concurrency: cfg.Shadow.Concurrency,
			},
		)
		log.Info().
			Int("concurrency", cfg.Shadow.Concurrency).
			Int("timeout_seconds", cfg.Shadow.TimeoutSeconds).
			Msg("Shadow comparison enabled")
	}

	handler := fedapi.New(cfg, p, runner, log)

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
		logger = logger.Output(zerolog.ConsoleWriter{Out: w, TimeFormat: time.RFC3339})
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
