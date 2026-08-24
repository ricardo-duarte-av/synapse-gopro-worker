// Package store provides read-only access to Synapse's PostgreSQL database.
//
// Every query here is a SELECT. The worker is expected to run as a role with
// only SELECT granted and default_transaction_read_only set, so a bug cannot
// write even if it tries.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// Store holds a pool of read-only connections to Synapse's database, with
// in-process caches for the data that cannot change.
type Store struct {
	pool   *pgxpool.Pool
	caches *caches
}

// Config describes how to reach the database.
type Config struct {
	// DSN is a libpq connection string. For a unix socket, set host to the
	// directory containing .s.PGSQL.5432, e.g.
	// "host=/var/sockets user=gopro_ro dbname=synapse-db".
	DSN string
	// MaxConns bounds the pool. Zero uses pgx's default.
	MaxConns int32
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout time.Duration
	// Cache bounds the in-process caches. Zero values take defaults.
	Cache cache.Settings
}

// Open connects and verifies the database is reachable.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}

	// Synapse's database may sit behind pgcat in transaction pooling mode,
	// where server-side prepared statements cannot be reused across
	// transactions. Describing statements on each exec keeps us compatible
	// with both a direct connection and a transaction-mode pooler.
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec

	if cfg.ConnectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool, caches: newCaches(cfg.Cache)}, nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for tests and metrics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// IsReadOnly reports whether the connected role is restricted to reads.
//
// The worker is only ever meant to read a production Synapse database, so this
// is checked at startup and reported rather than assumed.
func (s *Store) IsReadOnly(ctx context.Context) (bool, error) {
	var setting string
	if err := s.pool.QueryRow(ctx, `SHOW default_transaction_read_only`).Scan(&setting); err != nil {
		return false, fmt.Errorf("store: check read-only: %w", err)
	}
	return setting == "on", nil
}
