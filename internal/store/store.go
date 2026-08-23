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
)

// Store holds a pool of read-only connections to Synapse's database.
type Store struct {
	pool *pgxpool.Pool
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
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for tests and metrics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
