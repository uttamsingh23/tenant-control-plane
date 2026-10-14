// Package postgres implements the application repositories on PostgreSQL
// using pgx.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
)

// querier is the subset of pgx shared by *pgxpool.Pool and pgx.Tx, so the
// same repository code runs inside or outside a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewPool parses dsn and creates a connection pool. Connections are opened
// lazily, so this does not fail when the database is unreachable.
//
// Every session runs with TimeZone=UTC so that timestamps rendered or
// truncated by the server (now(), date_trunc) are in UTC as well.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	return pool, nil
}

// Store provides the repositories and transactions backed by a pool.
type Store struct {
	pool *pgxpool.Pool
	repositories
}

var (
	_ application.Transactor   = (*Store)(nil)
	_ application.Repositories = (*Store)(nil)
)

// NewStore returns a Store whose repositories run on pool outside any
// explicit transaction.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, repositories: repositories{q: pool}}
}

// WithinTx implements application.Transactor. It commits if fn returns nil
// and rolls back if fn returns an error or panics; fn's error is returned
// unchanged so callers can match domain sentinels.
//
// Transactions use READ COMMITTED. Concurrency correctness does not depend
// on a stricter level: tenant writes are conditional on the version, slug
// uniqueness and the one-open-task rule are constraints, and task updates
// take a row lock through GetForUpdate.
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context, repos application.Repositories) error) error {
	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return fn(ctx, repositories{q: tx})
	})
}

// repositories binds every repository to one querier.
type repositories struct {
	q querier
}

func (r repositories) Tenants() application.TenantRepository { return tenantRepository(r) }
func (r repositories) Tasks() application.TaskRepository     { return taskRepository(r) }
func (r repositories) Outbox() application.OutboxRepository  { return outboxRepository(r) }
func (r repositories) ProcessedUpdates() application.ProcessedUpdateRepository {
	return processedUpdateRepository(r)
}
