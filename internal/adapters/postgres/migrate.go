package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/uttamsingh23/tenant-control-plane/migrations"
)

// Migrate applies every pending migration from the migrations package.
// Each migration runs in its own transaction, and a PostgreSQL advisory
// lock serializes concurrent callers, so several instances may start at
// the same time.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// The *sql.DB borrows connections from pool; closing it leaves pool open.
	db := stdlib.OpenDBFromPool(pool)
	defer func() { _ = db.Close() }()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
