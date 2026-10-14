package postgres_test

import (
	"context"
	"io/fs"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/migrations"
)

var schemaTables = []string{"tenants", "tasks", "outbox_events", "processed_updates"}

func migrationCount(t *testing.T) int64 {
	t.Helper()
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no embedded migrations: %v", err)
	}
	return int64(len(files))
}

func gooseProvider(t *testing.T, pool *pgxpool.Pool) *goose.Provider {
	t.Helper()
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func assertTables(t *testing.T, pool *pgxpool.Pool, wantExist bool) {
	t.Helper()
	for _, table := range schemaTables {
		var exists bool
		if err := pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != wantExist {
			t.Errorf("table %s exists = %v, want %v", table, exists, wantExist)
		}
	}
}

func TestMigrateEmptyDatabase(t *testing.T) {
	pool := newEmptyDB(t)
	assertTables(t, pool, false)

	if err := postgres.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	assertTables(t, pool, true)
	version, err := gooseProvider(t, pool).GetDBVersion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := migrationCount(t); version != want {
		t.Errorf("schema version = %d, want %d", version, want)
	}

	// Re-running is a no-op.
	if err := postgres.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestMigrateConcurrentCallers(t *testing.T) {
	pool := newEmptyDB(t)
	errs := parallel(4, func(int) error { return postgres.Migrate(context.Background(), pool) })
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	assertTables(t, pool, true)
}

func TestMigrationsDownAndUpAgain(t *testing.T) {
	pool := newMigratedDB(t)
	p := gooseProvider(t, pool)

	if _, err := p.DownTo(t.Context(), 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	assertTables(t, pool, false)

	if _, err := p.Up(t.Context()); err != nil {
		t.Fatalf("up again: %v", err)
	}
	assertTables(t, pool, true)
}

func TestSessionTimeZoneIsUTC(t *testing.T) {
	pool := newMigratedDB(t)
	var tz string
	if err := pool.QueryRow(t.Context(), `SHOW timezone`).Scan(&tz); err != nil {
		t.Fatal(err)
	}
	if tz != "UTC" {
		t.Errorf("timezone = %q, want UTC", tz)
	}
}
