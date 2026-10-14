package postgres_test

// Integration-test harness: every test gets its own fresh database from
// pgtest (a testcontainers PostgreSQL or TEST_DATABASE_URL).

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

func newMigratedDB(t *testing.T) *pgxpool.Pool { return pgtest.NewMigratedDB(t) }

func newEmptyDB(t *testing.T) *pgxpool.Pool { return pgtest.NewEmptyDB(t) }

// newStore returns a Store on a fresh migrated database, plus its pool for
// assertions that need raw SQL.
func newStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	pool := newMigratedDB(t)
	return postgres.NewStore(pool), pool
}
