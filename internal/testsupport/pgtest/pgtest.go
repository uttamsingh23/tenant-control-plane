// Package pgtest provides PostgreSQL databases for integration tests.
//
// PostgreSQL comes from one of two places:
//
//  1. TEST_DATABASE_URL, if set: an existing server (for example the one
//     started by `make up`). The URL's user needs CREATEDB.
//  2. Otherwise a disposable postgres:17-alpine container started with
//     testcontainers-go on first use and removed by Main.
//
// Tests skip under -short, and when neither source is available (no
// TEST_DATABASE_URL and no reachable Docker daemon).
//
// The schema is migrated once into a template database; every test then
// gets its own database cloned from it (CREATE DATABASE ... TEMPLATE),
// which is fast and fully isolates tests from each other.
//
// A test package using pgtest calls Main (or Teardown) from its TestMain.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
)

const postgresImage = "postgres:17-alpine"

var harness struct {
	once      sync.Once
	err       error
	adminURL  string // connects to the maintenance database "postgres"
	template  string
	container *tcpostgres.PostgresContainer

	createMu sync.Mutex // CREATE DATABASE from one template is serialized
	counter  atomic.Int64
}

// Main runs the tests of a package and removes the database it started.
// Call it from TestMain: func TestMain(m *testing.M) { pgtest.Main(m) }.
func Main(m *testing.M) {
	code := m.Run()
	Teardown()
	os.Exit(code)
}

// adminURL returns the maintenance connection URL, starting the database
// on first use, or skips the test when no database is available.
func adminURL(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	external := os.Getenv("TEST_DATABASE_URL")
	if external == "" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
	}
	harness.once.Do(func() { harness.err = setupHarness(external) })
	if harness.err != nil {
		t.Fatalf("set up test database: %v", harness.err)
	}
	return harness.adminURL
}

func setupHarness(external string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	harness.adminURL = external
	if external == "" {
		c, err := tcpostgres.Run(ctx, postgresImage,
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("test"),
			tcpostgres.WithPassword("test"),
			tcpostgres.BasicWaitStrategies(),
		)
		harness.container = c // set even on error so teardown removes it
		if err != nil {
			return fmt.Errorf("start %s container: %w", postgresImage, err)
		}
		if harness.adminURL, err = c.ConnectionString(ctx, "sslmode=disable"); err != nil {
			return err
		}
	}

	harness.template = fmt.Sprintf("cp_template_%d", os.Getpid())
	if err := createDatabase(ctx, harness.template, ""); err != nil {
		return err
	}
	pool, err := postgres.NewPool(ctx, databaseURL(harness.template))
	if err != nil {
		return err
	}
	// A template must have no open connections when it is cloned.
	defer pool.Close()
	return postgres.Migrate(ctx, pool)
}

// Teardown removes the database server or template the harness created.
// Main calls it; a package that also uses another harness calls it from
// its own TestMain.
func Teardown() {
	if harness.container != nil {
		_ = testcontainers.TerminateContainer(harness.container)
		return
	}
	if harness.template != "" { // external server: leave it clean
		_ = dropDatabase(context.Background(), harness.template)
	}
}

// NewMigratedDB returns a pool on a fresh database holding the full schema.
func NewMigratedDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	adminURL(t)
	return newDatabase(t, harness.template)
}

// NewEmptyDB returns a pool on a fresh database with no schema.
func NewEmptyDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	adminURL(t)
	return newDatabase(t, "")
}

func newDatabase(t *testing.T, template string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("cp_test_%d_%d", os.Getpid(), harness.counter.Add(1))
	if err := createDatabase(ctx, name, template); err != nil {
		t.Fatal(err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL(name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := dropDatabase(ctx, name); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})
	return pool
}

func createDatabase(ctx context.Context, name, template string) error {
	harness.createMu.Lock()
	defer harness.createMu.Unlock()
	sql := "CREATE DATABASE " + pgx.Identifier{name}.Sanitize()
	if template != "" {
		sql += " TEMPLATE " + pgx.Identifier{template}.Sanitize()
	}
	return adminExec(ctx, sql)
}

func dropDatabase(ctx context.Context, name string) error {
	return adminExec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
}

func adminExec(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, harness.adminURL)
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("%s: %w", sql, err)
	}
	return nil
}

// databaseURL points the admin URL at another database on the same server.
func databaseURL(name string) string {
	u, err := url.Parse(harness.adminURL)
	if err != nil {
		panic(fmt.Sprintf("TEST_DATABASE_URL must be a postgres:// URL: %v", err))
	}
	u.Path = "/" + name
	return u.String()
}
