package postgres_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

var base = time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)

func newTenant(t *testing.T, slug string, at time.Time) domain.Tenant {
	t.Helper()
	tn, err := domain.NewTenant(uuid.NewString(), slug, "Tenant "+slug, at)
	if err != nil {
		t.Fatal(err)
	}
	return tn
}

func insertTenant(t *testing.T, store *postgres.Store, slug string) domain.Tenant {
	t.Helper()
	tn := newTenant(t, slug, base)
	if err := store.Tenants().Insert(t.Context(), tn); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	return tn
}

func newTask(t *testing.T, typ domain.TaskType, tenantID string, at time.Time) domain.Task {
	t.Helper()
	task, err := domain.NewTask(uuid.NewString(), typ, tenantID, at)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func insertTask(t *testing.T, store *postgres.Store, typ domain.TaskType, tenantID string) domain.Task {
	t.Helper()
	task := newTask(t, typ, tenantID, base)
	if err := store.Tasks().Insert(t.Context(), task); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	return task
}

// mutate applies a domain mutation to tn and persists it with optimistic
// locking against the version tn had before.
func mutate(t *testing.T, store *postgres.Store, tn *domain.Tenant, fn func(*domain.Tenant) error) {
	t.Helper()
	prev := tn.Version
	if err := fn(tn); err != nil {
		t.Fatalf("domain mutation: %v", err)
	}
	if err := store.Tenants().Update(t.Context(), *tn, prev); err != nil {
		t.Fatalf("update tenant: %v", err)
	}
}

// outcome returns a mutation applying a terminal task outcome.
func outcome(typ domain.TaskType, status domain.TaskStatus) func(*domain.Tenant) error {
	return func(tn *domain.Tenant) error {
		task := domain.Task{ID: uuid.NewString(), Type: typ, TenantID: tn.ID, Status: status}
		return tn.ApplyTaskOutcome(task, tn.UpdatedAt.Add(time.Second))
	}
}

func requestDestroy(tn *domain.Tenant) error { return tn.RequestDestroy(tn.UpdatedAt.Add(time.Second)) }

// pgError asserts err carries a PostgreSQL error with the given SQLSTATE
// and constraint name.
func pgError(t *testing.T, err error, code, constraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("err = %v, want *pgconn.PgError", err)
	}
	if pgErr.Code != code || pgErr.ConstraintName != constraint {
		t.Errorf("got SQLSTATE %s constraint %q, want %s %q (%s)", pgErr.Code, pgErr.ConstraintName, code, constraint, pgErr.Message)
	}
}

// parallel runs fn n times concurrently, released together, and returns
// the errors in index order.
func parallel(n int, fn func(i int) error) []error {
	errs := make([]error, n)
	start := make(chan struct{})
	done := make(chan struct{})
	for i := range n {
		go func() {
			defer func() { done <- struct{}{} }()
			<-start
			errs[i] = fn(i)
		}()
	}
	close(start)
	for range n {
		<-done
	}
	return errs
}
