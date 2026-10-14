package postgres_test

// Integration tests of the application use cases on PostgreSQL: the
// transactional and concurrency guarantees that an in-memory fake cannot
// demonstrate.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

type services struct {
	pool    *pgxpool.Pool
	tenants *application.TenantService
	tasks   *application.TaskService
}

func newServices(t *testing.T) services {
	t.Helper()
	store, pool := newStore(t)
	return servicesWith(pool, store, store)
}

// servicesWith wires the services with tx for writes and store for reads.
func servicesWith(pool *pgxpool.Pool, tx application.Transactor, store *postgres.Store) services {
	return services{
		pool:    pool,
		tenants: application.NewTenantService(tx, store, uuid.NewString, time.Now),
		tasks:   application.NewTaskService(tx, store, time.Now),
	}
}

// failingOutboxTx runs real transactions whose outbox writes fail after
// the tenant and task rows were written, to prove the rollback is real.
type failingOutboxTx struct{ store *postgres.Store }

func (f *failingOutboxTx) WithinTx(ctx context.Context, fn func(context.Context, application.Repositories) error) error {
	return f.store.WithinTx(ctx, func(ctx context.Context, repos application.Repositories) error {
		return fn(ctx, failingOutboxRepos{repos})
	})
}

type failingOutboxRepos struct{ application.Repositories }

func (r failingOutboxRepos) Outbox() application.OutboxRepository { return failingOutbox{} }

type failingOutbox struct{ application.OutboxRepository }

var errOutboxDown = errors.New("outbox unavailable")

func (failingOutbox) Enqueue(context.Context, domain.Task) error { return errOutboxDown }

// counts returns the number of tenants, tasks and outbox events.
func (s services) counts(t *testing.T) [3]int {
	t.Helper()
	var c [3]int
	err := s.pool.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM tenants), (SELECT count(*) FROM tasks), (SELECT count(*) FROM outbox_events)`,
	).Scan(&c[0], &c[1], &c[2])
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// workerUpdate is the update a worker sends to report status for task,
// with a fresh idempotency key.
func workerUpdate(task domain.Task, status domain.TaskStatus) domain.TaskUpdate {
	previous := domain.TaskStatusInProgress
	if status == domain.TaskStatusInProgress {
		previous = domain.TaskStatusAccepted
	}
	return domain.TaskUpdate{
		ID: uuid.NewString(), TaskID: task.ID, TenantID: task.TenantID, TaskType: task.Type,
		PreviousStatus: previous, Status: status, OccurredAt: time.Now(),
	}
}

// complete drives task through in_progress to status, as the worker would.
func (s services) complete(t *testing.T, taskID string, status domain.TaskStatus) {
	t.Helper()
	task, err := s.tasks.Get(t.Context(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []domain.TaskStatus{domain.TaskStatusInProgress, status} {
		got, err := s.tasks.ApplyUpdate(t.Context(), workerUpdate(task, st))
		if err != nil || got != application.UpdateApplied {
			t.Fatalf("apply %s: %s, %v", st, got, err)
		}
	}
}

func (s services) createActive(t *testing.T, slug string) domain.Tenant {
	t.Helper()
	m, err := s.tenants.Create(t.Context(), application.CreateTenantInput{Slug: slug, Name: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	s.complete(t, m.Task.ID, domain.TaskStatusDone)
	tn, err := s.tenants.Get(t.Context(), m.Tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	return tn
}

func TestAppLifecycle(t *testing.T) {
	s := newServices(t)
	ctx := t.Context()

	created, err := s.tenants.Create(ctx, application.CreateTenantInput{Slug: "acme", Name: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	s.complete(t, created.Task.ID, domain.TaskStatusDone)
	active, _ := s.tenants.Get(ctx, created.Tenant.ID)
	if active.Status != domain.TenantStatusActive || active.Version != 2 {
		t.Fatalf("after deploy: %s v%d, want active v2", active.Status, active.Version)
	}

	updated, err := s.tenants.Update(ctx, active.ID, application.UpdateTenantInput{Name: ptr("Acme Corp"), ExpectedVersion: active.Version})
	if err != nil {
		t.Fatal(err)
	}
	s.complete(t, updated.Task.ID, domain.TaskStatusFailed)

	deleted, err := s.tenants.Delete(ctx, active.ID)
	if err != nil {
		t.Fatalf("delete failed tenant: %v", err)
	}
	s.complete(t, deleted.Task.ID, domain.TaskStatusDone)

	final, _ := s.tenants.Get(ctx, active.ID)
	if final.Status != domain.TenantStatusDestroyed || final.Name != "Acme Corp" || final.Version != 6 {
		t.Errorf("final = %s %q v%d, want destroyed \"Acme Corp\" v6", final.Status, final.Name, final.Version)
	}
	page, err := s.tasks.List(ctx, application.TaskFilter{TenantID: active.ID}, application.PageRequest{})
	if err != nil || len(page.Items) != 3 {
		t.Fatalf("tasks = %d, %v; want 3", len(page.Items), err)
	}
	for _, task := range page.Items {
		if !task.Status.Terminal() {
			t.Errorf("task %s %s left open", task.Type, task.Status)
		}
	}
	if c := s.counts(t); c != [3]int{1, 3, 3} {
		t.Errorf("tenants, tasks, events = %v, want [1 3 3]", c)
	}

	// The slug stays taken after destruction.
	if _, err := s.tenants.Create(ctx, application.CreateTenantInput{Slug: "acme", Name: "Again"}); !errors.Is(err, domain.ErrTenantAlreadyExists) {
		t.Errorf("reuse destroyed slug: err = %v, want ErrTenantAlreadyExists", err)
	}
}

func TestAppConcurrentCreatesSameSlug(t *testing.T) {
	s := newServices(t)
	errs := parallel(16, func(int) error {
		_, err := s.tenants.Create(t.Context(), application.CreateTenantInput{Slug: "race", Name: "Race"})
		return err
	})
	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, domain.ErrTenantAlreadyExists):
			t.Errorf("loser err = %v, want ErrTenantAlreadyExists", err)
		}
	}
	if c := s.counts(t); wins != 1 || c != [3]int{1, 1, 1} {
		t.Errorf("wins = %d, tenants/tasks/events = %v; want 1 and [1 1 1]", wins, c)
	}
}

// All PATCHes read the tenant at the same version before any of them
// writes; the conditional update lets exactly one commit.
func TestAppConcurrentUpdatesSameVersion(t *testing.T) {
	s := newServices(t)
	tn := s.createActive(t, "acme")

	errs := parallel(16, func(i int) error {
		_, err := s.tenants.Update(t.Context(), tn.ID, application.UpdateTenantInput{Name: ptr(fmt.Sprintf("Name %d", i)), ExpectedVersion: tn.Version})
		return err
	})
	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, domain.ErrTenantVersionConflict):
			// Losers that read before the winner committed fail at the
			// conditional write; later ones at the version check, which
			// precedes the status check.
			t.Errorf("loser err = %v, want ErrTenantVersionConflict", err)
		}
	}
	got, _ := s.tenants.Get(t.Context(), tn.ID)
	if wins != 1 || got.Version != tn.Version+1 || got.Status != domain.TenantStatusUpdating {
		t.Errorf("wins = %d, tenant %s v%d; want 1 winner, updating v%d", wins, got.Status, got.Version, tn.Version+1)
	}
	if c := s.counts(t); c != [3]int{1, 2, 2} {
		t.Errorf("tenants, tasks, events = %v, want [1 2 2] (deploy + one update)", c)
	}
}

// PATCH and DELETE racing on one active tenant: exactly one wins.
func TestAppConcurrentUpdateAndDelete(t *testing.T) {
	s := newServices(t)
	tn := s.createActive(t, "acme")

	errs := parallel(2, func(i int) error {
		var err error
		if i == 0 {
			_, err = s.tenants.Update(t.Context(), tn.ID, application.UpdateTenantInput{Name: ptr("X"), ExpectedVersion: tn.Version})
		} else {
			_, err = s.tenants.Delete(t.Context(), tn.ID)
		}
		return err
	})
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("errs = %v, want exactly one success", errs)
	}
	if c := s.counts(t); c != [3]int{1, 2, 2} {
		t.Errorf("tenants, tasks, events = %v, want [1 2 2]", c)
	}
}

// A failure after the tenant and task rows are written rolls them back in
// PostgreSQL: no tenant without its task, no task without its event.
func TestAppMutationsRollBack(t *testing.T) {
	store, pool := newStore(t)
	s := servicesWith(pool, &failingOutboxTx{store}, store)

	if _, err := s.tenants.Create(t.Context(), application.CreateTenantInput{Slug: "acme", Name: "Acme"}); !errors.Is(err, errOutboxDown) {
		t.Fatalf("create err = %v, want outbox failure", err)
	}
	if c := s.counts(t); c != [3]int{} {
		t.Fatalf("after failed create: tenants, tasks, events = %v, want none", c)
	}

	// Seed an active tenant through the real store, then fail its PATCH.
	tn := servicesWith(pool, store, store).createActive(t, "acme")
	before := s.counts(t)

	if _, err := s.tenants.Update(t.Context(), tn.ID, application.UpdateTenantInput{Name: ptr("X"), ExpectedVersion: tn.Version}); !errors.Is(err, errOutboxDown) {
		t.Fatalf("update err = %v, want outbox failure", err)
	}
	if _, err := s.tenants.Delete(t.Context(), tn.ID); !errors.Is(err, errOutboxDown) {
		t.Fatalf("delete err = %v, want outbox failure", err)
	}
	got, _ := s.tenants.Get(t.Context(), tn.ID)
	if got != tn || s.counts(t) != before {
		t.Errorf("tenant %+v, counts %v; want unchanged %+v, %v", got, s.counts(t), tn, before)
	}
}

// The same update delivered concurrently many times is applied once.
func TestAppConcurrentDuplicateUpdates(t *testing.T) {
	s := newServices(t)
	m, err := s.tenants.Create(t.Context(), application.CreateTenantInput{Slug: "acme", Name: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	update := workerUpdate(m.Task, domain.TaskStatusDone)

	outcomes := make([]application.UpdateOutcome, 16)
	errs := parallel(len(outcomes), func(i int) error {
		var err error
		outcomes[i], err = s.tasks.ApplyUpdate(t.Context(), update)
		return err
	})
	applied := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
		if outcomes[i] == application.UpdateApplied {
			applied++
		} else if outcomes[i] != application.UpdateDuplicate {
			t.Errorf("delivery %d = %s, want duplicate", i, outcomes[i])
		}
	}
	got, _ := s.tenants.Get(t.Context(), m.Tenant.ID)
	if applied != 1 || got.Status != domain.TenantStatusActive || got.Version != 2 {
		t.Errorf("applied = %d, tenant %s v%d; want 1, active v2", applied, got.Status, got.Version)
	}
}

// Different updates for one task racing: the row lock serializes them, so
// the terminal outcome is applied exactly once whatever the order.
func TestAppConcurrentDistinctUpdates(t *testing.T) {
	s := newServices(t)
	m, err := s.tenants.Create(t.Context(), application.CreateTenantInput{Slug: "acme", Name: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	statuses := []domain.TaskStatus{domain.TaskStatusInProgress, domain.TaskStatusDone, domain.TaskStatusDone, domain.TaskStatusFailed}
	errs := parallel(len(statuses), func(i int) error {
		_, err := s.tasks.ApplyUpdate(t.Context(), workerUpdate(m.Task, statuses[i]))
		return err
	})
	for i, err := range errs {
		if err != nil {
			t.Errorf("update %d: %v", i, err)
		}
	}
	task, _ := s.tasks.Get(t.Context(), m.Task.ID)
	tn, _ := s.tenants.Get(t.Context(), m.Tenant.ID)
	want := map[domain.TaskStatus]domain.TenantStatus{domain.TaskStatusDone: domain.TenantStatusActive, domain.TaskStatusFailed: domain.TenantStatusFailed}
	if !task.Status.Terminal() || tn.Status != want[task.Status] || tn.Version != 2 {
		t.Errorf("task %s, tenant %s v%d; want terminal task, matching tenant at v2", task.Status, tn.Status, tn.Version)
	}
}

// A worker outcome the tenant does not accept rolls back the task change
// and the idempotency key, in PostgreSQL.
func TestAppRejectedUpdateRollsBack(t *testing.T) {
	s := newServices(t)
	m, err := s.tenants.Create(t.Context(), application.CreateTenantInput{Slug: "acme", Name: "Acme"})
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the tenant so it no longer expects a deploy outcome.
	if _, err := s.pool.Exec(t.Context(), `UPDATE tenants SET status = 'active', version = version + 1 WHERE id = $1`, m.Tenant.ID); err != nil {
		t.Fatal(err)
	}
	update := workerUpdate(m.Task, domain.TaskStatusDone)

	if _, err := s.tasks.ApplyUpdate(t.Context(), update); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
	task, _ := s.tasks.Get(t.Context(), m.Task.ID)
	tn, _ := s.tenants.Get(t.Context(), m.Tenant.ID)
	var keys int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM processed_updates`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if task.Status != domain.TaskStatusAccepted || tn.Version != 2 || keys != 0 {
		t.Errorf("task %s, tenant v%d, keys %d; want accepted, v2, 0", task.Status, tn.Version, keys)
	}
}

// Raw database errors never surface as contract errors: an unknown or
// malformed ID is a not-found, not a SQL type error.
func TestAppMalformedIDsAreNotFound(t *testing.T) {
	s := newServices(t)
	ctx := t.Context()
	if _, err := s.tenants.Get(ctx, "not-a-uuid"); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("get tenant: %v", err)
	}
	if _, err := s.tenants.Update(ctx, "not-a-uuid", application.UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 1}); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("update tenant: %v", err)
	}
	if _, err := s.tenants.Delete(ctx, uuid.NewString()); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("delete tenant: %v", err)
	}
	if _, err := s.tasks.Get(ctx, "not-a-uuid"); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("get task: %v", err)
	}
	malformed := workerUpdate(domain.Task{ID: "not-a-uuid", TenantID: uuid.NewString(), Type: domain.TaskTypeDeploy}, domain.TaskStatusDone)
	if _, err := s.tasks.ApplyUpdate(ctx, malformed); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("apply update: %v", err)
	}
}

func ptr[T any](v T) *T { return &v }
