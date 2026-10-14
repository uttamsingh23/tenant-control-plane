package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

var errBoom = errors.New("boom")

// createInTx writes a tenant, its deploy task and the outbox event in one
// transaction, the way the create use case will, then calls after.
func createInTx(ctx context.Context, tn domain.Tenant, task domain.Task, after func() error) func(context.Context, application.Repositories) error {
	return func(ctx context.Context, r application.Repositories) error {
		if err := r.Tenants().Insert(ctx, tn); err != nil {
			return err
		}
		if err := r.Tasks().Insert(ctx, task); err != nil {
			return err
		}
		if err := r.Outbox().Enqueue(ctx, task); err != nil {
			return err
		}
		return after()
	}
}

func TestWithinTxCommitsTenantTaskAndEvent(t *testing.T) {
	store, pool := newStore(t)
	ctx := t.Context()
	tn := newTenant(t, "acme", base)
	task := newTask(t, domain.TaskTypeDeploy, tn.ID, base)

	if err := store.WithinTx(ctx, createInTx(ctx, tn, task, func() error { return nil })); err != nil {
		t.Fatalf("WithinTx: %v", err)
	}

	if got, err := store.Tenants().Get(ctx, tn.ID); err != nil || got != tn {
		t.Errorf("tenant = %+v, %v; want %+v", got, err, tn)
	}
	if got, err := store.Tasks().Get(ctx, task.ID); err != nil || got != task {
		t.Errorf("task = %+v, %v; want %+v", got, err, task)
	}
	assertPendingEvents(t, pool, task.ID)
}

func TestWithinTxRollsBack(t *testing.T) {
	tests := []struct {
		name      string
		after     func() error
		wantErr   error
		wantPanic bool
	}{
		{name: "error returned", after: func() error { return errBoom }, wantErr: errBoom},
		{name: "panic", after: func() error { panic(errBoom) }, wantPanic: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newStore(t)
			ctx := t.Context()
			tn := newTenant(t, "acme", base)
			task := newTask(t, domain.TaskTypeDeploy, tn.ID, base)

			var err error
			recovered := func() (r any) {
				defer func() { r = recover() }()
				err = store.WithinTx(ctx, createInTx(ctx, tn, task, tt.after))
				return nil
			}()
			if (recovered != nil) != tt.wantPanic {
				t.Fatalf("panic = %v, want panic %v", recovered, tt.wantPanic)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("WithinTx err = %v, want %v", err, tt.wantErr)
			}
			assertNothingPersisted(t, store, tn, task)
		})
	}
}

// TestWithinTxRollsBackOnConstraintViolation fails on the last statement,
// after the tenant and task rows were written.
func TestWithinTxRollsBackOnConstraintViolation(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	tn := newTenant(t, "acme", base)
	task := newTask(t, domain.TaskTypeDeploy, tn.ID, base)

	err := store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
		return createInTx(ctx, tn, task, func() error { return r.Outbox().Enqueue(ctx, task) })(ctx, r)
	})
	pgError(t, err, "23505", "outbox_events_task_id_key")
	assertNothingPersisted(t, store, tn, task)
}

// TestWorkerUpdateIncrementsTenantVersion persists a worker-driven task
// outcome and the resulting tenant change in one transaction.
func TestWorkerUpdateIncrementsTenantVersion(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	tn := insertTenant(t, store, "acme")
	task := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)
	now := base.Add(time.Minute)

	for _, status := range []domain.TaskStatus{domain.TaskStatusInProgress, domain.TaskStatusDone} {
		err := store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
			tk, err := r.Tasks().GetForUpdate(ctx, task.ID)
			if err != nil {
				return err
			}
			if err := tk.TransitionTo(status, now); err != nil {
				return err
			}
			if err := r.Tasks().Update(ctx, tk); err != nil {
				return err
			}
			if !tk.Status.Terminal() {
				return nil
			}
			tenant, err := r.Tenants().Get(ctx, tk.TenantID)
			if err != nil {
				return err
			}
			prev := tenant.Version
			if err := tenant.ApplyTaskOutcome(tk, now); err != nil {
				return err
			}
			return r.Tenants().Update(ctx, tenant, prev)
		})
		if err != nil {
			t.Fatalf("apply %s: %v", status, err)
		}
	}

	got, err := store.Tenants().Get(ctx, tn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.TenantStatusActive || got.Version != tn.Version+1 || got.UpdatedAt != now {
		t.Errorf("tenant = %+v, want active at version %d updated %v", got, tn.Version+1, now)
	}
}

func assertNothingPersisted(t *testing.T, store interface {
	Tenants() application.TenantRepository
	Tasks() application.TaskRepository
	Outbox() application.OutboxRepository
}, tn domain.Tenant, task domain.Task) {
	t.Helper()
	ctx := t.Context()
	if _, err := store.Tenants().Get(ctx, tn.ID); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("tenant after rollback: err = %v, want ErrTenantNotFound", err)
	}
	if _, err := store.Tasks().Get(ctx, task.ID); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("task after rollback: err = %v, want ErrTaskNotFound", err)
	}
	if err := store.Outbox().MarkPublished(ctx, task.ID); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("outbox event after rollback: err = %v, want ErrTaskNotFound", err)
	}
	// The slug was not consumed either.
	if err := store.Tenants().Insert(ctx, newTenant(t, tn.Slug, base)); err != nil {
		t.Errorf("slug %q not reusable after rollback: %v", tn.Slug, err)
	}
}
