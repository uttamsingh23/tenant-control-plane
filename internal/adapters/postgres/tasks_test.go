package postgres_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

func TestTaskInsertGetRoundTrip(t *testing.T) {
	store, _ := newStore(t)
	tn := insertTenant(t, store, "acme")
	want := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)

	for name, get := range map[string]func(context.Context, string) (domain.Task, error){
		"Get":          store.Tasks().Get,
		"GetForUpdate": store.Tasks().GetForUpdate,
	} {
		got, err := get(t.Context(), want.ID)
		if err != nil || got != want {
			t.Errorf("%s = %+v, %v; want %+v", name, got, err, want)
		}
		if _, err := get(t.Context(), uuid.NewString()); !errors.Is(err, domain.ErrTaskNotFound) {
			t.Errorf("%s(unknown) err = %v, want ErrTaskNotFound", name, err)
		}
		if _, err := get(t.Context(), "not-a-uuid"); !errors.Is(err, domain.ErrTaskNotFound) {
			t.Errorf("%s(malformed) err = %v, want ErrTaskNotFound", name, err)
		}
	}
}

func TestTaskInsertUnknownTenant(t *testing.T) {
	store, _ := newStore(t)
	err := store.Tasks().Insert(t.Context(), newTask(t, domain.TaskTypeDeploy, uuid.NewString(), base))
	if !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("err = %v, want ErrTenantNotFound", err)
	}
}

func TestTaskOneOpenPerTenant(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	tn := insertTenant(t, store, "acme")
	first := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)
	other := insertTenant(t, store, "other")

	for _, status := range []domain.TaskStatus{domain.TaskStatusAccepted, domain.TaskStatusInProgress} {
		if status != first.Status {
			if err := first.TransitionTo(status, base); err != nil {
				t.Fatal(err)
			}
			if err := store.Tasks().Update(ctx, first); err != nil {
				t.Fatal(err)
			}
		}
		err := store.Tasks().Insert(ctx, newTask(t, domain.TaskTypeUpdate, tn.ID, base))
		if !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
			t.Errorf("second task while first is %s: err = %v, want ErrTenantUpdateNotAllowed", status, err)
		}
	}

	// Another tenant is unaffected.
	insertTask(t, store, domain.TaskTypeDeploy, other.ID)

	// Once the first task is terminal a new one may be opened.
	if err := first.TransitionTo(domain.TaskStatusDone, base); err != nil {
		t.Fatal(err)
	}
	if err := store.Tasks().Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	insertTask(t, store, domain.TaskTypeUpdate, tn.ID)
}

func TestTaskUpdate(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	tn := insertTenant(t, store, "acme")
	task := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)

	if err := task.TransitionTo(domain.TaskStatusInProgress, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.Tasks().Update(ctx, task); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Tasks().Get(ctx, task.ID); err != nil || got != task {
		t.Errorf("got %+v, %v; want %+v", got, err, task)
	}

	for _, id := range []string{uuid.NewString(), "not-a-uuid"} {
		missing := task
		missing.ID = id
		if err := store.Tasks().Update(ctx, missing); !errors.Is(err, domain.ErrTaskNotFound) {
			t.Errorf("Update(id=%q) err = %v, want ErrTaskNotFound", id, err)
		}
	}
}

// TestTerminalTaskIsFrozen writes regressions directly, bypassing the
// domain, to show the database itself refuses to modify a terminal task.
func TestTerminalTaskIsFrozen(t *testing.T) {
	for _, terminal := range []domain.TaskStatus{domain.TaskStatusDone, domain.TaskStatusFailed} {
		t.Run(string(terminal), func(t *testing.T) {
			store, _ := newStore(t)
			ctx := t.Context()
			tn := insertTenant(t, store, "acme")
			task := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)
			for _, s := range []domain.TaskStatus{domain.TaskStatusInProgress, terminal} {
				if err := task.TransitionTo(s, base); err != nil {
					t.Fatal(err)
				}
				if err := store.Tasks().Update(ctx, task); err != nil {
					t.Fatal(err)
				}
			}

			for _, status := range []domain.TaskStatus{domain.TaskStatusAccepted, domain.TaskStatusInProgress, domain.TaskStatusDone, domain.TaskStatusFailed} {
				regressed := task
				regressed.Status = status
				regressed.UpdatedAt = base.Add(time.Hour)
				err := store.Tasks().Update(ctx, regressed)
				if !errors.Is(err, domain.ErrInvalidTransition) {
					t.Errorf("%s -> %s: err = %v, want ErrInvalidTransition", terminal, status, err)
				}
			}
			if got, _ := store.Tasks().Get(ctx, task.ID); got != task {
				t.Errorf("terminal task changed: got %+v, want %+v", got, task)
			}
		})
	}
}

// TestTaskGetForUpdateLocksRow shows a second locking read waits for the
// first transaction, while a plain read does not.
func TestTaskGetForUpdateLocksRow(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	tn := insertTenant(t, store, "acme")
	task := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)

	locked, release, holderDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		holderDone <- store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
			if _, err := r.Tasks().GetForUpdate(ctx, task.ID); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	if _, err := store.Tasks().Get(ctx, task.ID); err != nil {
		t.Errorf("plain read blocked or failed: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	err := store.WithinTx(waitCtx, func(ctx context.Context, r application.Repositories) error {
		_, err := r.Tasks().GetForUpdate(ctx, task.ID)
		return err
	})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("locking read while held: err = %v, want it to wait until the deadline", err)
	}

	close(release)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	if _, err := store.Tasks().GetForUpdate(ctx, task.ID); err != nil {
		t.Errorf("locking read after release: %v", err)
	}
}

func TestTaskList(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	a := insertTenant(t, store, "tenant-a")
	b := insertTenant(t, store, "tenant-b")

	// Tenant a: three tasks over time, only the last open. Tenant b: one.
	var aTasks []domain.Task
	for i := range 3 {
		task := newTask(t, domain.TaskTypeUpdate, a.ID, base.Add(time.Duration(i)*time.Second))
		if err := store.Tasks().Insert(ctx, task); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			for _, s := range []domain.TaskStatus{domain.TaskStatusInProgress, domain.TaskStatusDone} {
				if err := task.TransitionTo(s, task.CreatedAt); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Tasks().Update(ctx, task); err != nil {
				t.Fatal(err)
			}
		}
		aTasks = append(aTasks, task)
	}
	bTask := insertTask(t, store, domain.TaskTypeDeploy, b.ID)

	list := func(f application.TaskFilter, p application.PageRequest) application.Page[domain.Task] {
		t.Helper()
		page, err := store.Tasks().List(ctx, f, p)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}

	t.Run("by tenant, paginated", func(t *testing.T) {
		p1 := list(application.TaskFilter{TenantID: a.ID}, application.PageRequest{Limit: 2})
		p2 := list(application.TaskFilter{TenantID: a.ID}, application.PageRequest{Limit: 2, Cursor: p1.NextCursor})
		got := append(p1.Items, p2.Items...)
		if !slices.Equal(got, aTasks) || p1.NextCursor == "" || p2.NextCursor != "" {
			t.Errorf("got %v / %v (cursors %q, %q), want %v", ids(p1.Items), ids(p2.Items), p1.NextCursor, p2.NextCursor, ids(aTasks))
		}
	})

	t.Run("by status", func(t *testing.T) {
		got := list(application.TaskFilter{Status: domain.TaskStatusDone}, application.PageRequest{})
		if !slices.Equal(got.Items, aTasks[:2]) {
			t.Errorf("got %v, want %v", ids(got.Items), ids(aTasks[:2]))
		}
	})

	t.Run("by tenant and status", func(t *testing.T) {
		got := list(application.TaskFilter{TenantID: b.ID, Status: domain.TaskStatusAccepted}, application.PageRequest{})
		if !slices.Equal(got.Items, []domain.Task{bTask}) {
			t.Errorf("got %v, want [%s]", ids(got.Items), bTask.ID[:8])
		}
	})

	t.Run("unknown or malformed tenant", func(t *testing.T) {
		for _, id := range []string{uuid.NewString(), "not-a-uuid"} {
			if got := list(application.TaskFilter{TenantID: id}, application.PageRequest{}); len(got.Items) != 0 {
				t.Errorf("tenant %q: got %v, want none", id, ids(got.Items))
			}
		}
	})
}
