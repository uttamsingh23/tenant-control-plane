package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// enqueuedTask inserts a tenant, its deploy task and the task's event.
func enqueuedTask(t *testing.T, store *postgres.Store, slug string) domain.Task {
	t.Helper()
	tn := insertTenant(t, store, slug)
	task := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)
	if err := store.Outbox().Enqueue(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	return task
}

func claim(t *testing.T, r application.Repositories, limit int) []domain.Task {
	t.Helper()
	tasks, err := r.Outbox().ClaimPending(t.Context(), limit)
	if err != nil {
		t.Fatal(err)
	}
	return tasks
}

func assertPendingEvents(t *testing.T, store *postgres.Store, wantTaskIDs ...string) {
	t.Helper()
	got := claim(t, store, 100)
	if len(got) != len(wantTaskIDs) {
		t.Fatalf("pending events = %v, want %v", got, wantTaskIDs)
	}
	for i, task := range got {
		if task.ID != wantTaskIDs[i] {
			t.Errorf("pending[%d] = %s, want %s", i, task.ID, wantTaskIDs[i])
		}
	}
}

// TestOutboxEventIsTaskSnapshot checks the claimed event is the task as it
// was when enqueued, even after the worker has moved the task on.
func TestOutboxEventIsTaskSnapshot(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	want := enqueuedTask(t, store, "acme")

	moved := want
	if err := moved.TransitionTo(domain.TaskStatusInProgress, base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Tasks().Update(ctx, moved); err != nil {
		t.Fatal(err)
	}

	got := claim(t, store, 10)
	if len(got) != 1 || got[0] != want {
		t.Errorf("claimed %+v, want [%+v]", got, want)
	}
}

func TestOutboxClaimOrderAndLimit(t *testing.T) {
	store, _ := newStore(t)
	a := enqueuedTask(t, store, "tenant-a")
	b := enqueuedTask(t, store, "tenant-b")
	c := enqueuedTask(t, store, "tenant-c")

	got := claim(t, store, 2)
	if len(got) != 2 || got[0].ID != a.ID || got[1].ID != b.ID {
		t.Errorf("claimed %v, want [%s %s]", ids(got), a.ID[:8], b.ID[:8])
	}
	assertPendingEvents(t, store, a.ID, b.ID, c.ID)

	for _, limit := range []int{0, -1} {
		if _, err := store.Outbox().ClaimPending(t.Context(), limit); err == nil {
			t.Errorf("ClaimPending(%d) succeeded, want error", limit)
		}
	}
}

func TestOutboxMarkPublished(t *testing.T) {
	store, pool := newStore(t)
	ctx := t.Context()
	a := enqueuedTask(t, store, "tenant-a")
	b := enqueuedTask(t, store, "tenant-b")

	if err := store.Outbox().MarkPublished(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	assertPendingEvents(t, store, b.ID)

	// Idempotent: a repeat keeps the first publication time.
	var first, second time.Time
	publishedAt := `SELECT published_at FROM outbox_events WHERE task_id = $1`
	if err := pool.QueryRow(ctx, publishedAt, a.ID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := store.Outbox().MarkPublished(ctx, a.ID); err != nil {
		t.Fatalf("second MarkPublished: %v", err)
	}
	if err := pool.QueryRow(ctx, publishedAt, a.ID).Scan(&second); err != nil || !second.Equal(first) {
		t.Errorf("published_at changed from %v to %v (%v)", first, second, err)
	}

	for _, id := range []string{uuid.NewString(), "not-a-uuid"} {
		if err := store.Outbox().MarkPublished(ctx, id); !errors.Is(err, domain.ErrTaskNotFound) {
			t.Errorf("MarkPublished(%q) err = %v, want ErrTaskNotFound", id, err)
		}
	}
}

func TestOutboxOneEventPerTask(t *testing.T) {
	store, _ := newStore(t)
	task := enqueuedTask(t, store, "acme")

	err := store.Outbox().Enqueue(t.Context(), task)
	pgError(t, err, "23505", "outbox_events_task_id_key")

	err = store.Outbox().Enqueue(t.Context(), newTask(t, domain.TaskTypeDeploy, task.TenantID, base))
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("event for unknown task: err = %v, want ErrTaskNotFound", err)
	}
}

// TestOutboxClaimSkipsLockedEvents shows two relays claim disjoint events.
func TestOutboxClaimSkipsLockedEvents(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	a := enqueuedTask(t, store, "tenant-a")
	b := enqueuedTask(t, store, "tenant-b")

	claimed, release, firstDone := make(chan []domain.Task), make(chan struct{}), make(chan error, 1)
	go func() {
		firstDone <- store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
			tasks, err := r.Outbox().ClaimPending(ctx, 1)
			if err != nil {
				return err
			}
			claimed <- tasks
			<-release
			return nil
		})
	}()
	first := <-claimed

	var second []domain.Task
	err := store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
		var err error
		second, err = r.Outbox().ClaimPending(ctx, 10)
		return err
	})
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 1 || first[0].ID != a.ID || len(second) != 1 || second[0].ID != b.ID {
		t.Errorf("first claimed %v, second %v; want [%s] and [%s]", ids(first), ids(second), a.ID[:8], b.ID[:8])
	}
}

func TestMarkProcessed(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	id := uuid.NewString()

	if fresh, err := store.ProcessedUpdates().MarkProcessed(ctx, id); err != nil || !fresh {
		t.Errorf("first MarkProcessed = %v, %v; want true", fresh, err)
	}
	if fresh, err := store.ProcessedUpdates().MarkProcessed(ctx, id); err != nil || fresh {
		t.Errorf("repeat MarkProcessed = %v, %v; want false", fresh, err)
	}
}

// TestMarkProcessedRolledBack shows a key recorded in a rolled-back
// transaction is not consumed, so the redelivered update is applied.
func TestMarkProcessedRolledBack(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	id := uuid.NewString()

	err := store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
		if _, err := r.ProcessedUpdates().MarkProcessed(ctx, id); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	if fresh, err := store.ProcessedUpdates().MarkProcessed(ctx, id); err != nil || !fresh {
		t.Errorf("MarkProcessed after rollback = %v, %v; want true", fresh, err)
	}
}

func TestMarkProcessedConcurrent(t *testing.T) {
	store, _ := newStore(t)
	id := uuid.NewString()
	const n = 16
	fresh := make([]bool, n)
	errs := parallel(n, func(i int) error {
		return store.WithinTx(context.Background(), func(ctx context.Context, r application.Repositories) error {
			var err error
			fresh[i], err = r.ProcessedUpdates().MarkProcessed(ctx, id)
			return err
		})
	})
	var wins int
	for i := range n {
		if errs[i] != nil {
			t.Errorf("caller %d: %v", i, errs[i])
		}
		if fresh[i] {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("%d callers saw the update as new, want exactly 1", wins)
	}
}

func TestMarkProcessedRejectsInvalidKeys(t *testing.T) {
	store, _ := newStore(t)
	for _, id := range []string{"", strings.Repeat("x", 201)} {
		if _, err := store.ProcessedUpdates().MarkProcessed(t.Context(), id); err == nil {
			t.Errorf("MarkProcessed(len %d) succeeded, want check violation", len(id))
		}
	}
}
