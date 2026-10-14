package application

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

var errInjected = errors.New("injected failure")

// fakeStore is an in-memory Transactor and Repositories. WithinTx runs
// transactions one at a time and restores the previous state when fn
// fails, like a rollback. The repositories enforce the same invariants as
// the PostgreSQL schema, so a use-case bug cannot hide behind the fake.
type fakeStore struct {
	mu sync.Mutex
	fakeState
	// failOn names one repository operation (e.g. "outbox.enqueue") that
	// returns errInjected, to exercise rollback.
	failOn   string
	lastPage PageRequest
}

type fakeState struct {
	tenants   map[string]domain.Tenant
	tasks     map[string]domain.Task
	outbox    []domain.Task
	processed map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{fakeState: fakeState{
		tenants:   map[string]domain.Tenant{},
		tasks:     map[string]domain.Task{},
		processed: map[string]bool{},
	}}
}

func (s fakeState) clone() fakeState {
	return fakeState{
		tenants:   maps.Clone(s.tenants),
		tasks:     maps.Clone(s.tasks),
		outbox:    slices.Clone(s.outbox),
		processed: maps.Clone(s.processed),
	}
}

func (f *fakeStore) WithinTx(ctx context.Context, fn func(context.Context, Repositories) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	saved := f.clone()
	if err := fn(ctx, f); err != nil {
		f.fakeState = saved
		return err
	}
	return nil
}

func (f *fakeStore) fail(op string) error {
	if f.failOn == op {
		return fmt.Errorf("%s: %w", op, errInjected)
	}
	return nil
}

func (f *fakeStore) Tenants() TenantRepository                   { return fakeTenants{f} }
func (f *fakeStore) Tasks() TaskRepository                       { return fakeTasks{f} }
func (f *fakeStore) Outbox() OutboxRepository                    { return fakeOutbox{f} }
func (f *fakeStore) ProcessedUpdates() ProcessedUpdateRepository { return fakeProcessed{f} }

type fakeTenants struct{ *fakeStore }

func (r fakeTenants) Insert(_ context.Context, t domain.Tenant) error {
	if err := r.fail("tenants.insert"); err != nil {
		return err
	}
	for _, existing := range r.tenants {
		if existing.Slug == t.Slug {
			return domain.ErrTenantAlreadyExists
		}
	}
	r.tenants[t.ID] = t
	return nil
}

func (r fakeTenants) Get(_ context.Context, id string) (domain.Tenant, error) {
	t, ok := r.tenants[id]
	if !ok {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	return t, nil
}

func (r fakeTenants) List(_ context.Context, filter TenantFilter, page PageRequest) (Page[domain.Tenant], error) {
	r.lastPage = page
	var items []domain.Tenant
	for _, t := range r.tenants {
		if filter.Status == "" || t.Status == filter.Status {
			items = append(items, t)
		}
	}
	return Page[domain.Tenant]{Items: items}, nil
}

func (r fakeTenants) Update(_ context.Context, t domain.Tenant, expectedVersion int64) error {
	if err := r.fail("tenants.update"); err != nil {
		return err
	}
	current, ok := r.tenants[t.ID]
	switch {
	case !ok:
		return domain.ErrTenantNotFound
	case current.Version != expectedVersion:
		return domain.ErrTenantVersionConflict
	case t.Version != expectedVersion+1: // the tenants_guard_update trigger
		return fmt.Errorf("version must increase by exactly 1 (%d -> %d)", expectedVersion, t.Version)
	}
	r.tenants[t.ID] = t
	return nil
}

type fakeTasks struct{ *fakeStore }

func (r fakeTasks) Insert(_ context.Context, t domain.Task) error {
	if err := r.fail("tasks.insert"); err != nil {
		return err
	}
	if _, ok := r.tenants[t.TenantID]; !ok {
		return domain.ErrTenantNotFound
	}
	for _, existing := range r.tasks {
		if existing.TenantID == t.TenantID && existing.Status.Open() { // tasks_one_open_per_tenant_idx
			return domain.ErrTenantUpdateNotAllowed
		}
	}
	r.tasks[t.ID] = t
	return nil
}

func (r fakeTasks) Get(_ context.Context, id string) (domain.Task, error) {
	t, ok := r.tasks[id]
	if !ok {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	return t, nil
}

func (r fakeTasks) GetForUpdate(ctx context.Context, id string) (domain.Task, error) {
	return r.Get(ctx, id)
}

func (r fakeTasks) List(_ context.Context, filter TaskFilter, page PageRequest) (Page[domain.Task], error) {
	r.lastPage = page
	var items []domain.Task
	for _, t := range r.tasks {
		if (filter.TenantID == "" || t.TenantID == filter.TenantID) && (filter.Status == "" || t.Status == filter.Status) {
			items = append(items, t)
		}
	}
	return Page[domain.Task]{Items: items}, nil
}

func (r fakeTasks) Update(_ context.Context, t domain.Task) error {
	if err := r.fail("tasks.update"); err != nil {
		return err
	}
	current, ok := r.tasks[t.ID]
	if !ok {
		return domain.ErrTaskNotFound
	}
	if current.Status.Terminal() { // the tasks_guard_update trigger
		return domain.ErrInvalidTransition
	}
	r.tasks[t.ID] = t
	return nil
}

type fakeOutbox struct{ *fakeStore }

func (r fakeOutbox) Enqueue(_ context.Context, t domain.Task) error {
	if err := r.fail("outbox.enqueue"); err != nil {
		return err
	}
	r.outbox = append(r.outbox, t)
	return nil
}

// The relay's side of the outbox is exercised with fakeRelayOutbox
// (outbox_relay_test.go); the use cases only enqueue.

func (fakeOutbox) ClaimPending(context.Context, int, time.Duration) ([]OutboxEvent, error) {
	return nil, domain.ErrNotImplemented
}

func (fakeOutbox) MarkPublished(context.Context, string) error { return domain.ErrNotImplemented }

func (fakeOutbox) Reschedule(context.Context, OutboxEvent, time.Duration, error) error {
	return domain.ErrNotImplemented
}

type fakeProcessed struct{ *fakeStore }

func (r fakeProcessed) MarkProcessed(_ context.Context, id string) (bool, error) {
	if err := r.fail("processed.mark"); err != nil {
		return false, err
	}
	if r.processed[id] {
		return false, nil
	}
	r.processed[id] = true
	return true, nil
}

// ------------------------------------------------------------ fixtures

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)

// fakeClock returns t0, then t0+1s, t0+2s, ... so successive writes have
// distinct, increasing timestamps.
func fakeClock() Clock {
	var mu sync.Mutex
	next := t0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now := next
		next = next.Add(time.Second)
		return now
	}
}

// fakeIDs returns "id-1", "id-2", ...
func fakeIDs() IDGenerator {
	var mu sync.Mutex
	n := 0
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return fmt.Sprintf("id-%d", n)
	}
}

func newServices() (*fakeStore, *TenantService, *TaskService) {
	store := newFakeStore()
	clock := fakeClock()
	return store, NewTenantService(store, store, fakeIDs(), clock), NewTaskService(store, store, clock)
}

// seedTenant stores a tenant directly in status at version 3.
func seedTenant(store *fakeStore, status domain.TenantStatus) domain.Tenant {
	t := domain.Tenant{
		ID: "tenant-" + string(status), Slug: "acme-" + string(status), Name: "Acme",
		Status: status, Version: 3, CreatedAt: t0, UpdatedAt: t0,
	}
	store.tenants[t.ID] = t
	return t
}

// seedTask stores a task of typ in status for tenant.
func seedTask(store *fakeStore, tenant domain.Tenant, typ domain.TaskType, status domain.TaskStatus) domain.Task {
	task := domain.Task{
		ID: "task-" + string(typ), Type: typ, TenantID: tenant.ID,
		Status: status, CreatedAt: t0, UpdatedAt: t0,
	}
	store.tasks[task.ID] = task
	return task
}

// snapshot returns a copy of the store's state for unchanged-ness checks.
func snapshot(store *fakeStore) fakeState { return store.clone() }

func assertUnchanged(t *testing.T, store *fakeStore, before fakeState) {
	t.Helper()
	if !maps.Equal(store.tenants, before.tenants) {
		t.Errorf("tenants changed:\n got %+v\nwant %+v", store.tenants, before.tenants)
	}
	if !maps.Equal(store.tasks, before.tasks) {
		t.Errorf("tasks changed:\n got %+v\nwant %+v", store.tasks, before.tasks)
	}
	if !slices.Equal(store.outbox, before.outbox) {
		t.Errorf("outbox changed:\n got %+v\nwant %+v", store.outbox, before.outbox)
	}
	if !maps.Equal(store.processed, before.processed) {
		t.Errorf("processed updates changed:\n got %v\nwant %v", store.processed, before.processed)
	}
}

func ptr[T any](v T) *T { return &v }
