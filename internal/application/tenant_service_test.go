package application

import (
	"errors"
	"sync"
	"testing"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

func TestCreateTenant(t *testing.T) {
	store, tenants, _ := newServices()

	m, err := tenants.Create(t.Context(), CreateTenantInput{Slug: "acme", Name: "Acme"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	want := domain.Tenant{
		ID: "id-1", Slug: "acme", Name: "Acme", Status: domain.TenantStatusProvisioning,
		Version: domain.InitialTenantVersion, CreatedAt: t0, UpdatedAt: t0,
	}
	if m.Tenant != want {
		t.Errorf("tenant = %+v, want %+v", m.Tenant, want)
	}
	wantTask := domain.Task{
		ID: "id-2", Type: domain.TaskTypeDeploy, TenantID: "id-1",
		Status: domain.TaskStatusAccepted, CreatedAt: t0, UpdatedAt: t0,
	}
	if m.Task != wantTask {
		t.Errorf("task = %+v, want %+v", m.Task, wantTask)
	}
	if store.tenants["id-1"] != want || store.tasks["id-2"] != wantTask {
		t.Error("tenant or task not persisted as returned")
	}
	if len(store.outbox) != 1 || store.outbox[0] != wantTask {
		t.Errorf("outbox = %+v, want exactly the deploy task", store.outbox)
	}
}

func TestCreateTenantRejectsInvalidInputWithoutWriting(t *testing.T) {
	tests := map[string]CreateTenantInput{
		"slug too short":   {Slug: "ab", Name: "Acme"},
		"slug uppercase":   {Slug: "Acme", Name: "Acme"},
		"slug ends hyphen": {Slug: "acme-", Name: "Acme"},
		"empty name":       {Slug: "acme", Name: ""},
		"blank name":       {Slug: "acme", Name: "  "},
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			store, tenants, _ := newServices()
			before := snapshot(store)
			_, err := tenants.Create(t.Context(), in)
			if !errors.Is(err, domain.ErrInvalidInput) {
				t.Fatalf("err = %v, want ErrInvalidInput", err)
			}
			assertUnchanged(t, store, before)
		})
	}
}

func TestCreateTenantDuplicateSlug(t *testing.T) {
	store, tenants, _ := newServices()
	if _, err := tenants.Create(t.Context(), CreateTenantInput{Slug: "acme", Name: "Acme"}); err != nil {
		t.Fatal(err)
	}
	before := snapshot(store)

	_, err := tenants.Create(t.Context(), CreateTenantInput{Slug: "acme", Name: "Other"})
	if !errors.Is(err, domain.ErrTenantAlreadyExists) {
		t.Fatalf("err = %v, want ErrTenantAlreadyExists", err)
	}
	assertUnchanged(t, store, before)
}

// A failure at any write rolls back the ones before it: no tenant without
// its task, no task without its event.
func TestCreateTenantIsAtomic(t *testing.T) {
	for _, op := range []string{"tenants.insert", "tasks.insert", "outbox.enqueue"} {
		t.Run(op, func(t *testing.T) {
			store, tenants, _ := newServices()
			store.failOn = op
			before := snapshot(store)

			_, err := tenants.Create(t.Context(), CreateTenantInput{Slug: "acme", Name: "Acme"})
			if !errors.Is(err, errInjected) {
				t.Fatalf("err = %v, want injected failure", err)
			}
			assertUnchanged(t, store, before)
		})
	}
}

func TestUpdateTenant(t *testing.T) {
	store, tenants, _ := newServices()
	seeded := seedTenant(store, domain.TenantStatusActive)

	m, err := tenants.Update(t.Context(), seeded.ID, UpdateTenantInput{Name: ptr("Acme Corp"), ExpectedVersion: seeded.Version})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if m.Tenant.Status != domain.TenantStatusUpdating || m.Tenant.Name != "Acme Corp" || m.Tenant.Version != seeded.Version+1 {
		t.Errorf("tenant = %+v, want updating, renamed, version %d", m.Tenant, seeded.Version+1)
	}
	if m.Task.Type != domain.TaskTypeUpdate || m.Task.Status != domain.TaskStatusAccepted || m.Task.TenantID != seeded.ID {
		t.Errorf("task = %+v, want accepted update task for %s", m.Task, seeded.ID)
	}
	if m.Tenant.UpdatedAt != m.Task.CreatedAt {
		t.Errorf("tenant updated_at %v != task created_at %v; both come from one clock reading", m.Tenant.UpdatedAt, m.Task.CreatedAt)
	}
	if store.tenants[seeded.ID] != m.Tenant || store.tasks[m.Task.ID] != m.Task {
		t.Error("tenant or task not persisted as returned")
	}
	if len(store.outbox) != 1 || store.outbox[0] != m.Task {
		t.Errorf("outbox = %+v, want exactly the update task", store.outbox)
	}
}

func TestUpdateTenantErrors(t *testing.T) {
	tests := []struct {
		name    string
		status  domain.TenantStatus
		id      string // defaults to the seeded tenant
		in      UpdateTenantInput
		wantErr error
	}{
		{name: "not found", status: domain.TenantStatusActive, id: "missing", in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 3}, wantErr: domain.ErrTenantNotFound},
		{name: "stale version", status: domain.TenantStatusActive, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 2}, wantErr: domain.ErrTenantVersionConflict},
		{name: "future version", status: domain.TenantStatusActive, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 4}, wantErr: domain.ErrTenantVersionConflict},
		{name: "missing version", status: domain.TenantStatusActive, in: UpdateTenantInput{Name: ptr("X")}, wantErr: domain.ErrInvalidInput},
		{name: "negative version", status: domain.TenantStatusActive, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: -1}, wantErr: domain.ErrInvalidInput},
		{name: "no changes", status: domain.TenantStatusActive, in: UpdateTenantInput{ExpectedVersion: 3}, wantErr: domain.ErrInvalidInput},
		{name: "blank name", status: domain.TenantStatusActive, in: UpdateTenantInput{Name: ptr(" "), ExpectedVersion: 3}, wantErr: domain.ErrInvalidInput},
		{name: "provisioning", status: domain.TenantStatusProvisioning, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 3}, wantErr: domain.ErrTenantUpdateNotAllowed},
		{name: "updating", status: domain.TenantStatusUpdating, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 3}, wantErr: domain.ErrTenantUpdateNotAllowed},
		{name: "destroying", status: domain.TenantStatusDestroying, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 3}, wantErr: domain.ErrTenantUpdateNotAllowed},
		{name: "destroyed", status: domain.TenantStatusDestroyed, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 3}, wantErr: domain.ErrTenantUpdateNotAllowed},
		{name: "failed", status: domain.TenantStatusFailed, in: UpdateTenantInput{Name: ptr("X"), ExpectedVersion: 3}, wantErr: domain.ErrTenantUpdateNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, tenants, _ := newServices()
			seeded := seedTenant(store, tt.status)
			id := tt.id
			if id == "" {
				id = seeded.ID
			}
			before := snapshot(store)

			_, err := tenants.Update(t.Context(), id, tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			assertUnchanged(t, store, before)
		})
	}
}

func TestUpdateTenantIsAtomic(t *testing.T) {
	for _, op := range []string{"tenants.update", "tasks.insert", "outbox.enqueue"} {
		t.Run(op, func(t *testing.T) {
			store, tenants, _ := newServices()
			seeded := seedTenant(store, domain.TenantStatusActive)
			store.failOn = op
			before := snapshot(store)

			_, err := tenants.Update(t.Context(), seeded.ID, UpdateTenantInput{Name: ptr("X"), ExpectedVersion: seeded.Version})
			if !errors.Is(err, errInjected) {
				t.Fatalf("err = %v, want injected failure", err)
			}
			assertUnchanged(t, store, before)
		})
	}
}

// Two PATCHes with the same expected version: one wins, the other sees the
// bumped version and gets a conflict. The PostgreSQL integration test
// covers the case where both read before either writes.
func TestConcurrentUpdatesWithSameVersion(t *testing.T) {
	store, tenants, _ := newServices()
	seeded := seedTenant(store, domain.TenantStatusActive)

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			_, errs[i] = tenants.Update(t.Context(), seeded.ID, UpdateTenantInput{Name: ptr("X"), ExpectedVersion: seeded.Version})
		})
	}
	wg.Wait()

	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, domain.ErrTenantVersionConflict):
			t.Errorf("loser err = %v, want ErrTenantVersionConflict", err)
		}
	}
	if wins != 1 || len(store.outbox) != 1 {
		t.Errorf("wins = %d, outbox events = %d, want 1 and 1", wins, len(store.outbox))
	}
}

func TestDeleteTenant(t *testing.T) {
	for _, status := range []domain.TenantStatus{domain.TenantStatusActive, domain.TenantStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			store, tenants, _ := newServices()
			seeded := seedTenant(store, status)

			m, err := tenants.Delete(t.Context(), seeded.ID)
			if err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if m.Tenant.Status != domain.TenantStatusDestroying || m.Tenant.Version != seeded.Version+1 || m.Tenant.Name != seeded.Name {
				t.Errorf("tenant = %+v, want destroying at version %d", m.Tenant, seeded.Version+1)
			}
			if m.Task.Type != domain.TaskTypeDestroy || m.Task.Status != domain.TaskStatusAccepted {
				t.Errorf("task = %+v, want accepted destroy task", m.Task)
			}
			if store.tenants[seeded.ID] != m.Tenant || len(store.outbox) != 1 || store.outbox[0] != m.Task {
				t.Error("tenant, task and event not persisted together")
			}
		})
	}
}

func TestDeleteTenantErrors(t *testing.T) {
	tests := []struct {
		status  domain.TenantStatus
		id      string
		wantErr error
	}{
		{status: domain.TenantStatusActive, id: "missing", wantErr: domain.ErrTenantNotFound},
		{status: domain.TenantStatusProvisioning, wantErr: domain.ErrTenantUpdateNotAllowed},
		{status: domain.TenantStatusUpdating, wantErr: domain.ErrTenantUpdateNotAllowed},
		{status: domain.TenantStatusDestroying, wantErr: domain.ErrTenantUpdateNotAllowed},
		{status: domain.TenantStatusDestroyed, wantErr: domain.ErrTenantUpdateNotAllowed},
	}
	for _, tt := range tests {
		t.Run(string(tt.status)+"/"+tt.id, func(t *testing.T) {
			store, tenants, _ := newServices()
			seeded := seedTenant(store, tt.status)
			id := tt.id
			if id == "" {
				id = seeded.ID
			}
			before := snapshot(store)

			_, err := tenants.Delete(t.Context(), id)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			assertUnchanged(t, store, before)
		})
	}
}

func TestDeleteTenantIsAtomic(t *testing.T) {
	store, tenants, _ := newServices()
	seeded := seedTenant(store, domain.TenantStatusActive)
	store.failOn = "outbox.enqueue"
	before := snapshot(store)

	if _, err := tenants.Delete(t.Context(), seeded.ID); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want injected failure", err)
	}
	assertUnchanged(t, store, before)
}

func TestTenantReads(t *testing.T) {
	store, tenants, _ := newServices()
	seeded := seedTenant(store, domain.TenantStatusActive)
	seedTenant(store, domain.TenantStatusFailed)

	got, err := tenants.Get(t.Context(), seeded.ID)
	if err != nil || got != seeded {
		t.Errorf("Get = %+v, %v; want %+v", got, err, seeded)
	}
	if _, err := tenants.Get(t.Context(), "missing"); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Errorf("Get(missing) err = %v, want ErrTenantNotFound", err)
	}

	page, err := tenants.List(t.Context(), TenantFilter{Status: domain.TenantStatusActive}, PageRequest{Limit: MaxPageLimit + 1})
	if err != nil || len(page.Items) != 1 || page.Items[0] != seeded {
		t.Errorf("List = %+v, %v; want only the active tenant", page, err)
	}
	if store.lastPage.Limit != MaxPageLimit {
		t.Errorf("repository got limit %d, want it clamped to %d", store.lastPage.Limit, MaxPageLimit)
	}
}
