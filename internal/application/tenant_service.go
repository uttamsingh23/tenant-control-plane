package application

import (
	"context"
	"fmt"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// CreateTenantInput is the input of TenantService.Create.
type CreateTenantInput struct {
	Slug string
	Name string
}

// UpdateTenantInput is a partial update. Nil fields are left unchanged.
type UpdateTenantInput struct {
	Name *string
	// ExpectedVersion is the tenant version the caller last observed. It is
	// required: an update based on a stale read fails with
	// domain.ErrTenantVersionConflict.
	ExpectedVersion int64
}

// TenantMutation is the result of a state-changing tenant call: the tenant
// after the change and the task created for it.
type TenantMutation struct {
	Tenant domain.Tenant
	Task   domain.Task
}

// TenantService implements the tenant use cases.
//
// Every state change writes the tenant, its new task and the task's outbox
// event in one transaction, so the event is published (by the OutboxRelay)
// only if all three were committed. Errors match the domain sentinels
// (ErrTenantNotFound, ErrTenantAlreadyExists, ErrTenantUpdateNotAllowed,
// ErrTenantVersionConflict, ErrInvalidInput); anything else is internal.
type TenantService struct {
	tx    Transactor
	repos Repositories
	newID IDGenerator
	now   Clock
}

// NewTenantService wires a TenantService. repos is used for reads outside
// a transaction; tx for every state change.
func NewTenantService(tx Transactor, repos Repositories, newID IDGenerator, now Clock) *TenantService {
	return &TenantService{tx: tx, repos: repos, newID: newID, now: now}
}

// Create records a provisioning tenant with its deploy task and enqueues
// the task event, atomically. Slug uniqueness is enforced by the
// repository insert, not by a prior lookup, so of two concurrent creates
// with one slug exactly one succeeds.
func (s *TenantService) Create(ctx context.Context, in CreateTenantInput) (TenantMutation, error) {
	now := s.now()
	tenant, err := domain.NewTenant(s.newID(), in.Slug, in.Name, now)
	if err != nil {
		return TenantMutation{}, fmt.Errorf("create tenant: %w", err)
	}
	task, err := domain.NewTask(s.newID(), domain.TaskTypeDeploy, tenant.ID, now)
	if err != nil {
		return TenantMutation{}, fmt.Errorf("create tenant: %w", err)
	}
	err = s.tx.WithinTx(ctx, func(ctx context.Context, repos Repositories) error {
		if err := repos.Tenants().Insert(ctx, tenant); err != nil {
			return err
		}
		return recordTask(ctx, repos, task)
	})
	if err != nil {
		return TenantMutation{}, fmt.Errorf("create tenant: %w", err)
	}
	return TenantMutation{Tenant: tenant, Task: task}, nil
}

// Get returns a single tenant.
func (s *TenantService) Get(ctx context.Context, id string) (domain.Tenant, error) {
	return s.repos.Tenants().Get(ctx, id)
}

// List returns one page of tenants.
func (s *TenantService) List(ctx context.Context, filter TenantFilter, page PageRequest) (Page[domain.Tenant], error) {
	return s.repos.Tenants().List(ctx, filter, page.normalized())
}

// Update applies a partial update to an active tenant at
// in.ExpectedVersion, moving it to updating and creating an update task.
func (s *TenantService) Update(ctx context.Context, id string, in UpdateTenantInput) (TenantMutation, error) {
	if in.ExpectedVersion < domain.InitialTenantVersion {
		return TenantMutation{}, fmt.Errorf("update tenant %s: %w", id,
			&domain.ValidationError{Field: "version", Reason: "must be the tenant version last read"})
	}
	if in.Name == nil {
		return TenantMutation{}, fmt.Errorf("update tenant %s: %w", id,
			&domain.ValidationError{Field: "body", Reason: "must change at least one field"})
	}
	m, err := s.transition(ctx, id, domain.TaskTypeUpdate, func(t *domain.Tenant, now time.Time) error {
		if t.Version != in.ExpectedVersion {
			return domain.ErrTenantVersionConflict
		}
		return t.RequestUpdate(domain.TenantChanges{Name: in.Name}, now)
	})
	if err != nil {
		return TenantMutation{}, fmt.Errorf("update tenant %s: %w", id, err)
	}
	return m, nil
}

// Delete moves an active or failed tenant to destroying and creates a
// destroy task. DELETE carries no expected version; if the tenant changes
// between the read and the write, it fails with ErrTenantVersionConflict.
func (s *TenantService) Delete(ctx context.Context, id string) (TenantMutation, error) {
	m, err := s.transition(ctx, id, domain.TaskTypeDestroy, (*domain.Tenant).RequestDestroy)
	if err != nil {
		return TenantMutation{}, fmt.Errorf("delete tenant %s: %w", id, err)
	}
	return m, nil
}

// transition runs an API-driven tenant change and records the task of type
// typ it requires, in one transaction. change applies the domain rules to
// the tenant as read. The write is conditional on the version read, so of
// two concurrent transitions at most one commits; the other fails with
// domain.ErrTenantVersionConflict and nothing it wrote is kept.
func (s *TenantService) transition(ctx context.Context, id string, typ domain.TaskType, change func(*domain.Tenant, time.Time) error) (TenantMutation, error) {
	var m TenantMutation
	err := s.tx.WithinTx(ctx, func(ctx context.Context, repos Repositories) error {
		tenant, err := repos.Tenants().Get(ctx, id)
		if err != nil {
			return err
		}
		readVersion := tenant.Version
		now := s.now()
		if err := change(&tenant, now); err != nil {
			return err
		}
		task, err := domain.NewTask(s.newID(), typ, tenant.ID, now)
		if err != nil {
			return err
		}
		if err := repos.Tenants().Update(ctx, tenant, readVersion); err != nil {
			return err
		}
		if err := recordTask(ctx, repos, task); err != nil {
			return err
		}
		m = TenantMutation{Tenant: tenant, Task: task}
		return nil
	})
	return m, err
}

// recordTask stores a new task and its outbox event. It must run in the
// transaction that changed the tenant.
func recordTask(ctx context.Context, repos Repositories, task domain.Task) error {
	if err := repos.Tasks().Insert(ctx, task); err != nil {
		return err
	}
	return repos.Outbox().Enqueue(ctx, task)
}
