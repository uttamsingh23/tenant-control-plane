package application

import (
	"context"
	"fmt"

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
	// ExpectedVersion is the tenant version the caller last observed.
	ExpectedVersion int64
}

// TenantMutation is the result of a state-changing tenant call: the tenant
// after the change and the task created for it.
type TenantMutation struct {
	Tenant domain.Tenant
	Task   domain.Task
}

// TenantService implements the tenant use cases.
type TenantService struct {
	tx    Transactor
	repos Repositories
}

// NewTenantService wires a TenantService. repos is used for reads outside
// a transaction; tx for every state change.
func NewTenantService(tx Transactor, repos Repositories) *TenantService {
	return &TenantService{tx: tx, repos: repos}
}

// Create records a provisioning tenant with its deploy task and enqueues
// the task event, atomically.
func (s *TenantService) Create(ctx context.Context, _ CreateTenantInput) (TenantMutation, error) {
	err := s.tx.WithinTx(ctx, func(context.Context, Repositories) error {
		return domain.ErrNotImplemented
	})
	return TenantMutation{}, fmt.Errorf("create tenant: %w", err)
}

// Get returns a single tenant.
func (s *TenantService) Get(ctx context.Context, id string) (domain.Tenant, error) {
	return s.repos.Tenants().Get(ctx, id)
}

// List returns one page of tenants.
func (s *TenantService) List(ctx context.Context, filter TenantFilter, page PageRequest) (Page[domain.Tenant], error) {
	return s.repos.Tenants().List(ctx, filter, page.normalized())
}

// Update applies a partial update to an active tenant, moving it to
// updating and creating an update task.
func (s *TenantService) Update(ctx context.Context, _ string, _ UpdateTenantInput) (TenantMutation, error) {
	err := s.tx.WithinTx(ctx, func(context.Context, Repositories) error {
		return domain.ErrNotImplemented
	})
	return TenantMutation{}, fmt.Errorf("update tenant: %w", err)
}

// Delete moves an active or failed tenant to destroying and creates a
// destroy task.
func (s *TenantService) Delete(ctx context.Context, _ string) (TenantMutation, error) {
	err := s.tx.WithinTx(ctx, func(context.Context, Repositories) error {
		return domain.ErrNotImplemented
	})
	return TenantMutation{}, fmt.Errorf("delete tenant: %w", err)
}
