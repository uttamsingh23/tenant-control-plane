// Package httpapi exposes the control plane over a JSON/HTTP API.
//
// Handlers decode requests, delegate to the application services declared
// below and encode responses. They hold no business logic.
package httpapi

import (
	"context"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// TenantService is the tenant use-case API consumed by the handlers.
type TenantService interface {
	Create(ctx context.Context, in application.CreateTenantInput) (application.TenantMutation, error)
	Get(ctx context.Context, id string) (domain.Tenant, error)
	List(ctx context.Context, filter application.TenantFilter, page application.PageRequest) (application.Page[domain.Tenant], error)
	Update(ctx context.Context, id string, in application.UpdateTenantInput) (application.TenantMutation, error)
	Delete(ctx context.Context, id string) (application.TenantMutation, error)
}

// TaskService is the read-only task API consumed by the handlers.
type TaskService interface {
	Get(ctx context.Context, id string) (domain.Task, error)
	List(ctx context.Context, filter application.TaskFilter, page application.PageRequest) (application.Page[domain.Task], error)
}

var (
	_ TenantService = (*application.TenantService)(nil)
	_ TaskService   = (*application.TaskService)(nil)
)
