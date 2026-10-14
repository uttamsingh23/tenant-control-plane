package postgres

import (
	"context"
	"fmt"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

func notImplemented(op string) error {
	return fmt.Errorf("postgres %s: %w", op, domain.ErrNotImplemented)
}

type tenantRepository struct{ q querier }

func (r tenantRepository) Insert(context.Context, domain.Tenant) error {
	return notImplemented("insert tenant")
}

func (r tenantRepository) Get(context.Context, string) (domain.Tenant, error) {
	return domain.Tenant{}, notImplemented("get tenant")
}

func (r tenantRepository) List(context.Context, application.TenantFilter, application.PageRequest) (application.Page[domain.Tenant], error) {
	return application.Page[domain.Tenant]{}, notImplemented("list tenants")
}

func (r tenantRepository) Update(context.Context, domain.Tenant, int64) error {
	return notImplemented("update tenant")
}

type taskRepository struct{ q querier }

func (r taskRepository) Insert(context.Context, domain.Task) error {
	return notImplemented("insert task")
}

func (r taskRepository) Get(context.Context, string) (domain.Task, error) {
	return domain.Task{}, notImplemented("get task")
}

func (r taskRepository) GetForUpdate(context.Context, string) (domain.Task, error) {
	return domain.Task{}, notImplemented("get task for update")
}

func (r taskRepository) List(context.Context, application.TaskFilter, application.PageRequest) (application.Page[domain.Task], error) {
	return application.Page[domain.Task]{}, notImplemented("list tasks")
}

func (r taskRepository) Update(context.Context, domain.Task) error {
	return notImplemented("update task")
}

type outboxRepository struct{ q querier }

func (r outboxRepository) Enqueue(context.Context, domain.Task) error {
	return notImplemented("enqueue outbox event")
}

func (r outboxRepository) ClaimPending(context.Context, int) ([]domain.Task, error) {
	return nil, notImplemented("claim outbox events")
}

func (r outboxRepository) MarkPublished(context.Context, string) error {
	return notImplemented("mark outbox event published")
}

type processedUpdateRepository struct{ q querier }

func (r processedUpdateRepository) MarkProcessed(context.Context, string) (bool, error) {
	return false, notImplemented("mark update processed")
}
