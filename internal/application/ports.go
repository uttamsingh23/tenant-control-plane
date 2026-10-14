package application

import (
	"context"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// The interfaces below are what the application layer needs from the
// outside world. They are defined here, by their consumer, and implemented
// by the adapters (postgres, rabbitmq).

// IDGenerator returns a new unique identifier for a tenant or task. The
// PostgreSQL adapter stores IDs as UUIDs; production wiring uses
// uuid.NewString.
type IDGenerator func() string

// Clock returns the current time; production wiring uses time.Now.
type Clock func() time.Time

// TenantRepository persists tenants.
type TenantRepository interface {
	// Insert stores a new tenant. It returns domain.ErrTenantAlreadyExists
	// if the slug is taken, including by a destroyed tenant.
	Insert(ctx context.Context, tenant domain.Tenant) error
	// Get returns domain.ErrTenantNotFound if no tenant has the given ID.
	Get(ctx context.Context, id string) (domain.Tenant, error)
	List(ctx context.Context, filter TenantFilter, page PageRequest) (Page[domain.Tenant], error)
	// Update stores tenant only if the persisted version equals
	// expectedVersion, otherwise it returns domain.ErrTenantVersionConflict.
	Update(ctx context.Context, tenant domain.Tenant, expectedVersion int64) error
}

// TaskRepository persists tasks.
type TaskRepository interface {
	Insert(ctx context.Context, task domain.Task) error
	// Get returns domain.ErrTaskNotFound if no task has the given ID.
	Get(ctx context.Context, id string) (domain.Task, error)
	// GetForUpdate is Get plus a row lock held until the transaction ends.
	GetForUpdate(ctx context.Context, id string) (domain.Task, error)
	List(ctx context.Context, filter TaskFilter, page PageRequest) (Page[domain.Task], error)
	Update(ctx context.Context, task domain.Task) error
}

// OutboxRepository implements the transactional outbox: task events are
// recorded in the same transaction as the state change and published by
// the OutboxRelay only after commit.
type OutboxRepository interface {
	Enqueue(ctx context.Context, task domain.Task) error
	// ClaimPending locks up to limit unpublished events for the rest of the
	// transaction, skipping rows already claimed by another relay.
	ClaimPending(ctx context.Context, limit int) ([]domain.Task, error)
	MarkPublished(ctx context.Context, taskID string) error
}

// ProcessedUpdateRepository records the idempotency keys of applied
// worker updates.
type ProcessedUpdateRepository interface {
	// MarkProcessed records updateID and reports whether it was not seen before.
	MarkProcessed(ctx context.Context, updateID string) (bool, error)
}

// Repositories groups the repositories that take part in one unit of work.
type Repositories interface {
	Tenants() TenantRepository
	Tasks() TaskRepository
	Outbox() OutboxRepository
	ProcessedUpdates() ProcessedUpdateRepository
}

// Transactor runs fn inside a single database transaction. The transaction
// commits if fn returns nil and rolls back otherwise.
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, repos Repositories) error) error
}

// TaskPublisher publishes task events to the broker.
type TaskPublisher interface {
	PublishTask(ctx context.Context, task domain.Task) error
}
