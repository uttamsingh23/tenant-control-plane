package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// ---------------------------------------------------------------- tenants

type tenantRepository struct{ q querier }

const tenantColumns = `id, slug, name, status, version, created_at, updated_at`

func scanTenant(row pgx.Row) (domain.Tenant, error) {
	var t domain.Tenant
	if err := row.Scan(&t.ID, &t.Slug, &t.Name, &t.Status, &t.Version, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return domain.Tenant{}, err
	}
	t.CreatedAt = domain.NormalizeTime(t.CreatedAt)
	t.UpdatedAt = domain.NormalizeTime(t.UpdatedAt)
	return t, nil
}

func tenantKey(t domain.Tenant) (time.Time, string) { return t.CreatedAt, t.ID }

// Insert relies on the tenants_slug_key constraint for uniqueness: there is
// no check-then-insert, so concurrent creates with one slug yield exactly
// one success.
func (r tenantRepository) Insert(ctx context.Context, t domain.Tenant) error {
	_, err := r.q.Exec(ctx,
		`INSERT INTO tenants (`+tenantColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		t.ID, t.Slug, t.Name, t.Status, t.Version, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return translate("insert tenant", err)
	}
	return nil
}

func (r tenantRepository) Get(ctx context.Context, id string) (domain.Tenant, error) {
	if !isUUID(id) {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	t, err := scanTenant(r.q.QueryRow(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	if err != nil {
		return domain.Tenant{}, translate("get tenant", err)
	}
	return t, nil
}

func (r tenantRepository) List(ctx context.Context, filter application.TenantFilter, page application.PageRequest) (application.Page[domain.Tenant], error) {
	afterCreatedAt, afterID, err := decodeCursor(page.Cursor)
	if err != nil {
		return application.Page[domain.Tenant]{}, err
	}
	limit := pageLimit(page)
	rows, err := r.q.Query(ctx, `
		SELECT `+tenantColumns+` FROM tenants
		WHERE ($1::text IS NULL OR status = $1)
		  AND ($2::timestamptz IS NULL OR (created_at, id) > ($2, $3::uuid))
		ORDER BY created_at, id
		LIMIT $4`,
		nullIfEmpty(filter.Status), afterCreatedAt, afterID, limit+1)
	if err != nil {
		return application.Page[domain.Tenant]{}, translate("list tenants", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Tenant, error) { return scanTenant(row) })
	if err != nil {
		return application.Page[domain.Tenant]{}, translate("list tenants", err)
	}
	return toPage(items, limit, tenantKey), nil
}

// Update is the optimistic-locking write: it stores t only if the row is
// still at expectedVersion. When no row matches, a follow-up existence
// check tells a missing tenant from a version conflict. The
// tenants_guard_update trigger additionally rejects any write whose
// version is not exactly the old version plus one.
func (r tenantRepository) Update(ctx context.Context, t domain.Tenant, expectedVersion int64) error {
	if !isUUID(t.ID) {
		return domain.ErrTenantNotFound
	}
	tag, err := r.q.Exec(ctx, `
		UPDATE tenants
		SET name = $2, status = $3, version = $4, updated_at = $5
		WHERE id = $1 AND version = $6`,
		t.ID, t.Name, t.Status, t.Version, t.UpdatedAt, expectedVersion)
	if err != nil {
		return translate("update tenant", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var exists bool
	if err := r.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`, t.ID).Scan(&exists); err != nil {
		return translate("update tenant", err)
	}
	if !exists {
		return domain.ErrTenantNotFound
	}
	return domain.ErrTenantVersionConflict
}

// ------------------------------------------------------------------ tasks

type taskRepository struct{ q querier }

const taskColumns = `id, type, tenant_id, status, created_at, updated_at`

func scanTask(row pgx.Row) (domain.Task, error) {
	var t domain.Task
	if err := row.Scan(&t.ID, &t.Type, &t.TenantID, &t.Status, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return domain.Task{}, err
	}
	t.CreatedAt = domain.NormalizeTime(t.CreatedAt)
	t.UpdatedAt = domain.NormalizeTime(t.UpdatedAt)
	return t, nil
}

func taskKey(t domain.Task) (time.Time, string) { return t.CreatedAt, t.ID }

func (r taskRepository) Insert(ctx context.Context, t domain.Task) error {
	_, err := r.q.Exec(ctx,
		`INSERT INTO tasks (`+taskColumns+`) VALUES ($1, $2, $3, $4, $5, $6)`,
		t.ID, t.Type, t.TenantID, t.Status, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return translate("insert task", err)
	}
	return nil
}

func (r taskRepository) Get(ctx context.Context, id string) (domain.Task, error) {
	return r.get(ctx, id, `SELECT `+taskColumns+` FROM tasks WHERE id = $1`)
}

// GetForUpdate locks the task row until the surrounding transaction ends,
// so concurrent worker updates for one task are applied one at a time.
func (r taskRepository) GetForUpdate(ctx context.Context, id string) (domain.Task, error) {
	return r.get(ctx, id, `SELECT `+taskColumns+` FROM tasks WHERE id = $1 FOR UPDATE`)
}

func (r taskRepository) get(ctx context.Context, id, query string) (domain.Task, error) {
	if !isUUID(id) {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	t, err := scanTask(r.q.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	if err != nil {
		return domain.Task{}, translate("get task", err)
	}
	return t, nil
}

func (r taskRepository) List(ctx context.Context, filter application.TaskFilter, page application.PageRequest) (application.Page[domain.Task], error) {
	if filter.TenantID != "" && !isUUID(filter.TenantID) {
		return application.Page[domain.Task]{}, nil // no tenant has this id
	}
	afterCreatedAt, afterID, err := decodeCursor(page.Cursor)
	if err != nil {
		return application.Page[domain.Task]{}, err
	}
	limit := pageLimit(page)
	rows, err := r.q.Query(ctx, `
		SELECT `+taskColumns+` FROM tasks
		WHERE ($1::uuid IS NULL OR tenant_id = $1)
		  AND ($2::text IS NULL OR status = $2)
		  AND ($3::timestamptz IS NULL OR (created_at, id) > ($3, $4::uuid))
		ORDER BY created_at, id
		LIMIT $5`,
		nullIfEmpty(filter.TenantID), nullIfEmpty(filter.Status), afterCreatedAt, afterID, limit+1)
	if err != nil {
		return application.Page[domain.Task]{}, translate("list tasks", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Task, error) { return scanTask(row) })
	if err != nil {
		return application.Page[domain.Task]{}, translate("list tasks", err)
	}
	return toPage(items, limit, taskKey), nil
}

// Update stores the task's status. Transition rules are the domain's job;
// the tasks_guard_update trigger still refuses to modify a terminal task.
func (r taskRepository) Update(ctx context.Context, t domain.Task) error {
	if !isUUID(t.ID) {
		return domain.ErrTaskNotFound
	}
	tag, err := r.q.Exec(ctx, `UPDATE tasks SET status = $2, updated_at = $3 WHERE id = $1`,
		t.ID, t.Status, t.UpdatedAt)
	if err != nil {
		return translate("update task", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTaskNotFound
	}
	return nil
}

// ----------------------------------------------------------------- outbox

type outboxRepository struct{ q querier }

// Enqueue records the event for task, snapshotting its current status. It
// must run in the transaction that wrote the task. A second event for the
// same task violates outbox_events_task_id_key.
func (r outboxRepository) Enqueue(ctx context.Context, task domain.Task) error {
	_, err := r.q.Exec(ctx,
		`INSERT INTO outbox_events (task_id, task_status, occurred_at) VALUES ($1, $2, $3)`,
		task.ID, task.Status, task.UpdatedAt)
	if err != nil {
		return translate("enqueue outbox event", err)
	}
	return nil
}

// ClaimPending returns up to limit unpublished events, oldest first, as
// the task they announce (status and updated_at as of the event). FOR
// UPDATE SKIP LOCKED makes concurrent relays claim disjoint sets; the
// claim lasts until the surrounding transaction ends.
func (r outboxRepository) ClaimPending(ctx context.Context, limit int) ([]domain.Task, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("claim outbox events: limit must be positive, got %d", limit)
	}
	rows, err := r.q.Query(ctx, `
		SELECT t.id, t.type, t.tenant_id, o.task_status, t.created_at, o.occurred_at
		FROM outbox_events o
		JOIN tasks t ON t.id = o.task_id
		WHERE o.published_at IS NULL
		ORDER BY o.id
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED`, limit)
	if err != nil {
		return nil, translate("claim outbox events", err)
	}
	tasks, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Task, error) { return scanTask(row) })
	if err != nil {
		return nil, translate("claim outbox events", err)
	}
	return tasks, nil
}

// MarkPublished records that the event for taskID reached the broker. It
// is idempotent: the first publication time is kept.
func (r outboxRepository) MarkPublished(ctx context.Context, taskID string) error {
	if !isUUID(taskID) {
		return fmt.Errorf("outbox event for task %q: %w", taskID, domain.ErrTaskNotFound)
	}
	tag, err := r.q.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = COALESCE(published_at, date_trunc('milliseconds', now()))
		WHERE task_id = $1`, taskID)
	if err != nil {
		return translate("mark outbox event published", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox event for task %s: %w", taskID, domain.ErrTaskNotFound)
	}
	return nil
}

// ------------------------------------------------------ processed updates

type processedUpdateRepository struct{ q querier }

// MarkProcessed inserts updateID and reports whether it was new. A
// concurrent transaction inserting the same key blocks until the first one
// finishes: if it commits, this returns false; if it rolls back, true.
func (r processedUpdateRepository) MarkProcessed(ctx context.Context, updateID string) (bool, error) {
	tag, err := r.q.Exec(ctx,
		`INSERT INTO processed_updates (update_id) VALUES ($1) ON CONFLICT (update_id) DO NOTHING`, updateID)
	if err != nil {
		return false, translate("mark update processed", err)
	}
	return tag.RowsAffected() == 1, nil
}
