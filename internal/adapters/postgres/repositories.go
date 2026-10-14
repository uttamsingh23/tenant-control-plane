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

// ClaimPending leases up to limit due events, oldest first, and returns
// them as the task they announce (status and updated_at as of the event).
//
// The claim is a single UPDATE: run on the pool it commits on its own, so
// the row locks last only for the statement and no transaction is held
// while the caller publishes. FOR UPDATE SKIP LOCKED makes concurrent
// claims pick disjoint rows; once a claim commits, the moved available_at
// keeps other relays off the row until the lease expires. Lease and retry
// times use the database clock, so relays on different hosts agree.
//
// Rows of uncommitted transactions are invisible, so only committed events
// are claimed. The relay selects by published_at IS NULL rather than "id
// above the last one seen", so an event whose transaction commits after a
// later-numbered one is not skipped.
func (r outboxRepository) ClaimPending(ctx context.Context, limit int, lease time.Duration) ([]application.OutboxEvent, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("claim outbox events: limit must be positive, got %d", limit)
	}
	if lease <= 0 {
		return nil, fmt.Errorf("claim outbox events: lease must be positive, got %v", lease)
	}
	rows, err := r.q.Query(ctx, `
		WITH due AS (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL AND available_at <= now()
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE outbox_events o
			SET available_at = now() + $2::bigint * interval '1 microsecond',
			    claim_token  = gen_random_uuid()
			FROM due
			WHERE o.id = due.id
			RETURNING o.id, o.task_id, o.task_status, o.occurred_at, o.attempts, o.claim_token
		)
		SELECT t.id, t.type, t.tenant_id, c.task_status, t.created_at, c.occurred_at, c.attempts, c.claim_token
		FROM claimed c
		JOIN tasks t ON t.id = c.task_id
		ORDER BY c.id`,
		limit, lease.Microseconds())
	if err != nil {
		return nil, translate("claim outbox events", err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (application.OutboxEvent, error) {
		var e application.OutboxEvent
		t := &e.Task
		if err := row.Scan(&t.ID, &t.Type, &t.TenantID, &t.Status, &t.CreatedAt, &t.UpdatedAt, &e.Attempts, &e.ClaimToken); err != nil {
			return application.OutboxEvent{}, err
		}
		t.CreatedAt = domain.NormalizeTime(t.CreatedAt)
		t.UpdatedAt = domain.NormalizeTime(t.UpdatedAt)
		return e, nil
	})
	if err != nil {
		return nil, translate("claim outbox events", err)
	}
	return events, nil
}

// maxLastErrorLen bounds the stored failure text.
const maxLastErrorLen = 1000

// Reschedule ends e's claim and makes the event claimable after delay.
// The claim token check means a relay whose lease expired (and whose
// event was claimed again) cannot move another relay's lease; the
// published_at check means a published event stays published.
func (r outboxRepository) Reschedule(ctx context.Context, e application.OutboxEvent, delay time.Duration, failure error) error {
	if !isUUID(e.Task.ID) || !isUUID(e.ClaimToken) {
		return fmt.Errorf("reschedule outbox event for task %q: invalid task id or claim token", e.Task.ID)
	}
	var failed bool
	var reason *string
	if failure != nil {
		failed = true
		msg := failure.Error()
		reason = &msg
	}
	_, err := r.q.Exec(ctx, `
		UPDATE outbox_events
		SET available_at = now() + $3::bigint * interval '1 microsecond',
		    claim_token  = NULL,
		    attempts     = attempts + CASE WHEN $4 THEN 1 ELSE 0 END,
		    last_error   = CASE WHEN $4 THEN left($5, $6) ELSE last_error END
		WHERE task_id = $1 AND claim_token = $2 AND published_at IS NULL`,
		e.Task.ID, e.ClaimToken, max(delay, 0).Microseconds(), failed, reason, maxLastErrorLen)
	if err != nil {
		return translate("reschedule outbox event", err)
	}
	return nil
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
