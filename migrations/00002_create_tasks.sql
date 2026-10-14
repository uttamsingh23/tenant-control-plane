-- +goose Up
CREATE TABLE tasks (
    id         uuid        PRIMARY KEY,
    type       text        NOT NULL,
    tenant_id  uuid        NOT NULL,
    status     text        NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,

    CONSTRAINT tasks_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE RESTRICT,
    CONSTRAINT tasks_type_valid CHECK (type IN ('deploy', 'update', 'destroy')),
    CONSTRAINT tasks_status_valid CHECK (status IN ('accepted', 'in_progress', 'done', 'failed')),
    CONSTRAINT tasks_timestamps_millisecond CHECK (
        created_at = date_trunc('milliseconds', created_at)
        AND updated_at = date_trunc('milliseconds', updated_at)
    ),
    CONSTRAINT tasks_updated_not_before_created CHECK (updated_at >= created_at)
);

-- Keyset pagination; the tenant index also serves the foreign key.
CREATE INDEX tasks_created_at_id_idx ON tasks (created_at, id);
CREATE INDEX tasks_tenant_id_created_at_id_idx ON tasks (tenant_id, created_at, id);
CREATE INDEX tasks_status_created_at_id_idx ON tasks (status, created_at, id);

-- The tenant status guards already allow at most one open task per tenant;
-- this index is the database backstop for that invariant.
CREATE UNIQUE INDEX tasks_one_open_per_tenant_idx ON tasks (tenant_id)
    WHERE status IN ('accepted', 'in_progress');

-- Terminal tasks are frozen, so a stale worker update can never regress
-- one, and type, tenant and creation time never change.
-- +goose StatementBegin
CREATE FUNCTION tasks_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('done', 'failed') THEN
        RAISE EXCEPTION 'task %: status % is terminal', OLD.id, OLD.status
            USING ERRCODE = 'check_violation', CONSTRAINT = 'tasks_terminal_immutable';
    END IF;
    IF NEW.id <> OLD.id OR NEW.type <> OLD.type OR NEW.tenant_id <> OLD.tenant_id OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'task %: id, type, tenant_id and created_at are immutable', OLD.id
            USING ERRCODE = 'check_violation', CONSTRAINT = 'tasks_identity_immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER tasks_guard_update
    BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_guard_update();

-- +goose Down
DROP TABLE tasks;
DROP FUNCTION tasks_guard_update();
