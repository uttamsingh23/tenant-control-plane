-- +goose Up
CREATE TABLE tenants (
    id         uuid        PRIMARY KEY,
    slug       text        NOT NULL,
    name       text        NOT NULL,
    status     text        NOT NULL,
    version    bigint      NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,

    -- Slugs are never reused: the constraint deliberately covers every row,
    -- including destroyed tenants, and rows are never deleted.
    CONSTRAINT tenants_slug_key UNIQUE (slug),
    CONSTRAINT tenants_slug_format CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    CONSTRAINT tenants_name_not_blank CHECK (name ~ '\S'),
    CONSTRAINT tenants_status_valid CHECK (
        status IN ('provisioning', 'active', 'updating', 'destroying', 'destroyed', 'failed')
    ),
    CONSTRAINT tenants_version_positive CHECK (version >= 1),
    CONSTRAINT tenants_timestamps_millisecond CHECK (
        created_at = date_trunc('milliseconds', created_at)
        AND updated_at = date_trunc('milliseconds', updated_at)
    ),
    CONSTRAINT tenants_updated_not_before_created CHECK (updated_at >= created_at)
);

-- Keyset pagination, unfiltered and filtered by status.
CREATE INDEX tenants_created_at_id_idx ON tenants (created_at, id);
CREATE INDEX tenants_status_created_at_id_idx ON tenants (status, created_at, id);

-- Every update must bump the version by exactly one, whoever the writer is,
-- and identity columns never change.
-- +goose StatementBegin
CREATE FUNCTION tenants_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'tenant %: version must increase by exactly 1 (% -> %)', OLD.id, OLD.version, NEW.version
            USING ERRCODE = 'check_violation', CONSTRAINT = 'tenants_version_increment';
    END IF;
    IF NEW.id <> OLD.id OR NEW.slug <> OLD.slug OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'tenant %: id, slug and created_at are immutable', OLD.id
            USING ERRCODE = 'check_violation', CONSTRAINT = 'tenants_identity_immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER tenants_guard_update
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION tenants_guard_update();

-- +goose Down
DROP TABLE tenants;
DROP FUNCTION tenants_guard_update();
