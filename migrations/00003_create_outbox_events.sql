-- +goose Up
-- Transactional outbox: a row is written in the same transaction as the
-- task it announces and published to the broker only after commit.
CREATE TABLE outbox_events (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id      uuid        NOT NULL,
    -- Task status when the event was recorded. The event is immutable, so a
    -- re-publish after the worker has moved the task on still says what
    -- happened at the time. Type and tenant_id are read from tasks, where
    -- they are immutable.
    task_status  text        NOT NULL,
    occurred_at  timestamptz NOT NULL,
    published_at timestamptz,

    -- Exactly one event per task: the task id is the event id.
    CONSTRAINT outbox_events_task_id_key UNIQUE (task_id),
    CONSTRAINT outbox_events_task_id_fkey FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE RESTRICT,
    CONSTRAINT outbox_events_task_status_valid CHECK (
        task_status IN ('accepted', 'in_progress', 'done', 'failed')
    )
);

-- Pending events in insertion order; stays small as events get published.
CREATE INDEX outbox_events_pending_idx ON outbox_events (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox_events;
