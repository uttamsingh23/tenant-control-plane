-- +goose Up
-- Idempotency keys of worker progress updates already applied. A key is
-- recorded in the same transaction as the state change it caused, so a
-- redelivered update is detected and skipped.
CREATE TABLE processed_updates (
    update_id    text        PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT date_trunc('milliseconds', now()),

    CONSTRAINT processed_updates_update_id_length CHECK (char_length(update_id) BETWEEN 1 AND 200)
);

-- +goose Down
DROP TABLE processed_updates;
