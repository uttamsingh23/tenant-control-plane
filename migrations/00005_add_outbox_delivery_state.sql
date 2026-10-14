-- +goose Up
-- Delivery bookkeeping for the outbox relay. A relay claims an event by
-- leasing it (moving available_at into the future) in a short, committed
-- statement, and publishes with no transaction open. The lease is what
-- keeps other relays off the event; if the relay crashes, the lease
-- expires and the event becomes claimable again.
ALTER TABLE outbox_events
    -- The event may be claimed once available_at has passed. A claim moves
    -- it to the end of the lease, a failed attempt to the next retry time.
    ADD COLUMN available_at timestamptz NOT NULL DEFAULT now(),
    -- Failed publish attempts so far; drives the retry backoff. Events are
    -- never dropped, however high this gets.
    ADD COLUMN attempts     integer     NOT NULL DEFAULT 0,
    -- Identifies the current claim, so a relay whose lease has expired
    -- cannot reschedule an event another relay has claimed since.
    ADD COLUMN claim_token  uuid,
    ADD COLUMN last_error   text,
    ADD CONSTRAINT outbox_events_attempts_non_negative CHECK (attempts >= 0);

-- +goose Down
ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_attempts_non_negative,
    DROP COLUMN last_error,
    DROP COLUMN claim_token,
    DROP COLUMN attempts,
    DROP COLUMN available_at;
