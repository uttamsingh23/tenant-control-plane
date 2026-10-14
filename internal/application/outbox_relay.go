package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

// OutboxRelayOptions configures an OutboxRelay.
type OutboxRelayOptions struct {
	// PollInterval is the wait between polls when no event is due.
	PollInterval time.Duration
	// BatchSize is the maximum number of events claimed per poll.
	BatchSize int
	// Lease is how long a claim keeps other relays off an event. If the
	// relay dies mid-batch, its events become claimable when it expires.
	Lease time.Duration
	// PublishTimeout bounds one publish, including the wait for the
	// broker's confirmation, and the write that marks the event published.
	// Lease must exceed twice this value.
	PublishTimeout time.Duration
	// RetryMin and RetryMax bound the exponential backoff, both between
	// attempts to publish one event and between failed polls.
	RetryMin time.Duration
	RetryMax time.Duration
}

func (o OutboxRelayOptions) validate() error {
	var errs []error
	for name, d := range map[string]time.Duration{
		"poll interval": o.PollInterval, "lease": o.Lease, "publish timeout": o.PublishTimeout,
		"minimum retry delay": o.RetryMin, "maximum retry delay": o.RetryMax,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %v", name, d))
		}
	}
	if o.BatchSize <= 0 {
		errs = append(errs, fmt.Errorf("batch size must be positive, got %d", o.BatchSize))
	}
	if o.Lease <= 2*o.PublishTimeout {
		errs = append(errs, fmt.Errorf("lease (%v) must exceed twice the publish timeout (%v)", o.Lease, o.PublishTimeout))
	}
	if o.RetryMin > o.RetryMax {
		errs = append(errs, fmt.Errorf("minimum retry delay (%v) must not exceed the maximum (%v)", o.RetryMin, o.RetryMax))
	}
	return errors.Join(errs...)
}

// OutboxRelay publishes committed task events from the outbox to the
// broker.
//
// Delivery is at-least-once. An event is marked published only after the
// broker confirms it. If the relay stops between the confirmation and the
// mark (a crash, or a failed database write), the event's lease expires
// and the event is published again: consumers must treat the task ID,
// which is the event ID, as an idempotency key.
//
// No database transaction is open while the relay waits for the broker:
// claiming, marking and rescheduling are single committed statements.
// Several relays (one per control-plane replica) can run at once; leases
// make them claim disjoint events.
//
// Events are never discarded. Every publish failure is retried, with
// backoff, for as long as it takes.
type OutboxRelay struct {
	outbox    OutboxRepository
	publisher TaskPublisher
	opts      OutboxRelayOptions
	logger    *slog.Logger
	// jitter randomizes a backoff delay; replaced in tests.
	jitter func(time.Duration) time.Duration
}

// NewOutboxRelay wires an OutboxRelay. outbox must not be bound to a
// transaction, so that each of its calls commits on its own.
func NewOutboxRelay(outbox OutboxRepository, publisher TaskPublisher, opts OutboxRelayOptions, logger *slog.Logger) (*OutboxRelay, error) {
	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("outbox relay: %w", err)
	}
	return &OutboxRelay{outbox: outbox, publisher: publisher, opts: opts, logger: logger, jitter: equalJitter}, nil
}

// Run relays events until ctx is cancelled, then returns ctx.Err(). It
// does not return on broker or database failures; it retries them with
// backoff, so the broker being unavailable at startup is not fatal.
func (r *OutboxRelay) Run(ctx context.Context) error {
	failures := 0
	for {
		published, err := r.RelayBatch(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		wait := r.opts.PollInterval
		switch {
		case err != nil:
			failures++
			wait = r.backoff(failures)
			r.logger.Warn("outbox relay: batch failed, backing off",
				"err", err, "consecutive_failures", failures, "retry_in", wait.String())
		case published == r.opts.BatchSize:
			failures = 0
			wait = 0 // a full batch suggests more events are due
		default:
			failures = 0
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// RelayBatch claims one batch of due events and publishes it, returning
// how many events were published and marked. It stops at the first
// failure and hands the remaining claimed events back.
func (r *OutboxRelay) RelayBatch(ctx context.Context) (int, error) {
	claimedAt := time.Now()
	events, err := r.outbox.ClaimPending(ctx, r.opts.BatchSize, r.opts.Lease)
	if err != nil {
		return 0, fmt.Errorf("claim outbox events: %w", err)
	}
	// Each publish may take up to two timeouts (publish, then mark). Past
	// this point the rest of the lease might run out mid-publish, letting
	// another relay publish the same event concurrently.
	latestStart := claimedAt.Add(r.opts.Lease - 2*r.opts.PublishTimeout)

	for i, event := range events {
		if ctx.Err() != nil {
			// Shutting down: let another relay, or the next start, take
			// the untried events without waiting for the lease.
			r.handBack(ctx, events[i:], 0)
			return i, ctx.Err()
		}
		if time.Now().After(latestStart) {
			r.handBack(ctx, events[i:], 0)
			return i, nil
		}
		if err := r.publishOne(ctx, event); err != nil {
			var mark *markError
			switch {
			case errors.As(err, &mark):
				// Published; its claim is left to expire (see publishOne).
			case ctx.Err() != nil:
				// Interrupted by shutdown, not a broker failure.
				r.handBack(ctx, events[i:], 0)
				return i, ctx.Err()
			default:
				delay := r.backoff(event.Attempts + 1)
				r.recordFailure(ctx, event, delay, err)
				// The broker is probably down for the rest as well.
				r.handBack(ctx, events[i+1:], delay)
			}
			return i, err
		}
	}
	return len(events), nil
}

// markError reports a confirmed publication that could not be recorded.
type markError struct{ err error }

func (e *markError) Error() string { return e.err.Error() }
func (e *markError) Unwrap() error { return e.err }

func (r *OutboxRelay) publishOne(ctx context.Context, event OutboxEvent) error {
	pubCtx, cancel := context.WithTimeout(ctx, r.opts.PublishTimeout)
	err := r.publisher.PublishTask(pubCtx, event.Task)
	cancel()
	if err != nil {
		return fmt.Errorf("publish task %s: %w", event.Task.ID, err)
	}

	// The broker has the event. Record that even if shutdown has begun, so
	// it is not published again needlessly.
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.opts.PublishTimeout)
	defer cancel()
	if err := r.outbox.MarkPublished(markCtx, event.Task.ID); err != nil {
		// The claim is left to expire: the event will be published again
		// after the lease, a duplicate that consumers deduplicate by ID.
		r.logger.Error("outbox relay: event published but not marked; it will be published again when its lease expires",
			"task_id", event.Task.ID, "lease", r.opts.Lease.String(), "err", err)
		return &markError{fmt.Errorf("mark task %s published: %w", event.Task.ID, err)}
	}
	r.logger.Debug("outbox relay: event published", "task_id", event.Task.ID, "task_type", event.Task.Type)
	return nil
}

// failureLogThreshold is the attempt count from which failures are logged
// as errors: an event this stuck needs an operator's attention.
const failureLogThreshold = 5

func (r *OutboxRelay) recordFailure(ctx context.Context, event OutboxEvent, delay time.Duration, failure error) {
	attempt := event.Attempts + 1
	level := slog.LevelWarn
	if attempt >= failureLogThreshold {
		level = slog.LevelError
	}
	r.logger.Log(ctx, level, "outbox relay: publish failed, will retry",
		"task_id", event.Task.ID, "attempt", attempt, "retry_in", delay.String(), "err", failure)
	if err := r.reschedule(ctx, event, delay, failure); err != nil {
		r.logger.Error("outbox relay: could not reschedule event; it is retried when its lease expires",
			"task_id", event.Task.ID, "err", err)
	}
}

// handBack ends the claim on events that were not attempted. If that
// fails, they are retried when their lease expires.
func (r *OutboxRelay) handBack(ctx context.Context, events []OutboxEvent, delay time.Duration) {
	for _, event := range events {
		if err := r.reschedule(ctx, event, delay, nil); err != nil {
			r.logger.Warn("outbox relay: could not hand back event; it is retried when its lease expires",
				"task_id", event.Task.ID, "err", err)
		}
	}
}

// reschedule runs even during shutdown, bounded by the publish timeout.
func (r *OutboxRelay) reschedule(ctx context.Context, event OutboxEvent, delay time.Duration, failure error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.opts.PublishTimeout)
	defer cancel()
	return r.outbox.Reschedule(ctx, event, delay, failure)
}

// backoff returns the jittered delay before retry number attempt (1-based):
// RetryMin doubled per attempt, capped at RetryMax.
func (r *OutboxRelay) backoff(attempt int) time.Duration {
	d := r.opts.RetryMin
	for i := 1; i < attempt && d < r.opts.RetryMax; i++ {
		d *= 2
	}
	return r.jitter(min(d, r.opts.RetryMax))
}

// equalJitter returns a random delay in [d/2, d], so relays that failed
// together do not retry in lockstep.
func equalJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1) //nolint:gosec // jitter, not security-sensitive
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
