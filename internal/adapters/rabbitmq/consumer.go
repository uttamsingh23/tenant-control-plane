package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// UpdateApplier applies a worker progress update; *application.TaskService
// satisfies it.
type UpdateApplier interface {
	ApplyUpdate(ctx context.Context, update domain.TaskUpdate) (application.UpdateOutcome, error)
}

// UpdateConsumerOptions tunes an UpdateConsumer. Zero values take the
// defaults below.
type UpdateConsumerOptions struct {
	// Concurrency is how many updates are applied in parallel; it is also
	// the prefetch count. Default 4.
	Concurrency int
	// MaxAttempts is how often one delivery is applied before a retryable
	// failure hands it back to the queue. Default 10.
	MaxAttempts int
	// RetryMin and RetryMax bound the exponential backoff between attempts
	// and between reconnects. Defaults 200ms and 10s.
	RetryMin time.Duration
	RetryMax time.Duration
	// ApplyTimeout bounds one attempt, so a hung database call cannot hold
	// a delivery forever. Default 10s.
	ApplyTimeout time.Duration
	// ShutdownTimeout is how long in-flight attempts may keep running once
	// Run's context is cancelled. Default 10s.
	ShutdownTimeout time.Duration
}

func (o UpdateConsumerOptions) withDefaults() UpdateConsumerOptions {
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	if o.RetryMin <= 0 {
		o.RetryMin = 200 * time.Millisecond
	}
	if o.RetryMax < o.RetryMin {
		o.RetryMax = max(10*time.Second, o.RetryMin)
	}
	if o.ApplyTimeout <= 0 {
		o.ApplyTimeout = 10 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 10 * time.Second
	}
	return o
}

// UpdateConsumer consumes worker progress updates and applies them with an
// UpdateApplier.
//
// Delivery is at-least-once, and the consumer is idempotent; it does not
// make processing exactly-once. Every update may arrive more than once,
// late or out of order, and the outcome is the same as if it had arrived
// once and in order:
//
//   - The update's message_id is the idempotency key. ApplyUpdate records
//     it in processed_updates in the same transaction as the task and
//     tenant changes, so a key is recorded if and only if its effect is.
//     A repeat finds the key and changes nothing.
//   - A delivery is acked only after ApplyUpdate has returned, so after
//     the commit. If the process dies between the commit and the ack, the
//     broker redelivers, the key is found, and the redelivery is acked
//     with no further effect.
//   - Stale and conflicting updates (in_progress after done, a second
//     outcome) are recorded and acked without changing anything.
//
// Settling a delivery:
//
//	decoded and applied, duplicate, stale or conflict -> ack
//	malformed (ErrMalformedMessage)                   -> reject, dead-lettered
//	permanent (application.IsPermanentUpdateError)    -> reject, dead-lettered
//	retryable, attempts left                          -> retried here, after backoff
//	retryable, MaxAttempts reached or shutting down   -> nack and requeue
//
// Poison-message policy for an unknown task: dead-letter at once, without
// recording the key. Tasks are committed before their event is published,
// and the worker only reports on tasks it was sent, so an update for a
// missing task is not a race that resolves itself. It comes from another
// environment, a hand-published message or a restored database. Retrying
// cannot fix it and dropping it would hide it; in the dead-letter queue it
// can be inspected and, once fixed, replayed safely, because the consumer
// is idempotent.
//
// Retryable failures (a lost database connection, a timeout, anything not
// known to be permanent) are retried in-process while the delivery is
// held, with backoff capped at RetryMax: never a hot loop. After
// MaxAttempts the delivery goes back to the queue, so one stuck message
// cannot hold a slot forever. The quorum queue's delivery limit (20 by
// default in RabbitMQ 4) is the final bound: a message that fails every
// delivery is dead-lettered with reason delivery_limit, and replaying it
// later is safe.
//
// Shutdown: cancelling Run's context stops taking deliveries, gives
// in-flight attempts ShutdownTimeout to finish and settle, and closes the
// connection, which returns every unsettled delivery to the queue.
type UpdateConsumer struct {
	url      string
	topology Topology
	applier  UpdateApplier
	opts     UpdateConsumerOptions
	logger   *slog.Logger
}

// NewUpdateConsumer returns a consumer for the broker at url.
func NewUpdateConsumer(url string, topology Topology, applier UpdateApplier, opts UpdateConsumerOptions, logger *slog.Logger) *UpdateConsumer {
	return &UpdateConsumer{url: url, topology: topology, applier: applier, opts: opts.withDefaults(), logger: logger}
}

// Run consumes until ctx is cancelled, reconnecting with capped
// exponential backoff, and then returns ctx's error.
func (c *UpdateConsumer) Run(ctx context.Context) error {
	delay := c.opts.RetryMin
	for {
		connected, err := c.consume(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if connected {
			delay = c.opts.RetryMin
		}
		c.logger.Warn("update consumer disconnected, reconnecting", "err", err, "retry_in", delay.String())
		if sleepContext(ctx, delay) != nil {
			return ctx.Err()
		}
		delay = min(2*delay, c.opts.RetryMax)
	}
}

// consume runs one connection until ctx is done (nil) or the connection
// fails. connected reports whether it got as far as consuming.
func (c *UpdateConsumer) consume(ctx context.Context) (connected bool, err error) {
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Properties: amqp.Table{"connection_name": appID + " update consumer"},
		Dial:       dialer(ctx),
	})
	if err != nil {
		return false, fmt.Errorf("connect to broker: %w", err)
	}
	// Closing the connection requeues whatever is unsettled.
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("open channel: %w", err)
	}
	if err := c.topology.Declare(ch); err != nil {
		return false, err
	}
	if err := ch.Qos(c.opts.Concurrency, 0, false); err != nil {
		return false, fmt.Errorf("set prefetch: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	deliveries, err := ch.Consume(c.topology.UpdateQueue, "", false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("consume %s: %w", c.topology.UpdateQueue, err)
	}
	c.logger.Info("update consumer connected", "queue", c.topology.UpdateQueue, "concurrency", c.opts.Concurrency)

	// The session ends on shutdown or when the channel is lost; its cause
	// is the error to reconnect on.
	session, end := context.WithCancelCause(ctx)
	defer end(nil)
	var handlers sync.WaitGroup
	for range c.opts.Concurrency {
		handlers.Go(func() {
			for {
				select {
				case <-session.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						end(errors.New("delivery channel closed"))
						return
					}
					// select picks at random among ready cases: never start
					// on a delivery once the session has ended. It is
					// requeued when the connection closes.
					if session.Err() != nil {
						return
					}
					if err := c.handle(session, d); err != nil {
						end(err)
						return
					}
				}
			}
		})
	}

	select {
	case <-session.Done():
	case amqpErr := <-closed:
		// A graceful close delivers no error (a nil *amqp.Error).
		if amqpErr == nil {
			end(errors.New("channel closed"))
		} else {
			end(fmt.Errorf("channel closed: %w", amqpErr))
		}
	}
	// In-flight handlers finish, within ShutdownTimeout, and settle on the
	// still-open channel before the deferred close.
	handlers.Wait()
	if ctx.Err() != nil {
		return true, nil
	}
	return true, context.Cause(session)
}

// handle applies one delivery and settles it. It returns an error only if
// settling failed, meaning the channel is gone.
func (c *UpdateConsumer) handle(ctx context.Context, d amqp.Delivery) error {
	log := c.logger.With("message_id", d.MessageId, "redelivered", d.Redelivered)
	update, err := DecodeTaskUpdateMessage(d.MessageId, d.CorrelationId, d.Body)
	if err != nil {
		log.Error("dead-lettering malformed task update", "err", err, "body", excerpt(d.Body))
		return settledUpdate("reject", d.MessageId, d.Reject(false))
	}
	log = log.With("task_id", update.TaskID, "status", string(update.Status))

	delay := c.opts.RetryMin
	for attempt := 1; ; attempt++ {
		outcome, err := c.apply(ctx, update)
		switch {
		case err == nil:
			logOutcome(log, outcome)
			return settledUpdate("ack", update.ID, d.Ack(false))
		case application.IsPermanentUpdateError(err):
			log.Error("dead-lettering task update that can never be applied", "err", err)
			return settledUpdate("reject", update.ID, d.Reject(false))
		case attempt >= c.opts.MaxAttempts || ctx.Err() != nil:
			log.Warn("task update still failing, requeueing", "attempt", attempt, "err", err)
			return settledUpdate("requeue", update.ID, d.Nack(false, true))
		}
		log.Warn("applying task update failed, retrying", "attempt", attempt, "retry_in", delay.String(), "err", err)
		if sleepContext(ctx, delay) != nil {
			return settledUpdate("requeue", update.ID, d.Nack(false, true))
		}
		delay = min(2*delay, c.opts.RetryMax)
	}
}

// apply runs one attempt. A shutdown does not cut it short at once: it
// gets ShutdownTimeout to finish, so a nearly done transaction is not
// thrown away. Either way the outcome is safe, since an attempt that
// committed but was not acked is redelivered and found to be a duplicate.
func (c *UpdateConsumer) apply(ctx context.Context, update domain.TaskUpdate) (application.UpdateOutcome, error) {
	work, cancel := graceContext(ctx, c.opts.ShutdownTimeout)
	defer cancel()
	work, cancelAttempt := context.WithTimeout(work, c.opts.ApplyTimeout)
	defer cancelAttempt()
	return c.applier.ApplyUpdate(work, update)
}

func logOutcome(log *slog.Logger, outcome application.UpdateOutcome) {
	switch outcome {
	case application.UpdateApplied:
		log.Info("task update applied")
	case application.UpdateDuplicate:
		log.Info("duplicate task update ignored")
	case application.UpdateStale:
		log.Info("stale task update ignored")
	case application.UpdateConflict:
		log.Warn("conflicting task outcome ignored; the first outcome recorded stands")
	default:
		log.Warn("task update settled with unknown outcome", "outcome", string(outcome))
	}
}

func settledUpdate(op, id string, err error) error {
	if err != nil {
		return fmt.Errorf("%s task update %s: %w", op, id, err)
	}
	return nil
}
