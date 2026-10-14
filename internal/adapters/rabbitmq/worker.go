package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// updatePublishTimeout bounds one update publish, confirmation included.
const updatePublishTimeout = 10 * time.Second

// UpdatePublisher publishes worker progress updates with publisher
// confirms, on its own connection: RabbitMQ throttles publishing
// connections under memory pressure, and that must not stall the
// consumer's acks.
type UpdatePublisher struct {
	pub *confirmPublisher
}

// NewUpdatePublisher returns a publisher for the broker at url. It does
// not connect until the first publish.
func NewUpdatePublisher(url string, topology Topology, logger *slog.Logger) *UpdatePublisher {
	return &UpdatePublisher{pub: newConfirmPublisher(url, workerAppID+" update publisher", topology, logger)}
}

// PublishUpdate implements worker.UpdatePublisher. The update's ID is the
// AMQP message-id and its task ID the correlation-id, so both are visible
// without parsing the body.
func (p *UpdatePublisher) PublishUpdate(ctx context.Context, u domain.TaskUpdate) error {
	body, err := json.Marshal(NewTaskUpdateMessage(u))
	if err != nil {
		return fmt.Errorf("encode update %s: %w", u.ID, err)
	}
	ctx, cancel := context.WithTimeout(ctx, updatePublishTimeout)
	defer cancel()
	return p.pub.publish(ctx, "update", p.pub.topology.UpdateExchange, UpdateRoutingKey(u.Status), amqp.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		MessageId:     u.ID,
		CorrelationId: u.TaskID,
		Type:          TaskUpdateEventType,
		AppId:         workerAppID,
		Timestamp:     u.OccurredAt,
		Body:          body,
	})
}

// Close closes the connection. Later publishes fail with ErrPublisherClosed.
func (p *UpdatePublisher) Close() error { return p.pub.close() }

// TaskConsumerOptions tunes a TaskConsumer. Zero durations other than
// ShutdownTimeout take the defaults below.
type TaskConsumerOptions struct {
	// ShutdownTimeout is how long the in-flight task may keep running
	// once Run's context is cancelled. Past it, the task's context is
	// cancelled and the task requeued. Zero abandons it at once.
	ShutdownTimeout time.Duration
	// RetryMin and RetryMax bound the exponential reconnect backoff.
	// Defaults 500ms and 30s.
	RetryMin time.Duration
	RetryMax time.Duration
	// RequeueDelay is the pause before requeueing a task whose processing
	// failed, so a persistent failure cannot spin. Default 1s.
	RequeueDelay time.Duration
}

func (o TaskConsumerOptions) withDefaults() TaskConsumerOptions {
	if o.RetryMin <= 0 {
		o.RetryMin = 500 * time.Millisecond
	}
	if o.RetryMax < o.RetryMin {
		o.RetryMax = max(30*time.Second, o.RetryMin)
	}
	if o.RequeueDelay <= 0 {
		o.RequeueDelay = time.Second
	}
	return o
}

// TaskConsumer consumes task events and hands them to a TaskHandler.
//
// Reliability contract (at-least-once):
//
//   - Manual acknowledgements, prefetch 1: one task at a time per
//     consumer. Scale out by running more workers, which compete for the
//     queue.
//   - A task is acked only after the handler returns nil, that is after
//     its in_progress and outcome updates have been confirmed by the
//     broker. A crash before the ack makes the broker redeliver it; the
//     rerun publishes the same update IDs, which the control plane
//     deduplicates.
//   - A handler error requeues the task (after RequeueDelay). The quorum
//     queue's delivery limit eventually dead-letters a task that fails
//     every time.
//   - A malformed message is rejected without requeue, so it goes to the
//     dead-letter queue for inspection instead of being retried forever
//     or silently dropped.
//   - A lost connection is re-established with capped exponential
//     backoff; unacked deliveries go back to the queue meanwhile.
type TaskConsumer struct {
	url      string
	topology Topology
	handler  TaskHandler
	opts     TaskConsumerOptions
	logger   *slog.Logger
}

// NewTaskConsumer returns a consumer for the broker at url.
func NewTaskConsumer(url string, topology Topology, handler TaskHandler, opts TaskConsumerOptions, logger *slog.Logger) *TaskConsumer {
	return &TaskConsumer{url: url, topology: topology, handler: handler, opts: opts.withDefaults(), logger: logger}
}

// Run consumes until ctx is cancelled, reconnecting as needed, and then
// returns ctx's error. On cancellation it stops taking deliveries, lets
// the in-flight task finish within ShutdownTimeout, and closes the
// connection, which returns any unacked delivery to the queue.
func (c *TaskConsumer) Run(ctx context.Context) error {
	delay := c.opts.RetryMin
	for {
		connected, err := c.consume(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if connected {
			delay = c.opts.RetryMin
		}
		c.logger.Warn("task consumer disconnected, reconnecting", "err", err, "retry_in", delay.String())
		if sleepContext(ctx, delay) != nil {
			return ctx.Err()
		}
		delay = min(2*delay, c.opts.RetryMax)
	}
}

// consume runs one connection until ctx is done (nil) or the connection
// fails. connected reports whether it got as far as consuming.
func (c *TaskConsumer) consume(ctx context.Context) (connected bool, err error) {
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Properties: amqp.Table{"connection_name": workerAppID + " task consumer"},
		Dial:       dialer(ctx),
	})
	if err != nil {
		return false, fmt.Errorf("connect to broker: %w", err)
	}
	// Closing the connection requeues whatever is unacked.
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("open channel: %w", err)
	}
	if err := c.topology.Declare(ch); err != nil {
		return false, err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return false, fmt.Errorf("set prefetch: %w", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	deliveries, err := ch.Consume(c.topology.Queue, "", false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("consume %s: %w", c.topology.Queue, err)
	}
	c.logger.Info("task consumer connected", "queue", c.topology.Queue)

	for {
		select {
		case <-ctx.Done():
			return true, nil
		case amqpErr := <-closed:
			// A graceful close delivers no error (a nil *amqp.Error).
			if amqpErr == nil {
				return true, errors.New("channel closed")
			}
			return true, fmt.Errorf("channel closed: %w", amqpErr)
		case d, ok := <-deliveries:
			if !ok {
				return true, errors.New("delivery channel closed")
			}
			// select picks at random among ready cases: never start a
			// task once shutdown has begun. The delivery is requeued
			// when the connection closes.
			if ctx.Err() != nil {
				return true, nil
			}
			if err := c.handle(ctx, d); err != nil {
				return true, err
			}
		}
	}
}

// handle processes one delivery and settles it. It returns an error only
// if settling failed, meaning the channel is gone.
func (c *TaskConsumer) handle(ctx context.Context, d amqp.Delivery) error {
	log := c.logger.With("message_id", d.MessageId, "redelivered", d.Redelivered)
	task, err := DecodeTaskMessage(d.MessageId, d.Body)
	if err != nil {
		log.Error("dead-lettering malformed task message", "err", err, "body", excerpt(d.Body))
		return settled("reject", d.MessageId, d.Reject(false))
	}

	work, cancel := graceContext(ctx, c.opts.ShutdownTimeout)
	err = c.handler.Process(work, task)
	cancel()
	if err == nil {
		return settled("ack", task.ID, d.Ack(false))
	}

	log.Warn("task processing failed, requeueing", "task_id", task.ID, "err", err)
	if ctx.Err() == nil {
		_ = sleepContext(ctx, c.opts.RequeueDelay)
	}
	return settled("requeue", task.ID, d.Nack(false, true))
}

func settled(op, id string, err error) error {
	if err != nil {
		return fmt.Errorf("%s task message %s: %w", op, id, err)
	}
	return nil
}

// graceContext returns the context for one task's work. Cancelling parent
// does not cancel it straight away: the work gets grace more time to
// finish, so a shutdown does not throw away a task that is nearly done.
func graceContext(parent context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	work, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, func() {
		t := time.NewTimer(grace)
		defer t.Stop()
		select {
		case <-t.C:
			cancel()
		case <-work.Done():
		}
	})
	return work, func() {
		stop()
		cancel()
	}
}

// excerpt returns the start of a message body for logging.
func excerpt(body []byte) string {
	const limit = 512
	if len(body) > limit {
		return string(body[:limit]) + "..."
	}
	return string(body)
}

// sleepContext waits for d or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
