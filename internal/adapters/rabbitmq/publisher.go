package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// TaskEventType is the AMQP type property of task events.
const TaskEventType = "task"

// appID is the AMQP app-id property of everything the control plane publishes.
const appID = "tenant-control-plane"

// defaultDialTimeout bounds the TCP and AMQP handshake when the caller's
// context has no deadline.
const defaultDialTimeout = 30 * time.Second

// ErrPublisherClosed is returned by a publish after Close.
var ErrPublisherClosed = errors.New("publisher closed")

// TaskPublisher publishes task events to RabbitMQ with publisher confirms.
//
// PublishTask returns nil only once the broker has confirmed the message,
// which for a durable queue means it has been written to disk. The
// connection is opened on first use and reopened after any failure, so a
// broker that is down when the control plane starts, or restarts later,
// only causes publish errors (which the relay retries) until it is back.
type TaskPublisher struct {
	pub *confirmPublisher
}

// NewTaskPublisher returns a publisher for the broker at url. It does not
// connect until the first publish.
func NewTaskPublisher(url string, topology Topology, logger *slog.Logger) *TaskPublisher {
	return &TaskPublisher{pub: newConfirmPublisher(url, appID+" task publisher", topology, logger)}
}

// PublishTask implements application.TaskPublisher. Every error is worth
// retrying: none of them means the event can never be delivered.
func (p *TaskPublisher) PublishTask(ctx context.Context, task domain.Task) error {
	body, err := json.Marshal(NewTaskMessage(task))
	if err != nil {
		return fmt.Errorf("encode task %s: %w", task.ID, err)
	}
	return p.pub.publish(ctx, "task", p.pub.topology.Exchange, TaskRoutingKey(task.Type), amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    task.ID,
		Type:         TaskEventType,
		AppId:        appID,
		Timestamp:    task.UpdatedAt,
		Body:         body,
	})
}

// Close closes the connection. Later publishes fail with ErrPublisherClosed.
func (p *TaskPublisher) Close() error { return p.pub.close() }

// confirmPublisher publishes messages one at a time and waits for the
// broker's confirmation of each. It is shared by the control plane's
// TaskPublisher and the worker's UpdatePublisher.
//
// Publishes are serialized on one channel. That keeps confirm and return
// handling simple (one message in flight) at the cost of throughput,
// which is ample for provisioning traffic.
type confirmPublisher struct {
	url      string
	connName string
	topology Topology
	logger   *slog.Logger

	mu      sync.Mutex
	closed  bool
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns chan amqp.Return
}

func newConfirmPublisher(url, connName string, topology Topology, logger *slog.Logger) *confirmPublisher {
	return &confirmPublisher{url: url, connName: connName, topology: topology, logger: logger}
}

// publish sends msg and returns nil only once the broker has confirmed it.
// kind ("task", "update") names the message in errors.
//
// The message is published with the mandatory flag, so an unroutable
// message (say, the queue was deleted) is returned by the broker and
// reported as an error instead of being acknowledged and dropped.
func (p *confirmPublisher) publish(ctx context.Context, kind, exchange, key string, msg amqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPublisherClosed
	}
	ch, err := p.channel(ctx)
	if err != nil {
		return err
	}

	id := msg.MessageId
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key,
		true,  // mandatory: return the message if no queue is bound
		false, // immediate: unsupported by RabbitMQ
		msg)
	if err != nil {
		p.reset()
		return fmt.Errorf("publish %s %s: %w", kind, id, err)
	}
	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		// The outcome is unknown: the broker may still store the message.
		// Drop the channel so a late confirmation cannot be mistaken for
		// the next message's; the caller retries and the duplicate, if
		// any, carries the same message-id.
		p.reset()
		return fmt.Errorf("wait for confirmation of %s %s: %w", kind, id, err)
	}
	if !acked {
		if ch.IsClosed() {
			// Closing a channel settles its pending confirmations as nacks.
			p.reset()
			return fmt.Errorf("publish %s %s: channel closed before confirmation", kind, id)
		}
		return fmt.Errorf("publish %s %s: broker rejected the message (nack)", kind, id)
	}
	// The broker sends basic.return before the basic.ack of the same
	// message, and the client delivers it to p.returns synchronously, so a
	// return for this message is already buffered here.
	for {
		select {
		case ret := <-p.returns:
			if ret.MessageId == id {
				// Reconnect next time, which declares the topology again
				// and so restores a deleted queue or binding.
				p.reset()
				return fmt.Errorf("publish %s %s: unroutable (%d %s): no queue bound to exchange %s for key %s",
					kind, id, ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey)
			}
		default:
			return nil
		}
	}
}

// channel returns an open confirm-mode channel, connecting and declaring
// the topology first if needed. The caller holds p.mu.
func (p *confirmPublisher) channel(ctx context.Context) (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() && !p.conn.IsClosed() {
		return p.ch, nil
	}
	p.reset()

	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Properties: amqp.Table{"connection_name": p.connName},
		Dial:       dialer(ctx),
	})
	if err != nil {
		return nil, fmt.Errorf("connect to broker: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	if err := p.topology.Declare(ch); err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Buffered so the client's reader never blocks on a return: with one
	// message in flight, at most one return is pending at a time.
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	p.conn, p.ch = conn, ch
	p.logger.Info("publisher connected to broker", "connection", p.connName)
	return ch, nil
}

// reset closes the connection, if any. The caller holds p.mu.
func (p *confirmPublisher) reset() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn, p.ch, p.returns = nil, nil, nil
}

// close closes the connection. Later publishes fail with ErrPublisherClosed.
func (p *confirmPublisher) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.reset()
	return nil
}

// dialer connects within ctx and bounds the AMQP handshake by ctx's
// deadline, so connecting to an unresponsive broker cannot outlast the
// publish timeout. The client clears the deadline once connected.
func dialer(ctx context.Context) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Now().Add(defaultDialTimeout)
		}
		d := net.Dialer{Deadline: deadline}
		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return conn, nil
	}
}
