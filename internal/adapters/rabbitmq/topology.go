package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// Topology names the broker objects that carry task events.
//
//	control plane --(routing key task.<type>)--> Exchange (topic, durable)
//	                                                 | binding task.#
//	                                                 v
//	                                   Queue (quorum, durable) --> worker
type Topology struct {
	// Exchange is the durable topic exchange task events are published to.
	Exchange string
	// Queue is the durable quorum queue the worker consumes task events from.
	Queue string
}

// DefaultTopology is the topology used by the control plane and the worker.
func DefaultTopology() Topology {
	return Topology{Exchange: "tenant.tasks", Queue: "tenant.tasks.worker"}
}

// taskBindingKey binds the worker queue to events of every task type.
const taskBindingKey = "task.#"

// TaskRoutingKey is the routing key of a task event, e.g. "task.deploy".
// A consumer interested in one task type can bind to just that key.
func TaskRoutingKey(t domain.TaskType) string { return "task." + string(t) }

// Declare creates the topology if it does not exist. Declarations are
// idempotent, so both sides declare on every (re)connect and neither
// depends on the other having started first. The publisher declaring the
// queue matters: messages published before any worker has started are
// kept in the queue rather than being unroutable.
//
// The queue is a quorum queue: replicated and always durable, the
// RabbitMQ 4 recommendation for data that must not be lost. Declaring an
// existing queue with different arguments fails, and that failure is
// returned rather than worked around.
func (t Topology) Declare(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(t.Exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", t.Exchange, err)
	}
	args := amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeQuorum}
	if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue %s: %w", t.Queue, err)
	}
	if err := ch.QueueBind(t.Queue, taskBindingKey, t.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue %s to %s: %w", t.Queue, t.Exchange, err)
	}
	return nil
}
