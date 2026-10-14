package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// Topology names the broker objects between the control plane and the
// worker.
//
//	control plane --task.<type>--------> Exchange (topic) --task.#--> Queue (quorum) --> worker
//	worker --task_update.<status>------> UpdateExchange (topic) --task_update.#--> UpdateQueue (quorum) --> control plane
//
//	Queue, UpdateQueue --rejected or over delivery limit--> DeadLetterExchange (fanout) --> DeadLetterQueue
//
// Every object is durable and every queue is a quorum queue.
type Topology struct {
	// Exchange is the topic exchange task events are published to.
	Exchange string
	// Queue is the queue the worker consumes task events from.
	Queue string
	// UpdateExchange is the topic exchange worker progress updates are
	// published to.
	UpdateExchange string
	// UpdateQueue is the queue the control plane consumes updates from.
	UpdateQueue string
	// DeadLetterExchange receives messages a consumer rejected as
	// unprocessable, and messages over the queue's delivery limit.
	DeadLetterExchange string
	// DeadLetterQueue keeps dead-lettered messages for inspection.
	DeadLetterQueue string
}

// DefaultTopology is the topology used by the control plane and the worker.
func DefaultTopology() Topology {
	return Topology{
		Exchange:           "tenant.tasks",
		Queue:              "tenant.tasks.worker",
		UpdateExchange:     "tenant.task-updates",
		UpdateQueue:        "tenant.task-updates.control-plane",
		DeadLetterExchange: "tenant.dead-letter",
		DeadLetterQueue:    "tenant.dead-letter",
	}
}

const (
	// taskBindingKey binds the worker queue to events of every task type.
	taskBindingKey = "task.#"
	// updateBindingKey binds the control-plane queue to every update.
	updateBindingKey = "task_update.#"
)

// TaskRoutingKey is the routing key of a task event, e.g. "task.deploy".
// A consumer interested in one task type can bind to just that key.
func TaskRoutingKey(t domain.TaskType) string { return "task." + string(t) }

// UpdateRoutingKey is the routing key of a progress update, e.g.
// "task_update.done".
func UpdateRoutingKey(s domain.TaskStatus) string { return "task_update." + string(s) }

// Declare creates the topology if it does not exist. Declarations are
// idempotent, so both sides declare the whole topology on every
// (re)connect and neither depends on the other having started first:
// messages published before their consumer has ever run are kept in its
// queue rather than being unroutable.
//
// The queues are quorum queues: replicated and always durable, the
// RabbitMQ 4 recommendation for data that must not be lost. A quorum
// queue also counts deliveries and, past its delivery limit (20 by
// default in RabbitMQ 4), dead-letters a message that keeps being
// requeued, so a message that crashes its consumer cannot loop forever.
//
// Declaring an existing queue with different arguments fails, and that
// failure is returned rather than worked around.
func (t Topology) Declare(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(t.DeadLetterExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", t.DeadLetterExchange, err)
	}
	if err := declareQueue(ch, t.DeadLetterQueue, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(t.DeadLetterQueue, "", t.DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind queue %s to %s: %w", t.DeadLetterQueue, t.DeadLetterExchange, err)
	}
	deadLetter := amqp.Table{"x-dead-letter-exchange": t.DeadLetterExchange}
	for _, route := range []struct{ exchange, queue, key string }{
		{t.Exchange, t.Queue, taskBindingKey},
		{t.UpdateExchange, t.UpdateQueue, updateBindingKey},
	} {
		if err := ch.ExchangeDeclare(route.exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", route.exchange, err)
		}
		if err := declareQueue(ch, route.queue, deadLetter); err != nil {
			return err
		}
		if err := ch.QueueBind(route.queue, route.key, route.exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %s to %s: %w", route.queue, route.exchange, err)
		}
	}
	return nil
}

func declareQueue(ch *amqp.Channel, name string, args amqp.Table) error {
	all := amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeQuorum}
	for k, v := range args {
		all[k] = v
	}
	if _, err := ch.QueueDeclare(name, true, false, false, false, all); err != nil {
		return fmt.Errorf("declare queue %s: %w", name, err)
	}
	return nil
}
