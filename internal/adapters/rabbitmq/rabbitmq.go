// Package rabbitmq connects the control plane and the worker simulator
// through RabbitMQ.
//
// Control plane: TaskPublisher (outbound task events, see publisher.go) and
// UpdateConsumer (inbound progress updates, see consumer.go). Worker:
// TaskConsumer and UpdatePublisher (see worker.go). The broker objects are
// described in topology.go and the message formats in messages.go.
package rabbitmq

import (
	"context"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

var (
	_ application.TaskPublisher = (*TaskPublisher)(nil)
	_ worker.UpdatePublisher    = (*UpdatePublisher)(nil)
	_ TaskHandler               = (*worker.Simulator)(nil)
	_ UpdateApplier             = (*application.TaskService)(nil)
)

// TaskHandler processes one task event; *worker.Simulator satisfies it.
type TaskHandler interface {
	Process(ctx context.Context, task domain.Task) error
}
