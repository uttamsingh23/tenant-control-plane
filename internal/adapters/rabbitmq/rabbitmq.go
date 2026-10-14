// Package rabbitmq connects the control plane and the worker simulator
// through RabbitMQ.
//
// Control plane: TaskPublisher (outbound task events, see publisher.go) and
// UpdateConsumer (inbound progress updates). Worker: TaskConsumer and
// UpdatePublisher (see worker.go). The broker objects are described in
// topology.go and the message formats in messages.go.
package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

var (
	_ application.TaskPublisher = (*TaskPublisher)(nil)
	_ worker.UpdatePublisher    = (*UpdatePublisher)(nil)
	_ TaskHandler               = (*worker.Simulator)(nil)
)

func notImplemented(op string) error {
	return fmt.Errorf("rabbitmq %s: %w", op, domain.ErrNotImplemented)
}

// UpdateApplier applies a worker progress update; *application.TaskService
// satisfies it.
type UpdateApplier interface {
	ApplyUpdate(ctx context.Context, update domain.TaskUpdate) (application.UpdateOutcome, error)
}

// TaskHandler processes one task event; *worker.Simulator satisfies it.
type TaskHandler interface {
	Process(ctx context.Context, task domain.Task) error
}

// UpdateConsumer consumes worker progress updates and hands them to an
// UpdateApplier.
type UpdateConsumer struct {
	url     string
	applier UpdateApplier
	logger  *slog.Logger
}

// NewUpdateConsumer returns a consumer for the broker at url.
func NewUpdateConsumer(url string, applier UpdateApplier, logger *slog.Logger) *UpdateConsumer {
	return &UpdateConsumer{url: url, applier: applier, logger: logger}
}

// Run consumes until ctx is cancelled.
func (c *UpdateConsumer) Run(context.Context) error {
	return notImplemented("consume task updates")
}
