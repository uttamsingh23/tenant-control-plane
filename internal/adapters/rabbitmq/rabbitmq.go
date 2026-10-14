// Package rabbitmq connects the control plane and the worker simulator
// through RabbitMQ.
//
// Control plane: TaskPublisher (outbound task events) and UpdateConsumer
// (inbound progress updates). Worker: TaskConsumer and UpdatePublisher.
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

// TaskPublisher publishes task events for workers.
type TaskPublisher struct {
	url string
}

// NewTaskPublisher returns a publisher for the broker at url.
func NewTaskPublisher(url string) *TaskPublisher {
	return &TaskPublisher{url: url}
}

// PublishTask implements application.TaskPublisher.
func (p *TaskPublisher) PublishTask(context.Context, domain.Task) error {
	return notImplemented("publish task")
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

// TaskConsumer consumes task events and hands them to a TaskHandler.
type TaskConsumer struct {
	url     string
	handler TaskHandler
	logger  *slog.Logger
}

// NewTaskConsumer returns a consumer for the broker at url.
func NewTaskConsumer(url string, handler TaskHandler, logger *slog.Logger) *TaskConsumer {
	return &TaskConsumer{url: url, handler: handler, logger: logger}
}

// Run consumes until ctx is cancelled.
func (c *TaskConsumer) Run(context.Context) error {
	return notImplemented("consume tasks")
}

// UpdatePublisher publishes worker progress updates.
type UpdatePublisher struct {
	url string
}

// NewUpdatePublisher returns a publisher for the broker at url.
func NewUpdatePublisher(url string) *UpdatePublisher {
	return &UpdatePublisher{url: url}
}

// PublishUpdate implements worker.UpdatePublisher.
func (p *UpdatePublisher) PublishUpdate(context.Context, domain.TaskUpdate) error {
	return notImplemented("publish task update")
}
