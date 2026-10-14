package application

import (
	"context"
	"fmt"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// TaskService implements the task read use cases and applies worker
// progress updates.
type TaskService struct {
	tx    Transactor
	repos Repositories
}

// NewTaskService wires a TaskService.
func NewTaskService(tx Transactor, repos Repositories) *TaskService {
	return &TaskService{tx: tx, repos: repos}
}

// Get returns a single task.
func (s *TaskService) Get(ctx context.Context, id string) (domain.Task, error) {
	return s.repos.Tasks().Get(ctx, id)
}

// List returns one page of tasks.
func (s *TaskService) List(ctx context.Context, filter TaskFilter, page PageRequest) (Page[domain.Task], error) {
	return s.repos.Tasks().List(ctx, filter, page.normalized())
}

// ApplyUpdate applies a worker progress update to its task and, on a
// terminal outcome, to the tenant. It must be idempotent and must never
// regress state when updates arrive out of order.
func (s *TaskService) ApplyUpdate(ctx context.Context, _ domain.TaskUpdate) error {
	err := s.tx.WithinTx(ctx, func(context.Context, Repositories) error {
		return domain.ErrNotImplemented
	})
	return fmt.Errorf("apply task update: %w", err)
}
