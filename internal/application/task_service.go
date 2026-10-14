package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// MaxUpdateIDLength bounds the idempotency key of a worker update; it
// matches the processed_updates column constraint.
const MaxUpdateIDLength = 200

// UpdateOutcome says what ApplyUpdate did with a worker update. Every
// outcome is a success: the message has been dealt with.
type UpdateOutcome string

// Update outcomes.
const (
	// UpdateApplied: the task, and on a terminal outcome the tenant, changed.
	UpdateApplied UpdateOutcome = "applied"
	// UpdateDuplicate: the update's ID was applied before; nothing changed.
	UpdateDuplicate UpdateOutcome = "duplicate"
	// UpdateStale: the task already reached or passed the reported status,
	// for example in_progress arriving after done, or a second done; nothing
	// changed.
	UpdateStale UpdateOutcome = "stale"
	// UpdateConflict: a terminal outcome that contradicts the one already
	// recorded (failed after done, or done after failed); nothing changed.
	// The first outcome recorded wins. It is reported apart from
	// UpdateStale because it means a worker produced two different results
	// for one task, which is worth a warning.
	UpdateConflict UpdateOutcome = "conflict"
)

// TaskService implements the task read use cases and applies worker
// progress updates.
type TaskService struct {
	tx    Transactor
	repos Repositories
	now   Clock
}

// NewTaskService wires a TaskService. repos is used for reads outside a
// transaction; tx for applying updates.
func NewTaskService(tx Transactor, repos Repositories, now Clock) *TaskService {
	return &TaskService{tx: tx, repos: repos, now: now}
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
// terminal outcome, to the tenant, in one transaction:
//
//  1. Record update.ID as processed. A key seen before is a redelivery:
//     UpdateDuplicate. Concurrent deliveries of one key are serialized by
//     the repository, so only one of them proceeds.
//  2. Lock the task row, so updates for one task apply one at a time, and
//     check that the update's tenant and task type match the task.
//  3. Advance the task. A status the task has reached or passed is stale:
//     UpdateStale (UpdateConflict for a contradicting terminal outcome),
//     and only the processed key is committed.
//  4. On done or failed, move the tenant with an optimistic-locked write,
//     which increments its version.
//
// Any error rolls back every step, the processed key included, so an
// update that cannot be applied leaves both records untouched and can be
// retried or replayed. IsPermanentUpdateError tells the errors retrying
// cannot fix from those it might.
func (s *TaskService) ApplyUpdate(ctx context.Context, update domain.TaskUpdate) (UpdateOutcome, error) {
	if err := validateUpdate(update); err != nil {
		return "", fmt.Errorf("apply task update: %w", err)
	}
	var outcome UpdateOutcome
	err := s.tx.WithinTx(ctx, func(ctx context.Context, repos Repositories) error {
		fresh, err := repos.ProcessedUpdates().MarkProcessed(ctx, update.ID)
		if err != nil {
			return err
		}
		if !fresh {
			outcome = UpdateDuplicate
			return nil
		}
		task, err := repos.Tasks().GetForUpdate(ctx, update.TaskID)
		if err != nil {
			return err
		}
		if err := checkUpdateMatchesTask(update, task); err != nil {
			return err
		}
		now := s.now()
		if !advanceTask(&task, update.Status, now) {
			outcome = UpdateStale
			if task.Status.Terminal() && update.Status.Terminal() && task.Status != update.Status {
				outcome = UpdateConflict
			}
			return nil
		}
		if err := repos.Tasks().Update(ctx, task); err != nil {
			return err
		}
		if task.Status.Terminal() {
			if err := applyTaskOutcome(ctx, repos, task, now); err != nil {
				return err
			}
		}
		outcome = UpdateApplied
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("apply update %s to task %s: %w", update.ID, update.TaskID, err)
	}
	return outcome, nil
}

// IsPermanentUpdateError reports whether an ApplyUpdate error is one that
// retrying the same update cannot fix:
//
//   - domain.ErrInvalidInput: the update is malformed, or names a tenant
//     or task type other than its task's.
//   - domain.ErrTaskNotFound: no such task. Tasks are committed before
//     their event is published, so this is not a race that resolves
//     itself (see the consumer's poison-message policy).
//   - domain.ErrTenantNotFound, domain.ErrInvalidTransition: the tenant is
//     missing or not waiting for this outcome, which the status guards
//     rule out; the records disagree and need a human.
//
// Everything else, such as a lost database connection, a timeout or a
// tenant version conflict, may succeed on retry. Unknown errors count as
// retryable, so an outage is never mistaken for a bad message.
func IsPermanentUpdateError(err error) bool {
	for _, permanent := range []error{
		domain.ErrInvalidInput,
		domain.ErrTaskNotFound,
		domain.ErrTenantNotFound,
		domain.ErrInvalidTransition,
	} {
		if errors.Is(err, permanent) {
			return true
		}
	}
	return false
}

// validateUpdate rejects updates no task could accept. Workers report
// in_progress, done or failed; accepted is set only by the control plane.
// PreviousStatus -> Status must be an edge of the task state machine.
func validateUpdate(u domain.TaskUpdate) error {
	switch {
	case u.ID == "" || len(u.ID) > MaxUpdateIDLength:
		return &domain.ValidationError{Field: "update id", Reason: fmt.Sprintf("must be 1-%d characters", MaxUpdateIDLength)}
	case u.TaskID == "":
		return &domain.ValidationError{Field: "task_id", Reason: "must not be empty"}
	case u.TenantID == "":
		return &domain.ValidationError{Field: "tenant_id", Reason: "must not be empty"}
	case !u.TaskType.Valid():
		return &domain.ValidationError{Field: "task_type", Reason: fmt.Sprintf("must be deploy, update or destroy, got %q", u.TaskType)}
	case u.Status != domain.TaskStatusInProgress && !u.Status.Terminal():
		return &domain.ValidationError{Field: "status", Reason: fmt.Sprintf("must be in_progress, done or failed, got %q", u.Status)}
	case !u.PreviousStatus.CanTransitionTo(u.Status):
		return &domain.ValidationError{Field: "previous_status", Reason: fmt.Sprintf("%q -> %q is not a task transition", u.PreviousStatus, u.Status)}
	}
	return nil
}

// checkUpdateMatchesTask rejects an update that names the right task ID
// but the wrong tenant or task type: it was built from some other record,
// and applying it could move the wrong tenant.
func checkUpdateMatchesTask(u domain.TaskUpdate, task domain.Task) error {
	if u.TenantID != task.TenantID {
		return &domain.ValidationError{Field: "tenant_id", Reason: fmt.Sprintf("task %s belongs to tenant %s, update says %s", task.ID, task.TenantID, u.TenantID)}
	}
	if u.TaskType != task.Type {
		return &domain.ValidationError{Field: "task_type", Reason: fmt.Sprintf("task %s is a %s task, update says %s", task.ID, task.Type, u.TaskType)}
	}
	return nil
}

// advanceTask moves task to status and reports false, leaving the task
// unchanged, if the update is stale. A terminal outcome can overtake its
// in_progress report on the broker; it implies the work started, so the
// task steps through in_progress rather than dropping the outcome and
// staying accepted forever. The late in_progress is then stale.
func advanceTask(task *domain.Task, status domain.TaskStatus, now time.Time) bool {
	next := *task
	if next.Status == domain.TaskStatusAccepted && status.Terminal() {
		if next.TransitionTo(domain.TaskStatusInProgress, now) != nil {
			return false
		}
	}
	if next.TransitionTo(status, now) != nil {
		return false
	}
	*task = next
	return true
}

// applyTaskOutcome moves the tenant of a task that just became terminal.
// The tenant's status guards allow only one open task per tenant, so the
// tenant should be waiting for exactly this outcome; if it is not, the
// domain error rolls back the task change too.
func applyTaskOutcome(ctx context.Context, repos Repositories, task domain.Task, now time.Time) error {
	tenant, err := repos.Tenants().Get(ctx, task.TenantID)
	if err != nil {
		return err
	}
	readVersion := tenant.Version
	if err := tenant.ApplyTaskOutcome(task, now); err != nil {
		return err
	}
	return repos.Tenants().Update(ctx, tenant, readVersion)
}
