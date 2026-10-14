package domain

import "time"

// TaskType is the kind of operation a task performs on its tenant.
type TaskType string

// Task types.
const (
	TaskTypeDeploy  TaskType = "deploy"
	TaskTypeUpdate  TaskType = "update"
	TaskTypeDestroy TaskType = "destroy"
)

// Valid reports whether t is a known task type.
func (t TaskType) Valid() bool {
	switch t {
	case TaskTypeDeploy, TaskTypeUpdate, TaskTypeDestroy:
		return true
	}
	return false
}

// TaskStatus is a state in the task state machine.
type TaskStatus string

// Task states. TaskStatusDone and TaskStatusFailed are terminal.
const (
	TaskStatusAccepted   TaskStatus = "accepted"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusDone       TaskStatus = "done"
	TaskStatusFailed     TaskStatus = "failed"
)

// Valid reports whether s is a known task status.
func (s TaskStatus) Valid() bool {
	switch s {
	case TaskStatusAccepted, TaskStatusInProgress, TaskStatusDone, TaskStatusFailed:
		return true
	}
	return false
}

// taskTransitions is the single source of truth for the task state machine:
//
//	accepted    -> in_progress
//	in_progress -> done | failed
//	done, failed: terminal, no outgoing edges
//
// Terminal states are absent from the map, so nothing can leave them and a
// terminal task can never regress. Self-transitions are not listed either:
// a repeated status is a duplicate or stale delivery, and the caller decides
// whether to ignore it.
var taskTransitions = map[TaskStatus][]TaskStatus{
	TaskStatusAccepted:   {TaskStatusInProgress},
	TaskStatusInProgress: {TaskStatusDone, TaskStatusFailed},
}

// Terminal reports whether s is a final state.
func (s TaskStatus) Terminal() bool {
	return s == TaskStatusDone || s == TaskStatusFailed
}

// Open reports whether a task in status s is still pending or running. The
// tenant status guards make sure a tenant has at most one open task.
func (s TaskStatus) Open() bool {
	return s == TaskStatusAccepted || s == TaskStatusInProgress
}

// CanTransitionTo reports whether the task state machine allows s -> next.
func (s TaskStatus) CanTransitionTo(next TaskStatus) bool {
	for _, allowed := range taskTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Task is an asynchronous operation on a tenant. A task is the published
// event, persisted: its ID doubles as the event ID.
type Task struct {
	ID        string
	Type      TaskType
	TenantID  string
	Status    TaskStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewTask returns an accepted task of type typ for tenant tenantID. The
// caller supplies id (from its identifier generator) and now (from its
// clock), which keeps the domain deterministic.
func NewTask(id string, typ TaskType, tenantID string, now time.Time) (Task, error) {
	if id == "" {
		return Task{}, &ValidationError{Field: "id", Reason: "must not be empty"}
	}
	if !typ.Valid() {
		return Task{}, &ValidationError{Field: "type", Reason: "must be one of deploy, update, destroy"}
	}
	if tenantID == "" {
		return Task{}, &ValidationError{Field: "tenant_id", Reason: "must not be empty"}
	}
	now = NormalizeTime(now)
	return Task{
		ID:        id,
		Type:      typ,
		TenantID:  tenantID,
		Status:    TaskStatusAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// TransitionTo moves the task to next if the state machine allows it. On a
// forbidden move it returns a *TransitionError and leaves the task unchanged.
func (t *Task) TransitionTo(next TaskStatus, now time.Time) error {
	if !t.Status.CanTransitionTo(next) {
		return &TransitionError{Entity: "task", From: string(t.Status), Trigger: string(next)}
	}
	t.Status = next
	t.UpdatedAt = NormalizeTime(now)
	return nil
}

// TaskUpdate is a progress report from a worker about a single task.
type TaskUpdate struct {
	// ID is assigned by the worker and is stable across redeliveries.
	// It is the idempotency key for applying the update.
	ID     string
	TaskID string
	// TenantID and TaskType echo the task the update is about, so a
	// consumer can check the update against the task it has on record.
	TenantID string
	TaskType TaskType
	// PreviousStatus is the status the worker moved the task from; with
	// Status it names the transition the update reports.
	PreviousStatus TaskStatus
	Status         TaskStatus
	// Reason optionally explains a failed outcome.
	Reason     string
	OccurredAt time.Time
}
