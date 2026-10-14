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

// TaskStatus is a state in the task state machine.
type TaskStatus string

// Task states. TaskStatusDone and TaskStatusFailed are terminal.
const (
	TaskStatusAccepted   TaskStatus = "accepted"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusDone       TaskStatus = "done"
	TaskStatusFailed     TaskStatus = "failed"
)

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

// TaskUpdate is a progress report from a worker about a single task.
type TaskUpdate struct {
	// ID is assigned by the worker and is stable across redeliveries.
	// It is the idempotency key for applying the update.
	ID     string
	TaskID string
	Status TaskStatus
	// Reason optionally explains a failed outcome.
	Reason     string
	OccurredAt time.Time
}
