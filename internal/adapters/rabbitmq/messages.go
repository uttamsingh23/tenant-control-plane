package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// Wire formats. Both are documented as JSON Schemas in docs/schemas and
// checked against them by messages_test.go.
//
// Consumers ignore fields they do not know, so a field can be added
// without a new version; removing or changing one bumps the version.

// ErrMalformedMessage marks a message that can never be processed, no
// matter how often it is redelivered. Consumers dead-letter it.
var ErrMalformedMessage = errors.New("malformed message")

// MaxTaskIDLength bounds the task ID a worker accepts. Update IDs are
// derived from it and must fit the control plane's 200-character
// idempotency key (application.MaxUpdateIDLength).
const MaxTaskIDLength = 100

// TaskMessage is the JSON body of a task event (docs/schemas/task-event.v1.json).
// The task is the event: the body carries the task as it was when the
// event was recorded, and its id is also the AMQP message-id, the stable
// key consumers deduplicate on.
type TaskMessage struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// NewTaskMessage converts a task to its wire form.
func NewTaskMessage(t domain.Task) TaskMessage {
	return TaskMessage{
		ID:        t.ID,
		Type:      string(t.Type),
		TenantID:  t.TenantID,
		Status:    string(t.Status),
		CreatedAt: domain.FormatTimestamp(t.CreatedAt),
		UpdatedAt: domain.FormatTimestamp(t.UpdatedAt),
	}
}

// DecodeTaskMessage parses and validates a task event. messageID is the
// AMQP message-id property; when set, it must equal the body's id. Every
// error wraps ErrMalformedMessage.
func DecodeTaskMessage(messageID string, body []byte) (domain.Task, error) {
	var m TaskMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return domain.Task{}, malformed("task event is not a JSON object: %v", err)
	}
	switch {
	case m.ID == "" || len(m.ID) > MaxTaskIDLength:
		return domain.Task{}, malformed("task event id must be 1-%d characters", MaxTaskIDLength)
	case messageID != "" && messageID != m.ID:
		return domain.Task{}, malformed("task event message-id %q does not match body id %q", messageID, m.ID)
	case !domain.TaskType(m.Type).Valid():
		return domain.Task{}, malformed("task %s: type must be deploy, update or destroy, got %q", m.ID, m.Type)
	case m.TenantID == "":
		return domain.Task{}, malformed("task %s: tenant_id must not be empty", m.ID)
	case domain.TaskStatus(m.Status) != domain.TaskStatusAccepted:
		// The control plane publishes a task once, when it is accepted.
		return domain.Task{}, malformed("task %s: status must be accepted, got %q", m.ID, m.Status)
	}
	createdAt, err := domain.ParseTimestamp(m.CreatedAt)
	if err != nil {
		return domain.Task{}, malformed("task %s: created_at: %v", m.ID, err)
	}
	updatedAt, err := domain.ParseTimestamp(m.UpdatedAt)
	if err != nil {
		return domain.Task{}, malformed("task %s: updated_at: %v", m.ID, err)
	}
	return domain.Task{
		ID:        m.ID,
		Type:      domain.TaskType(m.Type),
		TenantID:  m.TenantID,
		Status:    domain.TaskStatusAccepted,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformedMessage, fmt.Sprintf(format, args...))
}

// UpdateSchemaVersion is the schema_version of TaskUpdateMessage.
const UpdateSchemaVersion = 1

// TaskUpdateEventType is the AMQP type property of progress updates.
const TaskUpdateEventType = "task.update"

// workerAppID is the AMQP app-id property of everything the worker publishes.
const workerAppID = "tenant-worker"

// TaskUpdateMessage is the JSON body of a progress update
// (docs/schemas/task-update.v1.json).
//
// MessageID is the idempotency key, also sent as the AMQP message-id. It
// is stable: redelivering a task to a worker produces the same IDs again.
// PreviousStatus -> Status is the transition being reported, and TenantID
// and TaskType let the consumer check the update against its task.
type TaskUpdateMessage struct {
	SchemaVersion  int    `json:"schema_version"`
	MessageID      string `json:"message_id"`
	TaskID         string `json:"task_id"`
	TenantID       string `json:"tenant_id"`
	TaskType       string `json:"task_type"`
	PreviousStatus string `json:"previous_status"`
	Status         string `json:"status"`
	// Reason explains a failed outcome; omitted otherwise.
	Reason     string `json:"reason,omitempty"`
	OccurredAt string `json:"occurred_at"`
}

// NewTaskUpdateMessage converts an update to its wire form.
func NewTaskUpdateMessage(u domain.TaskUpdate) TaskUpdateMessage {
	return TaskUpdateMessage{
		SchemaVersion:  UpdateSchemaVersion,
		MessageID:      u.ID,
		TaskID:         u.TaskID,
		TenantID:       u.TenantID,
		TaskType:       string(u.TaskType),
		PreviousStatus: string(u.PreviousStatus),
		Status:         string(u.Status),
		Reason:         u.Reason,
		OccurredAt:     domain.FormatTimestamp(u.OccurredAt),
	}
}

// DecodeTaskUpdateMessage parses and validates a progress update against
// docs/schemas/task-update.v1.json. messageID and correlationID are the
// AMQP message-id and correlation-id properties; when set, they must equal
// the body's message_id and task_id. Every error wraps
// ErrMalformedMessage: no redelivery can make such a message valid.
//
// It checks the message on its own. Whether the update fits the task on
// record (same tenant and type, not stale) is for the application to say.
func DecodeTaskUpdateMessage(messageID, correlationID string, body []byte) (domain.TaskUpdate, error) {
	var m TaskUpdateMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return domain.TaskUpdate{}, malformed("task update is not a JSON object: %v", err)
	}
	if m.SchemaVersion != UpdateSchemaVersion {
		return domain.TaskUpdate{}, malformed("task update schema_version must be %d, got %d", UpdateSchemaVersion, m.SchemaVersion)
	}
	switch {
	case m.MessageID == "" || len(m.MessageID) > application.MaxUpdateIDLength:
		return domain.TaskUpdate{}, malformed("task update message_id must be 1-%d characters", application.MaxUpdateIDLength)
	case messageID != "" && messageID != m.MessageID:
		return domain.TaskUpdate{}, malformed("task update message-id %q does not match body message_id %q", messageID, m.MessageID)
	case m.TaskID == "" || len(m.TaskID) > MaxTaskIDLength:
		return domain.TaskUpdate{}, malformed("update %s: task_id must be 1-%d characters", m.MessageID, MaxTaskIDLength)
	case correlationID != "" && correlationID != m.TaskID:
		return domain.TaskUpdate{}, malformed("update %s: correlation-id %q does not match task_id %q", m.MessageID, correlationID, m.TaskID)
	case m.TenantID == "":
		return domain.TaskUpdate{}, malformed("update %s: tenant_id must not be empty", m.MessageID)
	case !domain.TaskType(m.TaskType).Valid():
		return domain.TaskUpdate{}, malformed("update %s: task_type must be deploy, update or destroy, got %q", m.MessageID, m.TaskType)
	}
	previous, status := domain.TaskStatus(m.PreviousStatus), domain.TaskStatus(m.Status)
	switch {
	case status != domain.TaskStatusInProgress && !status.Terminal():
		return domain.TaskUpdate{}, malformed("update %s: status must be in_progress, done or failed, got %q", m.MessageID, m.Status)
	case !previous.CanTransitionTo(status):
		return domain.TaskUpdate{}, malformed("update %s: %q -> %q is not a task transition", m.MessageID, m.PreviousStatus, m.Status)
	case status == domain.TaskStatusFailed && m.Reason == "":
		return domain.TaskUpdate{}, malformed("update %s: a failed update must carry a reason", m.MessageID)
	case status != domain.TaskStatusFailed && m.Reason != "":
		return domain.TaskUpdate{}, malformed("update %s: only a failed update carries a reason", m.MessageID)
	}
	occurredAt, err := domain.ParseTimestamp(m.OccurredAt)
	if err != nil {
		return domain.TaskUpdate{}, malformed("update %s: occurred_at: %v", m.MessageID, err)
	}
	return domain.TaskUpdate{
		ID:             m.MessageID,
		TaskID:         m.TaskID,
		TenantID:       m.TenantID,
		TaskType:       domain.TaskType(m.TaskType),
		PreviousStatus: previous,
		Status:         status,
		Reason:         m.Reason,
		OccurredAt:     occurredAt,
	}, nil
}
