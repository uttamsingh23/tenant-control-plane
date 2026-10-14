package rabbitmq_test

// Unit tests of the wire formats: the encoders' output is checked against
// the JSON Schemas in docs/schemas, so documentation and code cannot drift.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

const (
	taskEventSchema  = "../../../docs/schemas/task-event.v1.json"
	taskUpdateSchema = "../../../docs/schemas/task-update.v1.json"
)

func compileSchema(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	sch, err := jsonschema.NewCompiler().Compile(path)
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return sch
}

func validate(t *testing.T, sch *jsonschema.Schema, doc []byte) error {
	t.Helper()
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		t.Fatalf("parse %s: %v", doc, err)
	}
	return sch.Validate(v)
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestTaskUpdateMessageGolden pins the exact field names and formats.
func TestTaskUpdateMessageGolden(t *testing.T) {
	got := marshal(t, rabbitmq.NewTaskUpdateMessage(domain.TaskUpdate{
		ID:             "t-1:outcome",
		TaskID:         "t-1",
		TenantID:       "tn-1",
		TaskType:       domain.TaskTypeDestroy,
		PreviousStatus: domain.TaskStatusInProgress,
		Status:         domain.TaskStatusFailed,
		Reason:         "simulated destroy failure",
		OccurredAt:     created,
	}))
	want := `{"schema_version":1,"message_id":"t-1:outcome","task_id":"t-1","tenant_id":"tn-1",` +
		`"task_type":"destroy","previous_status":"in_progress","status":"failed",` +
		`"reason":"simulated destroy failure","occurred_at":"2026-01-02T03:04:05.006Z"}`
	if string(got) != want {
		t.Errorf("encoded update\n got %s\nwant %s", got, want)
	}

	// reason is omitted unless the task failed.
	done := marshal(t, rabbitmq.NewTaskUpdateMessage(domain.TaskUpdate{
		ID: "t-1:outcome", TaskID: "t-1", Status: domain.TaskStatusDone, OccurredAt: created,
	}))
	if strings.Contains(string(done), "reason") {
		t.Errorf("done update carries a reason: %s", done)
	}
}

// capture encodes updates as the UpdatePublisher would.
type capture struct{ bodies [][]byte }

func (c *capture) PublishUpdate(_ context.Context, u domain.TaskUpdate) error {
	b, err := json.Marshal(rabbitmq.NewTaskUpdateMessage(u))
	c.bodies = append(c.bodies, b)
	return err
}

// TestSimulatedLifecycleMatchesSchema: the updates a real simulator emits
// for every task type and outcome validate against the schema, and their
// transitions chain from accepted to a terminal status.
func TestSimulatedLifecycleMatchesSchema(t *testing.T) {
	sch := compileSchema(t, taskUpdateSchema)
	for _, rate := range []float64{0, 1} {
		for _, typ := range []domain.TaskType{domain.TaskTypeDeploy, domain.TaskTypeUpdate, domain.TaskTypeDestroy} {
			c := &capture{}
			sim, err := worker.NewSimulator(config.Simulation{FailRate: rate}, c, rand.New(rand.NewPCG(1, 1)),
				slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			task := newTask(t, typ)
			if err := sim.Process(t.Context(), task); err != nil {
				t.Fatal(err)
			}
			if len(c.bodies) != 2 {
				t.Fatalf("%s at fail rate %v: %d updates, want 2", typ, rate, len(c.bodies))
			}
			status := domain.TaskStatusAccepted
			for _, body := range c.bodies {
				if err := validate(t, sch, body); err != nil {
					t.Errorf("update %s does not match the schema: %v", body, err)
				}
				var m rabbitmq.TaskUpdateMessage
				if err := json.Unmarshal(body, &m); err != nil {
					t.Fatal(err)
				}
				if m.TaskID != task.ID || m.TenantID != task.TenantID || m.TaskType != string(typ) {
					t.Errorf("update %s does not reference task %s of tenant %s", body, task.ID, task.TenantID)
				}
				if domain.TaskStatus(m.PreviousStatus) != status || !status.CanTransitionTo(domain.TaskStatus(m.Status)) {
					t.Errorf("update %s: %s -> %s does not follow %s", m.MessageID, m.PreviousStatus, m.Status, status)
				}
				status = domain.TaskStatus(m.Status)
			}
			if want := map[float64]domain.TaskStatus{0: domain.TaskStatusDone, 1: domain.TaskStatusFailed}[rate]; status != want {
				t.Errorf("%s at fail rate %v ends %s, want %s", typ, rate, status, want)
			}
		}
	}
}

// TestUpdateSchemaRejectsInvalidUpdates: the schema itself encodes the
// contract, transitions included.
func TestUpdateSchemaRejectsInvalidUpdates(t *testing.T) {
	sch := compileSchema(t, taskUpdateSchema)
	valid := map[string]any{
		"schema_version": 1, "message_id": "t:in_progress", "task_id": "t", "tenant_id": "tn",
		"task_type": "deploy", "previous_status": "accepted", "status": "in_progress",
		"occurred_at": "2026-01-02T03:04:05.006Z",
	}
	if err := validate(t, sch, marshal(t, valid)); err != nil {
		t.Fatalf("baseline update rejected: %v", err)
	}
	with := func(changes map[string]any) map[string]any {
		m := map[string]any{}
		for k, v := range valid {
			m[k] = v
		}
		for k, v := range changes {
			if v == nil {
				delete(m, k)
			} else {
				m[k] = v
			}
		}
		return m
	}
	tests := map[string]map[string]any{
		"missing message_id":            {"message_id": nil},
		"missing task_id":               {"task_id": nil},
		"missing previous_status":       {"previous_status": nil},
		"unknown schema version":        {"schema_version": 2},
		"accepted is not reportable":    {"status": "accepted"},
		"done straight from accepted":   {"status": "done"},
		"in_progress from in_progress":  {"previous_status": "in_progress"},
		"failed without reason":         {"status": "failed", "previous_status": "in_progress"},
		"reason on in_progress":         {"reason": "x"},
		"unknown task type":             {"task_type": "resize"},
		"timestamp without millis":      {"occurred_at": "2026-01-02T03:04:05Z"},
		"timestamp with offset":         {"occurred_at": "2026-01-02T03:04:05.006+01:00"},
		"message_id too long for a key": {"message_id": strings.Repeat("x", 201)},
	}
	for name, changes := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validate(t, sch, marshal(t, with(changes))); err == nil {
				t.Error("schema accepted an invalid update")
			}
		})
	}
	if err := validate(t, sch, marshal(t, with(map[string]any{"extra": true}))); err != nil {
		t.Errorf("unknown property rejected; consumers must tolerate additions: %v", err)
	}
}

func TestTaskMessageMatchesSchema(t *testing.T) {
	sch := compileSchema(t, taskEventSchema)
	for _, typ := range []domain.TaskType{domain.TaskTypeDeploy, domain.TaskTypeUpdate, domain.TaskTypeDestroy} {
		body := marshal(t, rabbitmq.NewTaskMessage(newTask(t, typ)))
		if err := validate(t, sch, body); err != nil {
			t.Errorf("task event %s does not match the schema: %v", body, err)
		}
	}
}

func TestDecodeTaskMessageRoundTrip(t *testing.T) {
	task := newTask(t, domain.TaskTypeUpdate)
	got, err := rabbitmq.DecodeTaskMessage(task.ID, marshal(t, rabbitmq.NewTaskMessage(task)))
	if err != nil {
		t.Fatalf("DecodeTaskMessage: %v", err)
	}
	if got != task {
		t.Errorf("decoded %+v, want %+v", got, task)
	}
	// Without a message-id (say, published by hand), the body alone counts;
	// unknown fields are ignored.
	body := `{"id":"t","type":"deploy","tenant_id":"tn","status":"accepted",` +
		`"created_at":"2026-01-02T03:04:05.006Z","updated_at":"2026-01-02T03:04:05.006Z","extra":1}`
	if _, err := rabbitmq.DecodeTaskMessage("", []byte(body)); err != nil {
		t.Errorf("DecodeTaskMessage without message-id: %v", err)
	}
}

// TestDecodeTaskMessageRejectsMalformed: everything the worker cannot
// process is reported as ErrMalformedMessage, so it is dead-lettered
// rather than retried.
func TestDecodeTaskMessageRejectsMalformed(t *testing.T) {
	good := map[string]any{
		"id": "t", "type": "deploy", "tenant_id": "tn", "status": "accepted",
		"created_at": "2026-01-02T03:04:05.006Z", "updated_at": "2026-01-02T03:04:05.006Z",
	}
	with := func(k string, v any) []byte {
		m := map[string]any{}
		for key, val := range good {
			m[key] = val
		}
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
		return marshal(t, m)
	}
	tests := []struct {
		name      string
		messageID string
		body      []byte
	}{
		{"empty body", "", nil},
		{"not JSON", "", []byte("deploy tenant acme")},
		{"JSON array", "", []byte(`[1,2]`)},
		{"JSON null", "", []byte(`null`)},
		{"id of wrong type", "", with("id", 42)},
		{"missing id", "", with("id", nil)},
		{"id too long", "", with("id", strings.Repeat("x", rabbitmq.MaxTaskIDLength+1))},
		{"message-id mismatch", "other", with("id", "t")},
		{"unknown type", "", with("type", "resize")},
		{"missing tenant", "", with("tenant_id", nil)},
		{"not accepted", "", with("status", "in_progress")},
		{"bad created_at", "", with("created_at", "yesterday")},
		{"missing updated_at", "", with("updated_at", nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rabbitmq.DecodeTaskMessage(tt.messageID, tt.body)
			if !errors.Is(err, rabbitmq.ErrMalformedMessage) {
				t.Errorf("err = %v, want ErrMalformedMessage", err)
			}
		})
	}
}
