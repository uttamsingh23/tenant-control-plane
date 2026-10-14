package domain

import (
	"errors"
	"testing"
	"time"
)

var allTaskStatuses = []TaskStatus{TaskStatusAccepted, TaskStatusInProgress, TaskStatusDone, TaskStatusFailed}

var allTaskTypes = []TaskType{TaskTypeDeploy, TaskTypeUpdate, TaskTypeDestroy}

// legalTaskTransitions is written out independently of taskTransitions so
// the test checks the table instead of restating it.
var legalTaskTransitions = map[[2]TaskStatus]bool{
	{TaskStatusAccepted, TaskStatusInProgress}: true,
	{TaskStatusInProgress, TaskStatusDone}:     true,
	{TaskStatusInProgress, TaskStatusFailed}:   true,
}

var (
	t0 = time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)
	t1 = t0.Add(time.Second)
)

func TestNewTask(t *testing.T) {
	for _, typ := range allTaskTypes {
		t.Run(string(typ), func(t *testing.T) {
			task, err := NewTask("task-1", typ, "tenant-1", t0)
			if err != nil {
				t.Fatalf("NewTask: %v", err)
			}
			want := Task{ID: "task-1", Type: typ, TenantID: "tenant-1", Status: TaskStatusAccepted, CreatedAt: t0, UpdatedAt: t0}
			if task != want {
				t.Errorf("got %+v, want %+v", task, want)
			}
		})
	}
}

func TestNewTaskValidation(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		typ      TaskType
		tenantID string
		field    string
	}{
		{"empty id", "", TaskTypeDeploy, "tenant-1", "id"},
		{"empty type", "task-1", "", "tenant-1", "type"},
		{"unknown type", "task-1", "rollback", "tenant-1", "type"},
		{"wrong case type", "task-1", "Deploy", "tenant-1", "type"},
		{"empty tenant id", "task-1", TaskTypeDeploy, "", "tenant_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTask(tt.id, tt.typ, tt.tenantID, t0)
			assertValidationError(t, err, tt.field)
		})
	}
}

// TestTaskTransitions covers every (from, to) pair, legal and illegal,
// including self-transitions.
func TestTaskTransitions(t *testing.T) {
	for _, from := range allTaskStatuses {
		for _, to := range allTaskStatuses {
			legal := legalTaskTransitions[[2]TaskStatus{from, to}]
			t.Run(string(from)+"->"+string(to), func(t *testing.T) {
				task := Task{ID: "task-1", Type: TaskTypeDeploy, TenantID: "tenant-1", Status: from, CreatedAt: t0, UpdatedAt: t0}
				before := task

				if got := from.CanTransitionTo(to); got != legal {
					t.Errorf("CanTransitionTo = %v, want %v", got, legal)
				}
				err := task.TransitionTo(to, t1)

				if legal {
					if err != nil {
						t.Fatalf("TransitionTo: unexpected error %v", err)
					}
					if task.Status != to || !task.UpdatedAt.Equal(t1) || !task.CreatedAt.Equal(t0) {
						t.Errorf("got %+v, want status %s, updated_at %v", task, to, t1)
					}
					return
				}
				assertTransitionError(t, err, "task", string(from), string(to))
				if task != before {
					t.Errorf("task changed on rejected transition: got %+v, want %+v", task, before)
				}
			})
		}
	}
}

// TestTerminalTaskNeverRegresses checks that no status, including the
// terminal status itself, is reachable from done or failed.
func TestTerminalTaskNeverRegresses(t *testing.T) {
	for _, terminal := range []TaskStatus{TaskStatusDone, TaskStatusFailed} {
		if !terminal.Terminal() || terminal.Open() {
			t.Errorf("%s: Terminal=%v Open=%v, want terminal and not open", terminal, terminal.Terminal(), terminal.Open())
		}
		for _, next := range allTaskStatuses {
			task := Task{Status: terminal}
			if err := task.TransitionTo(next, t1); !errors.Is(err, ErrInvalidTransition) || task.Status != terminal {
				t.Errorf("%s -> %s: err = %v, status = %s; want ErrInvalidTransition and unchanged", terminal, next, err, task.Status)
			}
		}
	}
}

func TestTaskStatusPredicates(t *testing.T) {
	tests := []struct {
		status   TaskStatus
		terminal bool
		open     bool
	}{
		{TaskStatusAccepted, false, true},
		{TaskStatusInProgress, false, true},
		{TaskStatusDone, true, false},
		{TaskStatusFailed, true, false},
		{"bogus", false, false},
	}
	for _, tt := range tests {
		if got := tt.status.Terminal(); got != tt.terminal {
			t.Errorf("%s.Terminal() = %v, want %v", tt.status, got, tt.terminal)
		}
		if got := tt.status.Open(); got != tt.open {
			t.Errorf("%s.Open() = %v, want %v", tt.status, got, tt.open)
		}
	}
}

func TestTaskTransitionNormalizesTime(t *testing.T) {
	task, err := NewTask("task-1", TaskTypeDeploy, "tenant-1", t0)
	if err != nil {
		t.Fatal(err)
	}
	ist := time.FixedZone("IST", 5*3600+1800)
	if err := task.TransitionTo(TaskStatusInProgress, time.Date(2026, 1, 2, 8, 34, 6, 7_891_234, ist)); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 1, 2, 3, 4, 6, 7_000_000, time.UTC); task.UpdatedAt != want {
		t.Errorf("UpdatedAt = %v, want %v", task.UpdatedAt, want)
	}
}

func assertValidationError(t *testing.T, err error, field string) {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *ValidationError", err)
	}
	if ve.Field != field {
		t.Errorf("Field = %q, want %q", ve.Field, field)
	}
	if !errors.Is(err, ErrInvalidInput) {
		t.Errorf("errors.Is(err, ErrInvalidInput) = false for %v", err)
	}
}

func assertTransitionError(t *testing.T, err error, entity, from, trigger string) {
	t.Helper()
	var te *TransitionError
	if !errors.As(err, &te) {
		t.Fatalf("err = %v, want *TransitionError", err)
	}
	if te.Entity != entity || te.From != from || te.Trigger != trigger {
		t.Errorf("got %+v, want entity=%s from=%s trigger=%s", te, entity, from, trigger)
	}
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("errors.Is(err, ErrInvalidTransition) = false for %v", err)
	}
}
