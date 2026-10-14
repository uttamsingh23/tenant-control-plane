package domain

import (
	"errors"
	"strings"
	"testing"
)

var allTenantStatuses = []TenantStatus{
	TenantStatusProvisioning, TenantStatusActive, TenantStatusUpdating,
	TenantStatusDestroying, TenantStatusDestroyed, TenantStatusFailed,
}

var allTenantEvents = []TenantEvent{
	TenantEventUpdateRequested, TenantEventDestroyRequested,
	TenantEventDeployDone, TenantEventDeployFailed,
	TenantEventUpdateDone, TenantEventUpdateFailed,
	TenantEventDestroyDone, TenantEventDestroyFailed,
}

type tenantEdge struct {
	from  TenantStatus
	event TenantEvent
}

// legalTenantTransitions restates the assignment's transition list
// independently of tenantTransitions.
var legalTenantTransitions = map[tenantEdge]TenantStatus{
	{TenantStatusProvisioning, TenantEventDeployDone}:   TenantStatusActive,
	{TenantStatusProvisioning, TenantEventDeployFailed}: TenantStatusFailed,
	{TenantStatusActive, TenantEventUpdateRequested}:    TenantStatusUpdating,
	{TenantStatusUpdating, TenantEventUpdateDone}:       TenantStatusActive,
	{TenantStatusUpdating, TenantEventUpdateFailed}:     TenantStatusFailed,
	{TenantStatusActive, TenantEventDestroyRequested}:   TenantStatusDestroying,
	{TenantStatusFailed, TenantEventDestroyRequested}:   TenantStatusDestroying,
	{TenantStatusDestroying, TenantEventDestroyDone}:    TenantStatusDestroyed,
	{TenantStatusDestroying, TenantEventDestroyFailed}:  TenantStatusFailed,
}

func tenantIn(status TenantStatus) Tenant {
	return Tenant{ID: "tenant-1", Slug: "acme", Name: "Acme", Status: status, Version: 7, CreatedAt: t0, UpdatedAt: t0}
}

func TestNewTenant(t *testing.T) {
	tenant, err := NewTenant("tenant-1", "acme", "Acme Corp", t0)
	if err != nil {
		t.Fatalf("NewTenant: %v", err)
	}
	want := Tenant{
		ID: "tenant-1", Slug: "acme", Name: "Acme Corp",
		Status: TenantStatusProvisioning, Version: InitialTenantVersion,
		CreatedAt: t0, UpdatedAt: t0,
	}
	if tenant != want {
		t.Errorf("got %+v, want %+v", tenant, want)
	}
	if InitialTenantVersion != 1 {
		t.Errorf("InitialTenantVersion = %d, want 1", InitialTenantVersion)
	}
}

func TestNewTenantValidation(t *testing.T) {
	tests := []struct {
		name, id, slug, tenantName, field string
	}{
		{"empty id", "", "acme", "Acme", "id"},
		{"invalid slug", "tenant-1", "Acme", "Acme", "slug"},
		{"empty name", "tenant-1", "acme", "", "name"},
		{"whitespace name", "tenant-1", "acme", " \t\n", "name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTenant(tt.id, tt.slug, tt.tenantName, t0)
			assertValidationError(t, err, tt.field)
		})
	}
}

func TestValidateSlug(t *testing.T) {
	tests := []struct {
		name  string
		slug  string
		valid bool
	}{
		// Length boundaries.
		{"empty", "", false},
		{"1 char", "a", false},
		{"2 chars (min-1)", "ab", false},
		{"3 chars (min)", "abc", true},
		{"28 chars (max)", "a" + strings.Repeat("b", 27), true},
		{"29 chars (max+1)", "a" + strings.Repeat("b", 28), false},
		{"28 chars with hyphens and digits", "a" + strings.Repeat("-9", 13) + "z", true},

		// Valid shapes.
		{"hyphen in middle", "a-b", true},
		{"digits after first", "a12", true},
		{"ends with digit", "team-42", true},
		{"consecutive hyphens", "a--b", true}, // allowed by the exact regex
		{"all letters", "acme", true},

		// Invalid first character.
		{"starts with digit", "1abc", false},
		{"starts with hyphen", "-abc", false},
		{"starts with uppercase", "Abc", false},

		// Invalid last character.
		{"ends with hyphen", "abc-", false},
		{"min length ending with hyphen", "ab-", false},

		// Invalid characters.
		{"uppercase in middle", "aBc", false},
		{"underscore", "a_bc", false},
		{"dot", "a.bc", false},
		{"space", "a bc", false},
		{"leading space", " abc", false},
		{"trailing newline", "abc\n", false},
		{"non-ascii letter", "abç", false},
		{"multibyte within length", "aéb", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSlug(tt.slug)
			if tt.valid {
				if err != nil {
					t.Errorf("ValidateSlug(%q) = %v, want nil", tt.slug, err)
				}
				return
			}
			assertValidationError(t, err, "slug")
		})
	}
}

// TestTenantTransitionTable covers every (status, event) pair, legal and
// illegal, against the pure table lookup.
func TestTenantTransitionTable(t *testing.T) {
	for _, from := range allTenantStatuses {
		for _, event := range allTenantEvents {
			want, wantOK := legalTenantTransitions[tenantEdge{from, event}]
			got, ok := NextTenantStatus(from, event)
			if ok != wantOK || got != want {
				t.Errorf("NextTenantStatus(%s, %s) = (%q, %v), want (%q, %v)", from, event, got, ok, want, wantOK)
			}
		}
	}
}

// TestTenantApply covers every (status, event) pair through the mutating
// path: legal moves change status, bump version and updated_at exactly
// once; illegal ones return a *TransitionError and change nothing.
func TestTenantApply(t *testing.T) {
	for _, from := range allTenantStatuses {
		for _, event := range allTenantEvents {
			want, legal := legalTenantTransitions[tenantEdge{from, event}]
			t.Run(string(from)+"/"+string(event), func(t *testing.T) {
				tenant := tenantIn(from)
				before := tenant

				err := tenant.apply(event, t1)

				if legal {
					if err != nil {
						t.Fatalf("unexpected error %v", err)
					}
					if tenant.Status != want {
						t.Errorf("Status = %s, want %s", tenant.Status, want)
					}
					if tenant.Version != before.Version+1 {
						t.Errorf("Version = %d, want %d", tenant.Version, before.Version+1)
					}
					if tenant.UpdatedAt != t1 || tenant.CreatedAt != t0 {
						t.Errorf("timestamps = (%v, %v), want created %v, updated %v", tenant.CreatedAt, tenant.UpdatedAt, t0, t1)
					}
					return
				}
				assertTransitionError(t, err, "tenant", string(from), string(event))
				if tenant != before {
					t.Errorf("tenant changed on rejected transition: got %+v, want %+v", tenant, before)
				}
			})
		}
	}
}

func TestTenantDestroyedIsTerminal(t *testing.T) {
	for _, event := range allTenantEvents {
		if to, ok := NextTenantStatus(TenantStatusDestroyed, event); ok {
			t.Errorf("destroyed --%s--> %s allowed, want terminal", event, to)
		}
	}
}

func TestRequestUpdate(t *testing.T) {
	newName := "Acme Renamed"
	blank := "  "
	tests := []struct {
		name        string
		from        TenantStatus
		changes     TenantChanges
		wantName    string
		wantErr     error // nil means success
		notAllowed  bool
		wantChanged bool
	}{
		{"active, rename", TenantStatusActive, TenantChanges{Name: &newName}, newName, nil, false, true},
		{"active, no fields", TenantStatusActive, TenantChanges{}, "Acme", nil, false, true},
		{"active, blank name", TenantStatusActive, TenantChanges{Name: &blank}, "Acme", ErrInvalidInput, false, false},
		{"provisioning", TenantStatusProvisioning, TenantChanges{Name: &newName}, "Acme", ErrInvalidTransition, true, false},
		{"updating", TenantStatusUpdating, TenantChanges{Name: &newName}, "Acme", ErrInvalidTransition, true, false},
		{"destroying", TenantStatusDestroying, TenantChanges{Name: &newName}, "Acme", ErrInvalidTransition, true, false},
		{"destroyed", TenantStatusDestroyed, TenantChanges{Name: &newName}, "Acme", ErrInvalidTransition, true, false},
		{"failed", TenantStatusFailed, TenantChanges{Name: &newName}, "Acme", ErrInvalidTransition, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant := tenantIn(tt.from)
			before := tenant

			err := tenant.RequestUpdate(tt.changes, t1)

			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got := errors.Is(err, ErrTenantUpdateNotAllowed); got != tt.notAllowed {
				t.Errorf("errors.Is(err, ErrTenantUpdateNotAllowed) = %v, want %v", got, tt.notAllowed)
			}
			if tt.notAllowed {
				var te *TransitionError
				if !errors.As(err, &te) {
					t.Errorf("errors.As(*TransitionError) failed for %v", err)
				}
			}
			if !tt.wantChanged {
				if tenant != before {
					t.Errorf("tenant changed on error: got %+v, want %+v", tenant, before)
				}
				return
			}
			if tenant.Status != TenantStatusUpdating || tenant.Name != tt.wantName || tenant.Version != before.Version+1 || tenant.UpdatedAt != t1 {
				t.Errorf("got %+v, want updating, name %q, version %d, updated_at %v", tenant, tt.wantName, before.Version+1, t1)
			}
		})
	}
}

func TestRequestDestroy(t *testing.T) {
	for _, from := range allTenantStatuses {
		t.Run(string(from), func(t *testing.T) {
			allowed := from == TenantStatusActive || from == TenantStatusFailed
			tenant := tenantIn(from)
			before := tenant

			err := tenant.RequestDestroy(t1)

			if allowed {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if tenant.Status != TenantStatusDestroying || tenant.Version != before.Version+1 {
					t.Errorf("got %+v, want destroying at version %d", tenant, before.Version+1)
				}
				return
			}
			if !errors.Is(err, ErrTenantUpdateNotAllowed) {
				t.Errorf("err = %v, want ErrTenantUpdateNotAllowed", err)
			}
			assertTransitionError(t, err, "tenant", string(from), string(TenantEventDestroyRequested))
			if tenant != before {
				t.Errorf("tenant changed on error: got %+v, want %+v", tenant, before)
			}
		})
	}
}

func TestTaskOutcomeEvent(t *testing.T) {
	tests := []struct {
		typ     TaskType
		status  TaskStatus
		want    TenantEvent
		wantErr bool
	}{
		{TaskTypeDeploy, TaskStatusDone, TenantEventDeployDone, false},
		{TaskTypeDeploy, TaskStatusFailed, TenantEventDeployFailed, false},
		{TaskTypeUpdate, TaskStatusDone, TenantEventUpdateDone, false},
		{TaskTypeUpdate, TaskStatusFailed, TenantEventUpdateFailed, false},
		{TaskTypeDestroy, TaskStatusDone, TenantEventDestroyDone, false},
		{TaskTypeDestroy, TaskStatusFailed, TenantEventDestroyFailed, false},
		{TaskTypeDeploy, TaskStatusAccepted, "", true},
		{TaskTypeDeploy, TaskStatusInProgress, "", true},
		{TaskTypeUpdate, TaskStatusInProgress, "", true},
		{TaskTypeDestroy, TaskStatusAccepted, "", true},
		{"rollback", TaskStatusDone, "", true},
	}
	for _, tt := range tests {
		t.Run(string(tt.typ)+"/"+string(tt.status), func(t *testing.T) {
			got, err := TaskOutcomeEvent(tt.typ, tt.status)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidInput) {
					t.Errorf("err = %v, want ErrInvalidInput", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("got (%q, %v), want (%q, nil)", got, err, tt.want)
			}
		})
	}
}

// TestApplyTaskOutcome drives every terminal task outcome against every
// tenant status, through the worker-facing API.
func TestApplyTaskOutcome(t *testing.T) {
	for _, from := range allTenantStatuses {
		for _, typ := range allTaskTypes {
			for _, status := range []TaskStatus{TaskStatusDone, TaskStatusFailed} {
				event, err := TaskOutcomeEvent(typ, status)
				if err != nil {
					t.Fatal(err)
				}
				want, legal := legalTenantTransitions[tenantEdge{from, event}]
				t.Run(string(from)+"/"+string(typ)+"_"+string(status), func(t *testing.T) {
					tenant := tenantIn(from)
					before := tenant
					task := Task{ID: "task-1", Type: typ, TenantID: tenant.ID, Status: status}

					err := tenant.ApplyTaskOutcome(task, t1)

					if legal {
						if err != nil || tenant.Status != want || tenant.Version != before.Version+1 {
							t.Errorf("err = %v, tenant = %+v; want status %s at version %d", err, tenant, want, before.Version+1)
						}
						return
					}
					assertTransitionError(t, err, "tenant", string(from), string(event))
					if errors.Is(err, ErrTenantUpdateNotAllowed) {
						t.Errorf("worker outcome error must not map to ErrTenantUpdateNotAllowed: %v", err)
					}
					if tenant != before {
						t.Errorf("tenant changed on error: got %+v, want %+v", tenant, before)
					}
				})
			}
		}
	}
}

func TestApplyTaskOutcomeRejectsBadTasks(t *testing.T) {
	tests := []struct {
		name  string
		task  Task
		field string
	}{
		{"other tenant", Task{ID: "task-1", Type: TaskTypeDeploy, TenantID: "tenant-2", Status: TaskStatusDone}, "task tenant_id"},
		{"accepted", Task{ID: "task-1", Type: TaskTypeDeploy, TenantID: "tenant-1", Status: TaskStatusAccepted}, "task status"},
		{"in progress", Task{ID: "task-1", Type: TaskTypeDeploy, TenantID: "tenant-1", Status: TaskStatusInProgress}, "task status"},
		{"unknown type", Task{ID: "task-1", Type: "rollback", TenantID: "tenant-1", Status: TaskStatusDone}, "task type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant := tenantIn(TenantStatusProvisioning)
			before := tenant
			assertValidationError(t, tenant.ApplyTaskOutcome(tt.task, t1), tt.field)
			if tenant != before {
				t.Errorf("tenant changed on error: got %+v, want %+v", tenant, before)
			}
		})
	}
}

// TestTenantVersionLifecycle walks a realistic lifecycle and checks that
// every mutation, API- or worker-driven, bumps the version by exactly one
// and that rejected attempts do not.
func TestTenantVersionLifecycle(t *testing.T) {
	tenant, err := NewTenant("tenant-1", "acme", "Acme", t0)
	if err != nil {
		t.Fatal(err)
	}
	outcome := func(typ TaskType, status TaskStatus) func(*Tenant) error {
		return func(tn *Tenant) error {
			return tn.ApplyTaskOutcome(Task{ID: "x", Type: typ, TenantID: tn.ID, Status: status}, t1)
		}
	}
	rename := "Acme 2"
	steps := []struct {
		name        string
		do          func(*Tenant) error
		wantStatus  TenantStatus
		wantVersion int64
	}{
		{"update while provisioning rejected", func(tn *Tenant) error { return tn.RequestUpdate(TenantChanges{}, t1) }, TenantStatusProvisioning, 1},
		{"deploy done", outcome(TaskTypeDeploy, TaskStatusDone), TenantStatusActive, 2},
		{"duplicate deploy done rejected", outcome(TaskTypeDeploy, TaskStatusDone), TenantStatusActive, 2},
		{"PATCH", func(tn *Tenant) error { return tn.RequestUpdate(TenantChanges{Name: &rename}, t1) }, TenantStatusUpdating, 3},
		{"second PATCH rejected", func(tn *Tenant) error { return tn.RequestUpdate(TenantChanges{}, t1) }, TenantStatusUpdating, 3},
		{"update failed", outcome(TaskTypeUpdate, TaskStatusFailed), TenantStatusFailed, 4},
		{"DELETE", func(tn *Tenant) error { return tn.RequestDestroy(t1) }, TenantStatusDestroying, 5},
		{"destroy failed", outcome(TaskTypeDestroy, TaskStatusFailed), TenantStatusFailed, 6},
		{"DELETE again", func(tn *Tenant) error { return tn.RequestDestroy(t1) }, TenantStatusDestroying, 7},
		{"destroy done", outcome(TaskTypeDestroy, TaskStatusDone), TenantStatusDestroyed, 8},
		{"DELETE destroyed rejected", func(tn *Tenant) error { return tn.RequestDestroy(t1) }, TenantStatusDestroyed, 8},
	}
	for _, s := range steps {
		before := tenant.Version
		err := s.do(&tenant)
		if (err == nil) != (s.wantVersion == before+1) {
			t.Fatalf("%s: err = %v but version %d -> want %d", s.name, err, before, s.wantVersion)
		}
		if tenant.Status != s.wantStatus || tenant.Version != s.wantVersion {
			t.Fatalf("%s: got (%s, v%d), want (%s, v%d)", s.name, tenant.Status, tenant.Version, s.wantStatus, s.wantVersion)
		}
	}
	if tenant.Name != rename {
		t.Errorf("Name = %q, want %q", tenant.Name, rename)
	}
}
