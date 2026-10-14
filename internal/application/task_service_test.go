package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// tenantStatusFor is the tenant status while a task of typ is open.
var tenantStatusFor = map[domain.TaskType]domain.TenantStatus{
	domain.TaskTypeDeploy:  domain.TenantStatusProvisioning,
	domain.TaskTypeUpdate:  domain.TenantStatusUpdating,
	domain.TaskTypeDestroy: domain.TenantStatusDestroying,
}

// updateFor is the update a worker sends to report status for task: it
// echoes the task's tenant and type, and names the transition.
func updateFor(task domain.Task, id string, status domain.TaskStatus) domain.TaskUpdate {
	previous := domain.TaskStatusInProgress
	if status == domain.TaskStatusInProgress {
		previous = domain.TaskStatusAccepted
	}
	return domain.TaskUpdate{
		ID: id, TaskID: task.ID, TenantID: task.TenantID, TaskType: task.Type,
		PreviousStatus: previous, Status: status, OccurredAt: t0,
	}
}

func TestApplyUpdate(t *testing.T) {
	const (
		accepted   = domain.TaskStatusAccepted
		inProgress = domain.TaskStatusInProgress
		done       = domain.TaskStatusDone
		failed     = domain.TaskStatusFailed
	)
	tests := []struct {
		typ          domain.TaskType
		from, report domain.TaskStatus
		want         UpdateOutcome
		wantTask     domain.TaskStatus
		wantTenant   domain.TenantStatus // empty: tenant unchanged
	}{
		{typ: domain.TaskTypeDeploy, from: accepted, report: inProgress, want: UpdateApplied, wantTask: inProgress},
		{typ: domain.TaskTypeDeploy, from: inProgress, report: done, want: UpdateApplied, wantTask: done, wantTenant: domain.TenantStatusActive},
		{typ: domain.TaskTypeDeploy, from: inProgress, report: failed, want: UpdateApplied, wantTask: failed, wantTenant: domain.TenantStatusFailed},
		{typ: domain.TaskTypeUpdate, from: inProgress, report: done, want: UpdateApplied, wantTask: done, wantTenant: domain.TenantStatusActive},
		{typ: domain.TaskTypeUpdate, from: inProgress, report: failed, want: UpdateApplied, wantTask: failed, wantTenant: domain.TenantStatusFailed},
		{typ: domain.TaskTypeDestroy, from: inProgress, report: done, want: UpdateApplied, wantTask: done, wantTenant: domain.TenantStatusDestroyed},
		{typ: domain.TaskTypeDestroy, from: inProgress, report: failed, want: UpdateApplied, wantTask: failed, wantTenant: domain.TenantStatusFailed},

		// A terminal outcome that overtook its in_progress report.
		{typ: domain.TaskTypeDeploy, from: accepted, report: done, want: UpdateApplied, wantTask: done, wantTenant: domain.TenantStatusActive},
		{typ: domain.TaskTypeUpdate, from: accepted, report: failed, want: UpdateApplied, wantTask: failed, wantTenant: domain.TenantStatusFailed},

		// Stale: the task already reached or passed the reported status.
		{typ: domain.TaskTypeDeploy, from: inProgress, report: inProgress, want: UpdateStale, wantTask: inProgress},
		{typ: domain.TaskTypeDeploy, from: done, report: inProgress, want: UpdateStale, wantTask: done},
		{typ: domain.TaskTypeDeploy, from: done, report: done, want: UpdateStale, wantTask: done},
		{typ: domain.TaskTypeDestroy, from: failed, report: inProgress, want: UpdateStale, wantTask: failed},
		{typ: domain.TaskTypeDestroy, from: failed, report: failed, want: UpdateStale, wantTask: failed},

		// Conflict: a late outcome contradicting the recorded one. The
		// first outcome wins.
		{typ: domain.TaskTypeDeploy, from: done, report: failed, want: UpdateConflict, wantTask: done},
		{typ: domain.TaskTypeDestroy, from: failed, report: done, want: UpdateConflict, wantTask: failed},
	}
	for _, tt := range tests {
		t.Run(string(tt.typ)+"/"+string(tt.from)+"->"+string(tt.report), func(t *testing.T) {
			store, _, tasks := newServices()
			tenantStatus := tenantStatusFor[tt.typ]
			if tt.from.Terminal() { // the outcome was already applied to the tenant
				tenantStatus = domain.TenantStatusActive
			}
			tenant := seedTenant(store, tenantStatus)
			task := seedTask(store, tenant, tt.typ, tt.from)
			before := snapshot(store)

			got, err := tasks.ApplyUpdate(t.Context(), updateFor(task, "u1", tt.report))
			if err != nil {
				t.Fatalf("ApplyUpdate: %v", err)
			}
			if got != tt.want {
				t.Errorf("outcome = %s, want %s", got, tt.want)
			}
			if !store.processed["u1"] {
				t.Error("idempotency key not recorded")
			}

			gotTask := store.tasks[task.ID]
			if gotTask.Status != tt.wantTask {
				t.Errorf("task status = %s, want %s", gotTask.Status, tt.wantTask)
			}
			if tt.want != UpdateApplied && gotTask != before.tasks[task.ID] {
				t.Errorf("%s update changed the task: %+v", tt.want, gotTask)
			}

			gotTenant := store.tenants[tenant.ID]
			if tt.wantTenant == "" {
				if gotTenant != tenant {
					t.Errorf("tenant changed on a non-terminal update: %+v", gotTenant)
				}
				return
			}
			if gotTenant.Status != tt.wantTenant || gotTenant.Version != tenant.Version+1 {
				t.Errorf("tenant = %s v%d, want %s v%d", gotTenant.Status, gotTenant.Version, tt.wantTenant, tenant.Version+1)
			}
			if gotTenant.UpdatedAt != gotTask.UpdatedAt {
				t.Errorf("tenant updated_at %v != task updated_at %v", gotTenant.UpdatedAt, gotTask.UpdatedAt)
			}
		})
	}
}

func TestApplyUpdateDuplicateChangesNothing(t *testing.T) {
	store, _, tasks := newServices()
	tenant := seedTenant(store, domain.TenantStatusProvisioning)
	task := seedTask(store, tenant, domain.TaskTypeDeploy, domain.TaskStatusInProgress)
	update := updateFor(task, "u1", domain.TaskStatusDone)

	if got, err := tasks.ApplyUpdate(t.Context(), update); err != nil || got != UpdateApplied {
		t.Fatalf("first delivery = %s, %v; want applied", got, err)
	}
	before := snapshot(store)
	if got, err := tasks.ApplyUpdate(t.Context(), update); err != nil || got != UpdateDuplicate {
		t.Fatalf("redelivery = %s, %v; want duplicate", got, err)
	}
	assertUnchanged(t, store, before)
}

// The full worker sequence delivered in reverse: done first, then the late
// in_progress. The outcome is applied once and nothing regresses.
func TestApplyUpdateOutOfOrder(t *testing.T) {
	store, _, tasks := newServices()
	tenant := seedTenant(store, domain.TenantStatusProvisioning)
	task := seedTask(store, tenant, domain.TaskTypeDeploy, domain.TaskStatusAccepted)

	if got, err := tasks.ApplyUpdate(t.Context(), updateFor(task, "u2", domain.TaskStatusDone)); err != nil || got != UpdateApplied {
		t.Fatalf("done = %s, %v; want applied", got, err)
	}
	afterDone := snapshot(store)
	if got, err := tasks.ApplyUpdate(t.Context(), updateFor(task, "u1", domain.TaskStatusInProgress)); err != nil || got != UpdateStale {
		t.Fatalf("late in_progress = %s, %v; want stale", got, err)
	}
	if store.tasks[task.ID] != afterDone.tasks[task.ID] || store.tenants[tenant.ID] != afterDone.tenants[tenant.ID] {
		t.Error("late in_progress changed the task or tenant")
	}
	if got := store.tenants[tenant.ID]; got.Status != domain.TenantStatusActive || got.Version != tenant.Version+1 {
		t.Errorf("tenant = %s v%d, want active v%d", got.Status, got.Version, tenant.Version+1)
	}
}

// Updates that cannot be applied fail without recording the idempotency
// key or touching the task or tenant.
func TestApplyUpdateRejectsWithoutPartialMutation(t *testing.T) {
	done := func(task domain.Task) domain.TaskUpdate { return updateFor(task, "u1", domain.TaskStatusDone) }
	with := func(edit func(*domain.TaskUpdate)) func(domain.Task) domain.TaskUpdate {
		return func(task domain.Task) domain.TaskUpdate {
			u := done(task)
			edit(&u)
			return u
		}
	}
	tests := []struct {
		name    string
		update  func(domain.Task) domain.TaskUpdate
		setup   func(*fakeStore)
		wantErr error
	}{
		{name: "missing update id", update: with(func(u *domain.TaskUpdate) { u.ID = "" }), wantErr: domain.ErrInvalidInput},
		{name: "update id too long", update: with(func(u *domain.TaskUpdate) { u.ID = strings.Repeat("x", MaxUpdateIDLength+1) }), wantErr: domain.ErrInvalidInput},
		{name: "missing task id", update: with(func(u *domain.TaskUpdate) { u.TaskID = "" }), wantErr: domain.ErrInvalidInput},
		{name: "missing tenant id", update: with(func(u *domain.TaskUpdate) { u.TenantID = "" }), wantErr: domain.ErrInvalidInput},
		{name: "unknown task type", update: with(func(u *domain.TaskUpdate) { u.TaskType = "resize" }), wantErr: domain.ErrInvalidInput},
		{name: "status accepted", update: with(func(u *domain.TaskUpdate) { u.Status = domain.TaskStatusAccepted }), wantErr: domain.ErrInvalidInput},
		{name: "unknown status", update: with(func(u *domain.TaskUpdate) { u.Status = "exploded" }), wantErr: domain.ErrInvalidInput},
		{name: "skipped transition", update: with(func(u *domain.TaskUpdate) { u.PreviousStatus = domain.TaskStatusAccepted }), wantErr: domain.ErrInvalidInput},
		{name: "transition out of terminal", update: with(func(u *domain.TaskUpdate) { u.PreviousStatus = domain.TaskStatusFailed }), wantErr: domain.ErrInvalidInput},
		{name: "missing previous status", update: with(func(u *domain.TaskUpdate) { u.PreviousStatus = "" }), wantErr: domain.ErrInvalidInput},
		{name: "unknown task", update: with(func(u *domain.TaskUpdate) { u.TaskID = "missing" }), wantErr: domain.ErrTaskNotFound},
		{name: "other tenant", update: with(func(u *domain.TaskUpdate) { u.TenantID = "tenant-active" }), wantErr: domain.ErrInvalidInput},
		{name: "other task type", update: with(func(u *domain.TaskUpdate) { u.TaskType = domain.TaskTypeDestroy }), wantErr: domain.ErrInvalidInput},
		{
			// The tenant is not waiting for a deploy outcome: the task
			// change must be rolled back with the rejected tenant change.
			name:   "tenant not expecting outcome",
			update: done,
			setup: func(s *fakeStore) {
				tn := s.tenants["tenant-provisioning"]
				tn.Status = domain.TenantStatusActive
				s.tenants[tn.ID] = tn
			},
			wantErr: domain.ErrInvalidTransition,
		},
		{name: "tenant write fails", update: done, setup: func(s *fakeStore) { s.failOn = "tenants.update" }, wantErr: errInjected},
		{name: "task write fails", update: done, setup: func(s *fakeStore) { s.failOn = "tasks.update" }, wantErr: errInjected},
		{name: "idempotency write fails", update: done, setup: func(s *fakeStore) { s.failOn = "processed.mark" }, wantErr: errInjected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _, tasks := newServices()
			tenant := seedTenant(store, domain.TenantStatusProvisioning)
			task := seedTask(store, tenant, domain.TaskTypeDeploy, domain.TaskStatusInProgress)
			if tt.setup != nil {
				tt.setup(store)
			}
			before := snapshot(store)

			got, err := tasks.ApplyUpdate(t.Context(), tt.update(task))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != "" {
				t.Errorf("outcome = %q on error, want empty", got)
			}
			assertUnchanged(t, store, before)
		})
	}
}

// IsPermanentUpdateError splits ApplyUpdate's errors into those a retry
// cannot fix (dead-letter) and those it might (retry), with unknown
// errors on the retry side.
func TestIsPermanentUpdateError(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("apply update u1 to task t1: %w", err) }
	tests := []struct {
		err  error
		want bool
	}{
		{wrap(&domain.ValidationError{Field: "tenant_id", Reason: "mismatch"}), true},
		{wrap(domain.ErrTaskNotFound), true},
		{wrap(domain.ErrTenantNotFound), true},
		{wrap(&domain.TransitionError{Entity: "tenant", From: "active", Trigger: "deploy_done"}), true},
		{wrap(domain.ErrTenantVersionConflict), false},
		{wrap(context.DeadlineExceeded), false},
		{wrap(context.Canceled), false},
		{wrap(errInjected), false},
		{errors.New("connection reset by peer"), false},
	}
	for _, tt := range tests {
		if got := IsPermanentUpdateError(tt.err); got != tt.want {
			t.Errorf("IsPermanentUpdateError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// A rejected update leaves the key unrecorded, so a retry after the cause
// is fixed is applied rather than treated as a duplicate.
func TestApplyUpdateRetryAfterFailure(t *testing.T) {
	store, _, tasks := newServices()
	tenant := seedTenant(store, domain.TenantStatusProvisioning)
	task := seedTask(store, tenant, domain.TaskTypeDeploy, domain.TaskStatusInProgress)
	update := updateFor(task, "u1", domain.TaskStatusDone)

	store.failOn = "tenants.update"
	if _, err := tasks.ApplyUpdate(t.Context(), update); err == nil {
		t.Fatal("want injected failure")
	}
	store.failOn = ""
	if got, err := tasks.ApplyUpdate(t.Context(), update); err != nil || got != UpdateApplied {
		t.Fatalf("retry = %s, %v; want applied", got, err)
	}
}

func TestTaskReads(t *testing.T) {
	store, _, tasks := newServices()
	tenant := seedTenant(store, domain.TenantStatusProvisioning)
	task := seedTask(store, tenant, domain.TaskTypeDeploy, domain.TaskStatusAccepted)

	got, err := tasks.Get(t.Context(), task.ID)
	if err != nil || got != task {
		t.Errorf("Get = %+v, %v; want %+v", got, err, task)
	}
	if _, err := tasks.Get(t.Context(), "missing"); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("Get(missing) err = %v, want ErrTaskNotFound", err)
	}

	page, err := tasks.List(t.Context(), TaskFilter{TenantID: tenant.ID}, PageRequest{})
	if err != nil || len(page.Items) != 1 || page.Items[0] != task {
		t.Errorf("List = %+v, %v; want the one task", page, err)
	}
	if store.lastPage.Limit != DefaultPageLimit {
		t.Errorf("repository got limit %d, want default %d", store.lastPage.Limit, DefaultPageLimit)
	}
}
