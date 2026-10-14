// Package domain holds the core types of the tenant provisioning control plane.
//
// It must not depend on transport, storage or broker packages: everything
// here is plain Go so it can be reasoned about and tested in isolation.
// Identifiers and the current time are supplied by the caller, never
// generated here.
package domain

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// TenantStatus is a state in the tenant state machine.
type TenantStatus string

// Tenant states. TenantStatusDestroyed is terminal.
const (
	TenantStatusProvisioning TenantStatus = "provisioning"
	TenantStatusActive       TenantStatus = "active"
	TenantStatusUpdating     TenantStatus = "updating"
	TenantStatusDestroying   TenantStatus = "destroying"
	TenantStatusDestroyed    TenantStatus = "destroyed"
	TenantStatusFailed       TenantStatus = "failed"
)

// TenantEvent is something that moves a tenant: an API request or the
// terminal outcome of one of its tasks.
type TenantEvent string

// Tenant events.
const (
	TenantEventUpdateRequested  TenantEvent = "update_requested"  // PATCH
	TenantEventDestroyRequested TenantEvent = "destroy_requested" // DELETE
	TenantEventDeployDone       TenantEvent = "deploy_done"
	TenantEventDeployFailed     TenantEvent = "deploy_failed"
	TenantEventUpdateDone       TenantEvent = "update_done"
	TenantEventUpdateFailed     TenantEvent = "update_failed"
	TenantEventDestroyDone      TenantEvent = "destroy_done"
	TenantEventDestroyFailed    TenantEvent = "destroy_failed"
)

// tenantTransitions is the single source of truth for the tenant state
// machine. Creation is the implicit entry edge (none -> provisioning, see
// NewTenant); every other edge is listed here:
//
//	provisioning --deploy_done-------> active
//	provisioning --deploy_failed-----> failed
//	active       --update_requested--> updating
//	active       --destroy_requested-> destroying
//	updating     --update_done-------> active
//	updating     --update_failed-----> failed
//	failed       --destroy_requested-> destroying
//	destroying   --destroy_done------> destroyed
//	destroying   --destroy_failed----> failed
//	destroyed: terminal
//
// Keying by event rather than by target status means a task outcome only
// applies in the state its task was created from: a destroy failure cannot
// move a provisioning tenant to failed, even though provisioning -> failed
// is a legal edge for a deploy failure. Every status that accepts a task
// outcome is an "open task" status, and the API events are only accepted
// from statuses without an open task, which keeps one open task per tenant.
var tenantTransitions = map[TenantStatus]map[TenantEvent]TenantStatus{
	TenantStatusProvisioning: {
		TenantEventDeployDone:   TenantStatusActive,
		TenantEventDeployFailed: TenantStatusFailed,
	},
	TenantStatusActive: {
		TenantEventUpdateRequested:  TenantStatusUpdating,
		TenantEventDestroyRequested: TenantStatusDestroying,
	},
	TenantStatusUpdating: {
		TenantEventUpdateDone:   TenantStatusActive,
		TenantEventUpdateFailed: TenantStatusFailed,
	},
	TenantStatusFailed: {
		TenantEventDestroyRequested: TenantStatusDestroying,
	},
	TenantStatusDestroying: {
		TenantEventDestroyDone:   TenantStatusDestroyed,
		TenantEventDestroyFailed: TenantStatusFailed,
	},
}

// NextTenantStatus returns the status a tenant in from moves to on event,
// and false if the state machine does not allow it.
func NextTenantStatus(from TenantStatus, event TenantEvent) (TenantStatus, bool) {
	to, ok := tenantTransitions[from][event]
	return to, ok
}

// TaskOutcomeEvent maps a terminal task outcome to the tenant event it
// triggers. It fails for unknown types and non-terminal statuses, which
// must never move a tenant.
func TaskOutcomeEvent(typ TaskType, status TaskStatus) (TenantEvent, error) {
	if !status.Terminal() {
		return "", &ValidationError{Field: "task status", Reason: fmt.Sprintf("%s is not a terminal outcome", status)}
	}
	done := status == TaskStatusDone
	switch typ {
	case TaskTypeDeploy:
		return pick(done, TenantEventDeployDone, TenantEventDeployFailed), nil
	case TaskTypeUpdate:
		return pick(done, TenantEventUpdateDone, TenantEventUpdateFailed), nil
	case TaskTypeDestroy:
		return pick(done, TenantEventDestroyDone, TenantEventDestroyFailed), nil
	}
	return "", &ValidationError{Field: "task type", Reason: fmt.Sprintf("unknown type %q", typ)}
}

func pick(done bool, onDone, onFailed TenantEvent) TenantEvent {
	if done {
		return onDone
	}
	return onFailed
}

// InitialTenantVersion is the version of a newly created tenant. Every
// subsequent mutation increments it by one.
const InitialTenantVersion int64 = 1

// Slug length bounds. slugPattern enforces them too; the explicit check
// only gives a clearer error message.
const (
	SlugMinLength = 3
	SlugMaxLength = 28
)

// slugPattern is the exact assignment rule (Kubernetes namespace compatible).
var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

// ValidateSlug checks the format of slug. Uniqueness, including against
// destroyed tenants, is a persistence concern and is not checked here.
func ValidateSlug(slug string) error {
	if n := len(slug); n < SlugMinLength || n > SlugMaxLength {
		return &ValidationError{Field: "slug", Reason: fmt.Sprintf("must be %d-%d characters, got %d", SlugMinLength, SlugMaxLength, n)}
	}
	if !slugPattern.MatchString(slug) {
		return &ValidationError{Field: "slug", Reason: "must start with a lowercase letter, contain only lowercase letters, digits and hyphens, and not end with a hyphen"}
	}
	return nil
}

// ValidateName checks that name is not empty or whitespace only.
func ValidateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return &ValidationError{Field: "name", Reason: "must not be empty"}
	}
	return nil
}

// Tenant is a customer environment. It is only ever mutated through tasks.
type Tenant struct {
	ID     string
	Slug   string
	Name   string
	Status TenantStatus
	// Version is incremented on every mutation and used for optimistic locking.
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TenantChanges is a partial update. Nil fields are left unchanged.
type TenantChanges struct {
	Name *string
}

// NewTenant returns a provisioning tenant at InitialTenantVersion. The
// caller supplies id (from its identifier generator) and now (from its
// clock), and must create the accompanying deploy task.
func NewTenant(id, slug, name string, now time.Time) (Tenant, error) {
	if id == "" {
		return Tenant{}, &ValidationError{Field: "id", Reason: "must not be empty"}
	}
	if err := ValidateSlug(slug); err != nil {
		return Tenant{}, err
	}
	if err := ValidateName(name); err != nil {
		return Tenant{}, err
	}
	now = NormalizeTime(now)
	return Tenant{
		ID:        id,
		Slug:      slug,
		Name:      name,
		Status:    TenantStatusProvisioning,
		Version:   InitialTenantVersion,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// RequestUpdate applies changes and moves an active tenant to updating
// (PATCH). The caller must create the accompanying update task. A forbidden
// status yields an error matching both ErrTenantUpdateNotAllowed and
// ErrInvalidTransition; invalid changes yield a *ValidationError. Either
// way the tenant is left unchanged.
func (t *Tenant) RequestUpdate(changes TenantChanges, now time.Time) error {
	if changes.Name != nil {
		if err := ValidateName(*changes.Name); err != nil {
			return err
		}
	}
	if err := t.apply(TenantEventUpdateRequested, now); err != nil {
		return fmt.Errorf("%w: %w", ErrTenantUpdateNotAllowed, err)
	}
	if changes.Name != nil {
		t.Name = *changes.Name
	}
	return nil
}

// RequestDestroy moves an active or failed tenant to destroying (DELETE).
// The caller must create the accompanying destroy task. A forbidden status
// yields an error matching both ErrTenantUpdateNotAllowed and
// ErrInvalidTransition, and leaves the tenant unchanged.
func (t *Tenant) RequestDestroy(now time.Time) error {
	if err := t.apply(TenantEventDestroyRequested, now); err != nil {
		return fmt.Errorf("%w: %w", ErrTenantUpdateNotAllowed, err)
	}
	return nil
}

// ApplyTaskOutcome moves the tenant according to the terminal outcome of
// one of its tasks (a worker-applied change). It fails, leaving the tenant
// unchanged, if task belongs to another tenant, is not terminal, or its
// outcome is not allowed from the tenant's current status.
func (t *Tenant) ApplyTaskOutcome(task Task, now time.Time) error {
	if task.TenantID != t.ID {
		return &ValidationError{Field: "task tenant_id", Reason: fmt.Sprintf("task %s belongs to tenant %s, not %s", task.ID, task.TenantID, t.ID)}
	}
	event, err := TaskOutcomeEvent(task.Type, task.Status)
	if err != nil {
		return err
	}
	return t.apply(event, now)
}

// apply is the only place a tenant changes status. It enforces the
// transition table and bumps Version and UpdatedAt together, so no
// mutation can skip the version increment.
func (t *Tenant) apply(event TenantEvent, now time.Time) error {
	to, ok := NextTenantStatus(t.Status, event)
	if !ok {
		return &TransitionError{Entity: "tenant", From: string(t.Status), Trigger: string(event)}
	}
	t.Status = to
	t.Version++
	t.UpdatedAt = NormalizeTime(now)
	return nil
}
