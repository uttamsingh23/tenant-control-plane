package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors forming the error contract of the control plane.
// Adapters translate these into transport-specific responses.
var (
	ErrTenantNotFound         = errors.New("tenant not found")
	ErrTenantAlreadyExists    = errors.New("tenant already exists")
	ErrTenantUpdateNotAllowed = errors.New("tenant update not allowed in current status")
	ErrTenantVersionConflict  = errors.New("tenant version conflict")
	ErrTaskNotFound           = errors.New("task not found")

	// ErrInvalidInput is matched by every *ValidationError.
	ErrInvalidInput = errors.New("invalid input")

	// ErrInvalidTransition is matched by every *TransitionError.
	ErrInvalidTransition = errors.New("invalid state transition")

	// ErrNotImplemented marks scaffolding that has not been built yet.
	// Stubs return it instead of silently reporting success.
	ErrNotImplemented = errors.New("not implemented")
)

// ValidationError reports a field that violates a domain rule.
// errors.Is(err, ErrInvalidInput) holds for every ValidationError.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

// Unwrap lets errors.Is match ErrInvalidInput.
func (e *ValidationError) Unwrap() error { return ErrInvalidInput }

// TransitionError reports a state change the state machine forbids.
// errors.Is(err, ErrInvalidTransition) holds for every TransitionError.
type TransitionError struct {
	// Entity is "tenant" or "task".
	Entity string
	// From is the status the entity was in.
	From string
	// Trigger is what was attempted: a TenantEvent for tenants, the
	// requested TaskStatus for tasks.
	Trigger string
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: %s not allowed from status %s", e.Entity, e.Trigger, e.From)
}

// Unwrap lets errors.Is match ErrInvalidTransition.
func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }
