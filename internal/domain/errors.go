package domain

import "errors"

// Sentinel errors forming the error contract of the control plane.
// Adapters translate these into transport-specific responses.
var (
	ErrTenantNotFound         = errors.New("tenant not found")
	ErrTenantAlreadyExists    = errors.New("tenant already exists")
	ErrTenantUpdateNotAllowed = errors.New("tenant update not allowed in current status")
	ErrTenantVersionConflict  = errors.New("tenant version conflict")
	ErrTaskNotFound           = errors.New("task not found")

	// ErrInvalidInput is wrapped by validation failures, for example
	// fmt.Errorf("slug %q: %w", slug, ErrInvalidInput).
	ErrInvalidInput = errors.New("invalid input")

	// ErrNotImplemented marks scaffolding that has not been built yet.
	// Stubs return it instead of silently reporting success.
	ErrNotImplemented = errors.New("not implemented")
)
