// Package domain holds the core types of the tenant provisioning control plane.
//
// It must not depend on transport, storage or broker packages: everything
// here is plain Go so it can be reasoned about and tested in isolation.
package domain

import "time"

// TenantStatus is a state in the tenant state machine.
type TenantStatus string

// Tenant states. See the README for the allowed transitions.
const (
	TenantStatusProvisioning TenantStatus = "provisioning"
	TenantStatusActive       TenantStatus = "active"
	TenantStatusUpdating     TenantStatus = "updating"
	TenantStatusDestroying   TenantStatus = "destroying"
	TenantStatusDestroyed    TenantStatus = "destroyed"
	TenantStatusFailed       TenantStatus = "failed"
)

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
