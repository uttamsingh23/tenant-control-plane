// Package application contains the use cases of the control plane. It
// orchestrates domain rules, persistence and event publication through the
// interfaces declared in ports.go and knows nothing about HTTP or SQL.
package application

import "github.com/uttamsingh23/tenant-control-plane/internal/domain"

// Page size bounds applied to every list query.
const (
	DefaultPageLimit = 50
	MaxPageLimit     = 200
)

// PageRequest selects one page of a keyset-paginated list.
type PageRequest struct {
	Limit int
	// Cursor is the opaque NextCursor of the previous page; empty for the first page.
	Cursor string
}

// normalized returns p with Limit clamped to [1, MaxPageLimit].
func (p PageRequest) normalized() PageRequest {
	switch {
	case p.Limit <= 0:
		p.Limit = DefaultPageLimit
	case p.Limit > MaxPageLimit:
		p.Limit = MaxPageLimit
	}
	return p
}

// Page is one page of results. NextCursor is empty on the last page.
type Page[T any] struct {
	Items      []T
	NextCursor string
}

// TenantFilter narrows a tenant list. Zero-valued fields do not filter.
type TenantFilter struct {
	Status domain.TenantStatus
}

// TaskFilter narrows a task list. Zero-valued fields do not filter.
type TaskFilter struct {
	TenantID string
	Status   domain.TaskStatus
}
