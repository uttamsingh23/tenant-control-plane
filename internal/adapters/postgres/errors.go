package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// constraintErrors maps the database constraints (and trigger checks named
// like constraints) defined in migrations/ to the domain errors they mean.
// Violations of any other constraint, such as the CHECKs mirroring domain
// validation, indicate a bug and are returned as opaque errors.
var constraintErrors = map[string]error{
	"tenants_slug_key":              domain.ErrTenantAlreadyExists,
	"tasks_tenant_id_fkey":          domain.ErrTenantNotFound,
	"tasks_one_open_per_tenant_idx": domain.ErrTenantUpdateNotAllowed,
	"tasks_terminal_immutable":      domain.ErrInvalidTransition,
	"outbox_events_task_id_fkey":    domain.ErrTaskNotFound,
}

// translate wraps a database error with the operation that failed and,
// when the error is a known constraint violation, with its domain error.
func translate(op string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if sentinel, ok := constraintErrors[pgErr.ConstraintName]; ok {
			return fmt.Errorf("%s: %w: %w", op, sentinel, err)
		}
	}
	return fmt.Errorf("%s: %w", op, err)
}

// isUUID reports whether s is a hyphenated UUID, the only form IDs take.
// Lookups by anything else cannot match a row, so they short-circuit to
// "not found" instead of surfacing a database type error.
func isUUID(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && strings.EqualFold(u.String(), s)
}
