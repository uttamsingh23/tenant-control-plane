package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

func TestTenantInsertGetRoundTrip(t *testing.T) {
	store, _ := newStore(t)
	// A non-UTC clock with sub-millisecond precision: what is stored and
	// read back is the normalized UTC millisecond value.
	ist := time.FixedZone("IST", 5*3600+1800)
	want := newTenant(t, "acme", time.Date(2026, 1, 2, 8, 34, 5, 6_789_123, ist))

	if err := store.Tenants().Insert(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Tenants().Get(t.Context(), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if got.CreatedAt.Location() != time.UTC || domain.FormatTimestamp(got.CreatedAt) != "2026-01-02T03:04:05.006Z" {
		t.Errorf("created_at = %v (%v), want 2026-01-02T03:04:05.006Z UTC", got.CreatedAt, got.CreatedAt.Location())
	}
}

func TestTenantGetNotFound(t *testing.T) {
	store, _ := newStore(t)
	insertTenant(t, store, "acme")
	for _, id := range []string{uuid.NewString(), "not-a-uuid", "", "urn:uuid:" + uuid.NewString()} {
		if _, err := store.Tenants().Get(t.Context(), id); !errors.Is(err, domain.ErrTenantNotFound) {
			t.Errorf("Get(%q) err = %v, want ErrTenantNotFound", id, err)
		}
	}
}

func TestTenantSlugUnique(t *testing.T) {
	t.Run("duplicate of live tenant", func(t *testing.T) {
		store, _ := newStore(t)
		insertTenant(t, store, "acme")
		err := store.Tenants().Insert(t.Context(), newTenant(t, "acme", base))
		if !errors.Is(err, domain.ErrTenantAlreadyExists) {
			t.Errorf("err = %v, want ErrTenantAlreadyExists", err)
		}
	})

	t.Run("duplicate of destroyed tenant", func(t *testing.T) {
		store, _ := newStore(t)
		tn := insertTenant(t, store, "acme")
		mutate(t, store, &tn, outcome(domain.TaskTypeDeploy, domain.TaskStatusDone))
		mutate(t, store, &tn, requestDestroy)
		mutate(t, store, &tn, outcome(domain.TaskTypeDestroy, domain.TaskStatusDone))
		if tn.Status != domain.TenantStatusDestroyed {
			t.Fatalf("setup: status = %s, want destroyed", tn.Status)
		}

		err := store.Tenants().Insert(t.Context(), newTenant(t, "acme", base))
		if !errors.Is(err, domain.ErrTenantAlreadyExists) {
			t.Errorf("err = %v, want ErrTenantAlreadyExists", err)
		}
	})

	t.Run("concurrent creates yield exactly one success", func(t *testing.T) {
		store, pool := newStore(t)
		const n = 16
		errs := parallel(n, func(int) error {
			tn := newTenant(t, "race", base)
			return store.WithinTx(context.Background(), func(ctx context.Context, r application.Repositories) error {
				return r.Tenants().Insert(ctx, tn)
			})
		})

		var ok, dup int
		for _, err := range errs {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, domain.ErrTenantAlreadyExists):
				dup++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}
		if ok != 1 || dup != n-1 {
			t.Errorf("successes = %d, duplicates = %d; want 1 and %d", ok, dup, n-1)
		}
		var rows int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM tenants WHERE slug = 'race'`).Scan(&rows); err != nil || rows != 1 {
			t.Errorf("rows with slug = %d (%v), want 1", rows, err)
		}
	})
}

func TestTenantUpdate(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	tn := insertTenant(t, store, "acme")

	// Success: every mutable field is persisted.
	mutate(t, store, &tn, outcome(domain.TaskTypeDeploy, domain.TaskStatusDone))
	name := "Renamed"
	mutate(t, store, &tn, func(tn *domain.Tenant) error {
		return tn.RequestUpdate(domain.TenantChanges{Name: &name}, tn.UpdatedAt.Add(time.Second))
	})
	got, err := store.Tenants().Get(ctx, tn.ID)
	if err != nil || got != tn {
		t.Fatalf("after update: got %+v, %v; want %+v", got, err, tn)
	}
	if got.Version != domain.InitialTenantVersion+2 {
		t.Errorf("version = %d, want %d", got.Version, domain.InitialTenantVersion+2)
	}

	t.Run("stale version", func(t *testing.T) {
		stale := tn
		stale.Version = tn.Version + 1
		err := store.Tenants().Update(ctx, stale, tn.Version-1)
		if !errors.Is(err, domain.ErrTenantVersionConflict) {
			t.Errorf("err = %v, want ErrTenantVersionConflict", err)
		}
	})

	t.Run("missing tenant", func(t *testing.T) {
		for _, id := range []string{uuid.NewString(), "not-a-uuid"} {
			missing := tn
			missing.ID = id
			missing.Version = tn.Version + 1
			if err := store.Tenants().Update(ctx, missing, tn.Version); !errors.Is(err, domain.ErrTenantNotFound) {
				t.Errorf("Update(id=%q) err = %v, want ErrTenantNotFound", id, err)
			}
		}
	})

	if after, _ := store.Tenants().Get(ctx, tn.ID); after != tn {
		t.Errorf("failed updates changed the row: got %+v, want %+v", after, tn)
	}
}

// TestTenantConcurrentUpdatesSameVersion is the PATCH race: every writer
// read the same version, exactly one may win.
func TestTenantConcurrentUpdatesSameVersion(t *testing.T) {
	store, _ := newStore(t)
	tn := insertTenant(t, store, "acme")
	mutate(t, store, &tn, outcome(domain.TaskTypeDeploy, domain.TaskStatusDone))
	observed := tn.Version

	const n = 16
	errs := parallel(n, func(i int) error {
		mine := tn // each writer works on its own copy of what it read
		name := fmt.Sprintf("writer-%d", i)
		if err := mine.RequestUpdate(domain.TenantChanges{Name: &name}, base.Add(time.Hour)); err != nil {
			return err
		}
		return store.WithinTx(context.Background(), func(ctx context.Context, r application.Repositories) error {
			return r.Tenants().Update(ctx, mine, observed)
		})
	})

	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			if winner != -1 {
				t.Errorf("writers %d and %d both succeeded", winner, i)
			}
			winner = i
		case !errors.Is(err, domain.ErrTenantVersionConflict):
			t.Errorf("writer %d: err = %v, want ErrTenantVersionConflict", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("no writer succeeded")
	}
	got, err := store.Tenants().Get(t.Context(), tn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != observed+1 || got.Name != fmt.Sprintf("writer-%d", winner) || got.Status != domain.TenantStatusUpdating {
		t.Errorf("got %+v, want winner %d's write at version %d", got, winner, observed+1)
	}
}

// TestTenantGuardTrigger checks the database rejects writes that skip the
// version increment or change identity columns, whoever issues them.
func TestTenantGuardTrigger(t *testing.T) {
	store, pool := newStore(t)
	ctx := t.Context()
	tn := insertTenant(t, store, "acme")

	for _, newVersion := range []int64{tn.Version, tn.Version + 2, tn.Version - 1} {
		bad := tn
		bad.Version = newVersion
		err := store.Tenants().Update(ctx, bad, tn.Version)
		if errors.Is(err, domain.ErrTenantVersionConflict) {
			t.Errorf("version %d -> %d reported as a conflict, want a guard violation", tn.Version, newVersion)
		}
		pgError(t, err, "23514", "tenants_version_increment")
	}

	for column, value := range map[string]string{"slug": "other", "id": uuid.NewString(), "created_at": "2020-01-01T00:00:00Z"} {
		_, err := pool.Exec(ctx, `UPDATE tenants SET `+column+` = $2, version = version + 1 WHERE id = $1`, tn.ID, value)
		pgError(t, err, "23514", "tenants_identity_immutable")
	}
}

// TestTenantCheckConstraints bypasses domain validation to prove the
// database enforces the same rules. Such violations are bugs, so they are
// not translated into domain errors (which might be shown to clients).
func TestTenantCheckConstraints(t *testing.T) {
	tests := []struct {
		name       string
		modify     func(*domain.Tenant)
		constraint string
	}{
		{"slug too short", func(tn *domain.Tenant) { tn.Slug = "ab" }, "tenants_slug_format"},
		{"slug too long", func(tn *domain.Tenant) { tn.Slug = "a" + strings.Repeat("b", 28) }, "tenants_slug_format"},
		{"slug uppercase", func(tn *domain.Tenant) { tn.Slug = "Acme" }, "tenants_slug_format"},
		{"slug trailing hyphen", func(tn *domain.Tenant) { tn.Slug = "acme-" }, "tenants_slug_format"},
		{"slug trailing newline", func(tn *domain.Tenant) { tn.Slug = "acme\n" }, "tenants_slug_format"},
		{"blank name", func(tn *domain.Tenant) { tn.Name = " \t" }, "tenants_name_not_blank"},
		{"unknown status", func(tn *domain.Tenant) { tn.Status = "paused" }, "tenants_status_valid"},
		{"version zero", func(tn *domain.Tenant) { tn.Version = 0 }, "tenants_version_positive"},
		{"sub-millisecond timestamp", func(tn *domain.Tenant) {
			tn.CreatedAt = tn.CreatedAt.Add(time.Microsecond)
			tn.UpdatedAt = tn.CreatedAt
		}, "tenants_timestamps_millisecond"},
		{"updated before created", func(tn *domain.Tenant) { tn.UpdatedAt = tn.CreatedAt.Add(-time.Second) }, "tenants_updated_not_before_created"},
	}
	store, _ := newStore(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tn := newTenant(t, "valid-slug", base)
			tt.modify(&tn)
			err := store.Tenants().Insert(t.Context(), tn)
			pgError(t, err, "23514", tt.constraint)
			if errors.Is(err, domain.ErrInvalidInput) || errors.Is(err, domain.ErrTenantAlreadyExists) {
				t.Errorf("check violation translated to a domain error: %v", err)
			}
		})
	}
	// Slugs matching the exact rule at the length boundaries are accepted.
	for _, slug := range []string{"abc", "a" + strings.Repeat("-", 26) + "z", "a--b"} {
		if err := store.Tenants().Insert(t.Context(), newTenant(t, slug, base)); err != nil {
			t.Errorf("slug %q rejected: %v", slug, err)
		}
	}
}

func TestTenantList(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()

	// Inserted newest first, with a created_at tie, to show ordering is by
	// (created_at, id) and not by insertion.
	var all []domain.Tenant
	for i := 4; i >= 0; i-- {
		at := base.Add(time.Duration(i/2) * time.Millisecond) // pairs share a timestamp
		tn := newTenant(t, fmt.Sprintf("tenant-%d", i), at)
		if err := store.Tenants().Insert(ctx, tn); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			mutate(t, store, &tn, outcome(domain.TaskTypeDeploy, domain.TaskStatusDone))
		}
		all = append(all, tn)
	}
	slices.SortFunc(all, func(a, b domain.Tenant) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})

	t.Run("pages through everything in order", func(t *testing.T) {
		var got []domain.Tenant
		var pages int
		page := application.PageRequest{Limit: 2}
		for {
			p, err := store.Tenants().List(ctx, application.TenantFilter{}, page)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			got = append(got, p.Items...)
			if p.NextCursor == "" {
				break
			}
			page.Cursor = p.NextCursor
		}
		if pages != 3 || !slices.Equal(got, all) {
			t.Errorf("pages = %d, items = %v; want 3 pages of %v", pages, ids(got), ids(all))
		}
	})

	t.Run("filters by status", func(t *testing.T) {
		p, err := store.Tenants().List(ctx, application.TenantFilter{Status: domain.TenantStatusActive}, application.PageRequest{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		want := slices.DeleteFunc(slices.Clone(all), func(tn domain.Tenant) bool { return tn.Status != domain.TenantStatusActive })
		if !slices.Equal(p.Items, want) || p.NextCursor != "" {
			t.Errorf("got %v (next %q), want %v", ids(p.Items), p.NextCursor, ids(want))
		}
	})

	t.Run("exact final page has no cursor", func(t *testing.T) {
		p, err := store.Tenants().List(ctx, application.TenantFilter{}, application.PageRequest{Limit: len(all)})
		if err != nil || len(p.Items) != len(all) || p.NextCursor != "" {
			t.Errorf("got %d items, next %q, err %v; want %d, none", len(p.Items), p.NextCursor, err, len(all))
		}
	})

	t.Run("malformed cursor", func(t *testing.T) {
		for _, c := range []string{"!!!", "e30", "eyJ0IjoieCJ9"} { // bad base64, {}, {"t":"x"}
			_, err := store.Tenants().List(ctx, application.TenantFilter{}, application.PageRequest{Cursor: c})
			if !errors.Is(err, domain.ErrInvalidInput) {
				t.Errorf("cursor %q: err = %v, want ErrInvalidInput", c, err)
			}
		}
	})
}

func ids[T interface{ domain.Tenant | domain.Task }](items []T) []string {
	out := make([]string, len(items))
	for i, it := range items {
		switch v := any(it).(type) {
		case domain.Tenant:
			out[i] = v.Slug
		case domain.Task:
			out[i] = v.ID[:8]
		}
	}
	return out
}
