package application

import (
	"context"
	"errors"
	"testing"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// passthroughTx runs fn without a real transaction.
type passthroughTx struct{}

func (passthroughTx) WithinTx(ctx context.Context, fn func(context.Context, Repositories) error) error {
	return fn(ctx, nil)
}

// Until the lifecycle is implemented, mutations must fail loudly rather
// than report success.
func TestTenantMutationsAreNotSilentlySuccessful(t *testing.T) {
	svc := NewTenantService(passthroughTx{}, nil)
	ctx := t.Context()

	_, errCreate := svc.Create(ctx, CreateTenantInput{Slug: "acme", Name: "Acme"})
	_, errUpdate := svc.Update(ctx, "id", UpdateTenantInput{ExpectedVersion: 1})
	_, errDelete := svc.Delete(ctx, "id")
	errApply := NewTaskService(passthroughTx{}, nil).ApplyUpdate(ctx, domain.TaskUpdate{})

	for name, err := range map[string]error{
		"create": errCreate, "update": errUpdate, "delete": errDelete, "apply update": errApply,
	} {
		if !errors.Is(err, domain.ErrNotImplemented) {
			t.Errorf("%s: err = %v, want ErrNotImplemented", name, err)
		}
	}
}
