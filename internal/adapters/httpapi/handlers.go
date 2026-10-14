package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

func tenantPath(id string) string { return "/v1/tenants/" + id }
func taskPath(id string) string   { return "/v1/tasks/" + id }

// handlers translates between HTTP and the use cases. Every handler
// returns its error instead of writing it, so all failures go through
// writeError and its single mapping.
type handlers struct {
	tenants TenantService
	tasks   TaskService
	logger  *slog.Logger
}

// handle adapts an error-returning handler to http.HandlerFunc.
func (h handlers) handle(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			writeError(w, r, h.logger, err)
		}
	}
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h handlers) createTenant(w http.ResponseWriter, r *http.Request) error {
	var req createTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return err
	}
	in, err := req.input()
	if err != nil {
		return err
	}
	m, err := h.tenants.Create(r.Context(), in)
	if err != nil {
		return err
	}
	w.Header().Set("Location", tenantPath(m.Tenant.ID))
	writeJSON(w, http.StatusCreated, newTenantMutationResponse(m))
	return nil
}

func (h handlers) listTenants(w http.ResponseWriter, r *http.Request) error {
	q, page, err := parseQuery(r, "status")
	if err != nil {
		return err
	}
	status, err := statusParam[domain.TenantStatus](q)
	if err != nil {
		return err
	}
	result, err := h.tenants.List(r.Context(), application.TenantFilter{Status: status}, page)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, newListResponse(result, newTenantResponse))
	return nil
}

func (h handlers) getTenant(w http.ResponseWriter, r *http.Request) error {
	t, err := h.tenants.Get(r.Context(), r.PathValue("tenantID"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, newTenantResponse(t))
	return nil
}

// updateTenant answers 202: the tenant is now updating, and the returned
// task (also linked by Location) tracks the asynchronous change.
func (h handlers) updateTenant(w http.ResponseWriter, r *http.Request) error {
	var req updateTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return err
	}
	in, err := req.input()
	if err != nil {
		return err
	}
	m, err := h.tenants.Update(r.Context(), r.PathValue("tenantID"), in)
	if err != nil {
		return err
	}
	writeAccepted(w, m)
	return nil
}

// deleteTenant answers 202: the tenant is now destroying, and the returned
// task (also linked by Location) tracks the asynchronous teardown. The
// tenant stays readable and ends in status destroyed.
func (h handlers) deleteTenant(w http.ResponseWriter, r *http.Request) error {
	m, err := h.tenants.Delete(r.Context(), r.PathValue("tenantID"))
	if err != nil {
		return err
	}
	writeAccepted(w, m)
	return nil
}

func writeAccepted(w http.ResponseWriter, m application.TenantMutation) {
	w.Header().Set("Location", taskPath(m.Task.ID))
	writeJSON(w, http.StatusAccepted, newTenantMutationResponse(m))
}

func (h handlers) listTasks(w http.ResponseWriter, r *http.Request) error {
	q, page, err := parseQuery(r, "tenant_id", "status")
	if err != nil {
		return err
	}
	status, err := statusParam[domain.TaskStatus](q)
	if err != nil {
		return err
	}
	filter := application.TaskFilter{TenantID: q.Get("tenant_id"), Status: status}
	result, err := h.tasks.List(r.Context(), filter, page)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, newListResponse(result, newTaskResponse))
	return nil
}

func (h handlers) getTask(w http.ResponseWriter, r *http.Request) error {
	t, err := h.tasks.Get(r.Context(), r.PathValue("taskID"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, newTaskResponse(t))
	return nil
}
