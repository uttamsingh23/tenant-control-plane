package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type tenantHandler struct {
	svc    TenantService
	logger *slog.Logger
}

func (h tenantHandler) create(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	m, err := h.svc.Create(r.Context(), application.CreateTenantInput{Slug: req.Slug, Name: req.Name})
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	w.Header().Set("Location", "/v1/tenants/"+m.Tenant.ID)
	writeJSON(w, http.StatusCreated, newTenantMutationResponse(m))
}

func (h tenantHandler) list(w http.ResponseWriter, r *http.Request) {
	page, err := parsePageRequest(r)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	filter := application.TenantFilter{Status: domain.TenantStatus(r.URL.Query().Get("status"))}
	result, err := h.svc.List(r.Context(), filter, page)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newListResponse(result, newTenantResponse))
}

func (h tenantHandler) get(w http.ResponseWriter, r *http.Request) {
	t, err := h.svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newTenantResponse(t))
}

func (h tenantHandler) update(w http.ResponseWriter, r *http.Request) {
	var req updateTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	if req.Version == nil {
		writeError(w, r, h.logger, fmt.Errorf("%w: version is required", errInvalidRequest))
		return
	}
	m, err := h.svc.Update(r.Context(), r.PathValue("id"), application.UpdateTenantInput{
		Name:            req.Name,
		ExpectedVersion: *req.Version,
	})
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusAccepted, newTenantMutationResponse(m))
}

func (h tenantHandler) delete(w http.ResponseWriter, r *http.Request) {
	m, err := h.svc.Delete(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusAccepted, newTenantMutationResponse(m))
}

type taskHandler struct {
	svc    TaskService
	logger *slog.Logger
}

func (h taskHandler) list(w http.ResponseWriter, r *http.Request) {
	h.listFiltered(w, r, application.TaskFilter{
		TenantID: r.URL.Query().Get("tenant_id"),
		Status:   domain.TaskStatus(r.URL.Query().Get("status")),
	})
}

func (h taskHandler) listForTenant(w http.ResponseWriter, r *http.Request) {
	h.listFiltered(w, r, application.TaskFilter{
		TenantID: r.PathValue("id"),
		Status:   domain.TaskStatus(r.URL.Query().Get("status")),
	})
}

func (h taskHandler) listFiltered(w http.ResponseWriter, r *http.Request, filter application.TaskFilter) {
	page, err := parsePageRequest(r)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	result, err := h.svc.List(r.Context(), filter, page)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newListResponse(result, newTaskResponse))
}

func (h taskHandler) get(w http.ResponseWriter, r *http.Request) {
	t, err := h.svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newTaskResponse(t))
}
