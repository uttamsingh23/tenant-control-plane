package httpapi

import (
	"log/slog"
	"net/http"
	"time"
)

// NewRouter returns the HTTP handler for the whole API. requestTimeout
// bounds the context of each request; zero disables it.
func NewRouter(tenants TenantService, tasks TaskService, logger *slog.Logger, requestTimeout time.Duration) http.Handler {
	h := handlers{tenants: tenants, tasks: tasks, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)

	mux.Handle("POST /v1/tenants", h.handle(h.createTenant))
	mux.Handle("GET /v1/tenants", h.handle(h.listTenants))
	mux.Handle("GET /v1/tenants/{tenantID}", h.handle(h.getTenant))
	mux.Handle("PATCH /v1/tenants/{tenantID}", h.handle(h.updateTenant))
	mux.Handle("DELETE /v1/tenants/{tenantID}", h.handle(h.deleteTenant))

	mux.Handle("GET /v1/tasks", h.handle(h.listTasks))
	mux.Handle("GET /v1/tasks/{taskID}", h.handle(h.getTask))

	return logRequests(logger,
		recoverPanics(logger,
			withTimeout(requestTimeout,
				structuredRouteErrors(mux))))
}
