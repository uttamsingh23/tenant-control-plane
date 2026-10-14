package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// NewRouter returns the HTTP handler for the whole API.
func NewRouter(tenants TenantService, tasks TaskService, logger *slog.Logger) http.Handler {
	th := tenantHandler{svc: tenants, logger: logger}
	kh := taskHandler{svc: tasks, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)

	mux.HandleFunc("POST /v1/tenants", th.create)
	mux.HandleFunc("GET /v1/tenants", th.list)
	mux.HandleFunc("GET /v1/tenants/{id}", th.get)
	mux.HandleFunc("PATCH /v1/tenants/{id}", th.update)
	mux.HandleFunc("DELETE /v1/tenants/{id}", th.delete)
	mux.HandleFunc("GET /v1/tenants/{id}/tasks", kh.listForTenant)

	mux.HandleFunc("GET /v1/tasks", kh.list)
	mux.HandleFunc("GET /v1/tasks/{id}", kh.get)

	return recoverPanics(logger, structuredRouteErrors(mux))
}

// structuredRouteErrors replaces ServeMux's plain-text 404 and 405
// responses with structured error bodies, preserving the Allow header.
func structuredRouteErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{header: http.Header{}}
		h.ServeHTTP(rec, r)
		if rec.status == http.StatusMethodNotAllowed {
			w.Header().Set("Allow", rec.header.Get("Allow"))
			writeJSON(w, http.StatusMethodNotAllowed, errorResponse{
				Error: errorDetail{Code: "method_not_allowed", Message: "method not allowed"},
			})
			return
		}
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: errorDetail{Code: "route_not_found", Message: "route not found"},
		})
	})
}

// statusRecorder captures the status and headers of a response, discarding the body.
type statusRecorder struct {
	header http.Header
	status int
}

func (s *statusRecorder) Header() http.Header         { return s.header }
func (s *statusRecorder) Write(b []byte) (int, error) { return len(b), nil }
func (s *statusRecorder) WriteHeader(status int)      { s.status = status }

// recoverPanics turns a handler panic into a logged, structured 500
// instead of a dropped connection.
func recoverPanics(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(v)
			}
			logger.ErrorContext(r.Context(), "panic serving request",
				"method", r.Method, "path", r.URL.Path, "panic", v, "stack", string(debug.Stack()))
			writeInternalError(w)
		}()
		next.ServeHTTP(w, r)
	})
}
