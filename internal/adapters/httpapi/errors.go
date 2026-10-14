package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// errInvalidRequest marks malformed requests detected by the HTTP layer
// (bad JSON, bad query parameters, missing required fields).
var errInvalidRequest = errors.New("invalid request")

// errorResponse is the body of every error response.
type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// errorMapping maps an error (matched with errors.Is) to a response.
// Detailed mappings expose the full error message to the client; the rest
// expose only the sentinel's message.
type errorMapping struct {
	target   error
	status   int
	code     string
	detailed bool
}

var errorMappings = []errorMapping{
	{target: domain.ErrTenantNotFound, status: http.StatusNotFound, code: "tenant_not_found"},
	{target: domain.ErrTaskNotFound, status: http.StatusNotFound, code: "task_not_found"},
	{target: domain.ErrTenantAlreadyExists, status: http.StatusConflict, code: "tenant_already_exists"},
	{target: domain.ErrTenantUpdateNotAllowed, status: http.StatusConflict, code: "tenant_update_not_allowed"},
	{target: domain.ErrTenantVersionConflict, status: http.StatusConflict, code: "tenant_version_conflict"},
	{target: domain.ErrInvalidInput, status: http.StatusBadRequest, code: "invalid_request", detailed: true},
	{target: errInvalidRequest, status: http.StatusBadRequest, code: "invalid_request", detailed: true},
	{target: domain.ErrNotImplemented, status: http.StatusNotImplemented, code: "not_implemented"},
}

// writeError translates err into a structured error response. Unknown
// errors are logged and reported as an opaque internal_error.
func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	for _, m := range errorMappings {
		if !errors.Is(err, m.target) {
			continue
		}
		msg := m.target.Error()
		if m.detailed {
			msg = err.Error()
		}
		writeJSON(w, m.status, errorResponse{Error: errorDetail{Code: m.code, Message: msg}})
		return
	}
	logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeInternalError(w)
}

func writeInternalError(w http.ResponseWriter) {
	writeJSON(w, http.StatusInternalServerError, errorResponse{
		Error: errorDetail{Code: "internal_error", Message: "internal server error"},
	})
}
