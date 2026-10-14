package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// Error codes returned in the "code" field of every error body. The first
// five are the contract of the brief; the rest describe HTTP-level problems.
const (
	codeTenantNotFound         = "tenant_not_found"
	codeTenantAlreadyExists    = "tenant_already_exists"
	codeTenantUpdateNotAllowed = "tenant_update_not_allowed"
	codeTenantVersionConflict  = "tenant_version_conflict"
	codeTaskNotFound           = "task_not_found"

	codeInvalidRequest       = "invalid_request"
	codeUnsupportedMediaType = "unsupported_media_type"
	codePayloadTooLarge      = "payload_too_large"
	codeRouteNotFound        = "route_not_found"
	codeMethodNotAllowed     = "method_not_allowed"
	codeRequestTimeout       = "request_timeout"
	codeInternal             = "internal_error"
)

// errorResponse is the body of every error response.
type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// Field names the offending input (body field or query parameter)
	// when the error is about one.
	Field string `json:"field,omitempty"`
}

// contractErrors maps the domain sentinels to their responses. The message
// is the sentinel's fixed text, never err.Error(): the wrapped chain carries
// IDs and, for errors from the postgres adapter, the database error.
var contractErrors = []struct {
	target error
	status int
	code   string
}{
	{domain.ErrTenantNotFound, http.StatusNotFound, codeTenantNotFound},
	{domain.ErrTaskNotFound, http.StatusNotFound, codeTaskNotFound},
	{domain.ErrTenantAlreadyExists, http.StatusConflict, codeTenantAlreadyExists},
	{domain.ErrTenantVersionConflict, http.StatusConflict, codeTenantVersionConflict},
	{domain.ErrTenantUpdateNotAllowed, http.StatusConflict, codeTenantUpdateNotAllowed},
}

// requestError is a problem with the request itself (headers, body or
// query string), found by the HTTP layer before any use case runs. Its
// message is written for the client.
type requestError struct {
	status  int
	code    string
	field   string
	message string
}

func (e *requestError) Error() string { return e.message }

// invalidRequest returns a 400 invalid_request error about field ("" if
// the error concerns the request as a whole).
func invalidRequest(field, format string, args ...any) error {
	return &requestError{
		status:  http.StatusBadRequest,
		code:    codeInvalidRequest,
		field:   field,
		message: fmt.Sprintf(format, args...),
	}
}

// classify is the single place where errors become responses. It reports
// whether err was expected (a client error or a timeout) or internal; only
// internal errors carry details worth logging, and those details never
// reach the client.
func classify(ctx context.Context, err error) (status int, detail errorDetail, internal bool) {
	var reqErr *requestError
	if errors.As(err, &reqErr) {
		return reqErr.status, errorDetail{Code: reqErr.code, Message: reqErr.message, Field: reqErr.field}, false
	}
	for _, m := range contractErrors {
		if errors.Is(err, m.target) {
			return m.status, errorDetail{Code: m.code, Message: m.target.Error()}, false
		}
	}
	// Domain validation messages name the field and the rule broken, e.g.
	// "invalid slug: must be 3-28 characters, got 2". Like the sentinels,
	// only the ValidationError itself is shown, not what wraps it.
	var valErr *domain.ValidationError
	if errors.As(err, &valErr) {
		return http.StatusBadRequest, errorDetail{Code: codeInvalidRequest, Message: valErr.Error(), Field: valErr.Field}, false
	}
	// The request deadline expired (or the client went away) while a use
	// case was running. Checked on the context rather than on err, because
	// how a database driver reports cancellation is its own business.
	if ctx.Err() != nil {
		return http.StatusServiceUnavailable, errorDetail{Code: codeRequestTimeout, Message: "request did not complete in time"}, false
	}
	return http.StatusInternalServerError, errorDetail{Code: codeInternal, Message: "internal server error"}, true
}

// writeError writes the structured response for err and logs it if it is
// internal.
func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	status, detail, internal := classify(r.Context(), err)
	if internal {
		logger.ErrorContext(r.Context(), "request failed",
			"request_id", w.Header().Get(requestIDHeader), "method", r.Method, "path", r.URL.Path, "err", err)
	}
	writeJSON(w, status, errorResponse{Error: detail})
}

func writeInternalError(w http.ResponseWriter) {
	writeJSON(w, http.StatusInternalServerError, errorResponse{
		Error: errorDetail{Code: codeInternal, Message: "internal server error"},
	})
}
