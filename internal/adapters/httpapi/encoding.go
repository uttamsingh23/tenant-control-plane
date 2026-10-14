package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

const maxBodyBytes = 1 << 20

// timestampLayout is ISO 8601 in UTC with millisecond precision and a Z suffix.
const timestampLayout = "2006-01-02T15:04:05.000Z"

func formatTime(t time.Time) string { return t.UTC().Format(timestampLayout) }

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line is already sent; an encode error here means the
	// client went away and there is nothing useful left to do.
	_ = json.NewEncoder(w).Encode(body)
}

// decodeJSON decodes a single JSON object from the request body into dst,
// rejecting unknown fields and trailing data.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: malformed JSON body: %w", errInvalidRequest, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: body must contain a single JSON object", errInvalidRequest)
	}
	return nil
}

func parsePageRequest(r *http.Request) (application.PageRequest, error) {
	q := r.URL.Query()
	page := application.PageRequest{Cursor: q.Get("cursor")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return page, fmt.Errorf("%w: limit must be a positive integer", errInvalidRequest)
		}
		page.Limit = n
	}
	return page, nil
}

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type updateTenantRequest struct {
	Name    *string `json:"name"`
	Version *int64  `json:"version"`
}

type tenantResponse struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func newTenantResponse(t domain.Tenant) tenantResponse {
	return tenantResponse{
		ID:        t.ID,
		Slug:      t.Slug,
		Name:      t.Name,
		Status:    string(t.Status),
		Version:   t.Version,
		CreatedAt: formatTime(t.CreatedAt),
		UpdatedAt: formatTime(t.UpdatedAt),
	}
}

type taskResponse struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func newTaskResponse(t domain.Task) taskResponse {
	return taskResponse{
		ID:        t.ID,
		Type:      string(t.Type),
		TenantID:  t.TenantID,
		Status:    string(t.Status),
		CreatedAt: formatTime(t.CreatedAt),
		UpdatedAt: formatTime(t.UpdatedAt),
	}
}

// tenantMutationResponse is returned by create, update and delete: the
// tenant after the change and the task tracking the asynchronous work.
type tenantMutationResponse struct {
	Tenant tenantResponse `json:"tenant"`
	Task   taskResponse   `json:"task"`
}

func newTenantMutationResponse(m application.TenantMutation) tenantMutationResponse {
	return tenantMutationResponse{Tenant: newTenantResponse(m.Tenant), Task: newTaskResponse(m.Task)}
}

type listResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func newListResponse[S, T any](page application.Page[S], convert func(S) T) listResponse[T] {
	items := make([]T, 0, len(page.Items))
	for _, s := range page.Items {
		items = append(items, convert(s))
	}
	return listResponse[T]{Items: items, NextCursor: page.NextCursor}
}
