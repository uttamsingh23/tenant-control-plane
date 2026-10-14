package httpapi

import (
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// The types below are the public JSON contract. They are built from domain
// values field by field, so the API never exposes persistence rows and a
// new internal field never leaks into a response by accident.
//
// Requests only check the shape of the body (presence, null, JSON types).
// Rules about values, such as the slug format or a non-empty name, belong
// to the domain and come back from the use case as ValidationErrors.

// createTenantRequest is the body of POST /v1/tenants.
type createTenantRequest struct {
	Slug optional[string] `json:"slug"`
	Name optional[string] `json:"name"`
}

func (req createTenantRequest) input() (application.CreateTenantInput, error) {
	slug, err := req.Slug.required("slug")
	if err != nil {
		return application.CreateTenantInput{}, err
	}
	name, err := req.Name.required("name")
	if err != nil {
		return application.CreateTenantInput{}, err
	}
	return application.CreateTenantInput{Slug: slug, Name: name}, nil
}

// updateTenantRequest is the body of PATCH /v1/tenants/{tenantID}: the
// fields to change plus the version the client last read. Omitted fields
// are left unchanged. The slug is immutable and therefore not a field.
type updateTenantRequest struct {
	Name    optional[string] `json:"name"`
	Version optional[int64]  `json:"version"`
}

func (req updateTenantRequest) input() (application.UpdateTenantInput, error) {
	version, err := req.Version.required("version")
	if err != nil {
		return application.UpdateTenantInput{}, err
	}
	name, err := req.Name.patch("name")
	if err != nil {
		return application.UpdateTenantInput{}, err
	}
	return application.UpdateTenantInput{Name: name, ExpectedVersion: version}, nil
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

// listResponse is one page of a list endpoint. NextCursor is omitted on
// the last page.
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
