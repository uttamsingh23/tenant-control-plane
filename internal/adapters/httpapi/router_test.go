package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// fakeTenants returns err from every call.
type fakeTenants struct{ err error }

func (f fakeTenants) Create(context.Context, application.CreateTenantInput) (application.TenantMutation, error) {
	return application.TenantMutation{}, f.err
}

func (f fakeTenants) Get(context.Context, string) (domain.Tenant, error) {
	return domain.Tenant{}, f.err
}

func (f fakeTenants) List(context.Context, application.TenantFilter, application.PageRequest) (application.Page[domain.Tenant], error) {
	return application.Page[domain.Tenant]{}, f.err
}

func (f fakeTenants) Update(context.Context, string, application.UpdateTenantInput) (application.TenantMutation, error) {
	return application.TenantMutation{}, f.err
}

func (f fakeTenants) Delete(context.Context, string) (application.TenantMutation, error) {
	return application.TenantMutation{}, f.err
}

// fakeTasks returns task (or err) from Get and an empty page from List.
type fakeTasks struct {
	task domain.Task
	err  error
}

func (f fakeTasks) Get(context.Context, string) (domain.Task, error) { return f.task, f.err }

func (f fakeTasks) List(context.Context, application.TaskFilter, application.PageRequest) (application.Page[domain.Task], error) {
	return application.Page[domain.Task]{}, f.err
}

func newTestRouter(tenants TenantService, tasks TaskService) http.Handler {
	return NewRouter(tenants, tasks, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func do(t *testing.T, h http.Handler, method, target, body string) (*http.Response, errorResponse) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := rec.Result()
	var e errorResponse
	if res.StatusCode >= 400 {
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			t.Fatalf("error body is not structured JSON: %v", err)
		}
	}
	return res, e
}

func TestHealth(t *testing.T) {
	res, _ := do(t, newTestRouter(fakeTenants{}, fakeTasks{}), http.MethodGet, "/healthz", "")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{domain.ErrTenantNotFound, http.StatusNotFound, "tenant_not_found"},
		{domain.ErrTenantAlreadyExists, http.StatusConflict, "tenant_already_exists"},
		{domain.ErrTenantUpdateNotAllowed, http.StatusConflict, "tenant_update_not_allowed"},
		{domain.ErrTenantVersionConflict, http.StatusConflict, "tenant_version_conflict"},
		{domain.ErrInvalidInput, http.StatusBadRequest, "invalid_request"},
		{domain.ErrNotImplemented, http.StatusNotImplemented, "not_implemented"},
		{errors.New("boom"), http.StatusInternalServerError, "internal_error"},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			h := newTestRouter(fakeTenants{err: fmt.Errorf("wrapped: %w", tt.err)}, fakeTasks{})
			res, body := do(t, h, http.MethodGet, "/v1/tenants/abc", "")
			if res.StatusCode != tt.status || body.Error.Code != tt.code {
				t.Errorf("got %d %q, want %d %q", res.StatusCode, body.Error.Code, tt.status, tt.code)
			}
		})
	}
}

func TestTaskNotFound(t *testing.T) {
	res, body := do(t, newTestRouter(fakeTenants{}, fakeTasks{err: domain.ErrTaskNotFound}), http.MethodGet, "/v1/tasks/x", "")
	if res.StatusCode != http.StatusNotFound || body.Error.Code != "task_not_found" {
		t.Errorf("got %d %q", res.StatusCode, body.Error.Code)
	}
}

func TestTaskResponseTimestamps(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 6_789_000, time.FixedZone("CET", 3600))
	h := newTestRouter(fakeTenants{}, fakeTasks{task: domain.Task{ID: "t1", CreatedAt: ts, UpdatedAt: ts}})
	res, _ := do(t, h, http.MethodGet, "/v1/tasks/t1", "")
	var got taskResponse
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if want := "2026-01-02T02:04:05.006Z"; got.CreatedAt != want {
		t.Errorf("created_at = %q, want %q", got.CreatedAt, want)
	}
}

func TestEmptyListEncodesItemsAsArray(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/tasks", nil)
	rec := httptest.NewRecorder()
	newTestRouter(fakeTenants{}, fakeTasks{}).ServeHTTP(rec, req)
	if got := strings.TrimSpace(rec.Body.String()); got != `{"items":[]}` {
		t.Errorf("body = %s", got)
	}
}

func TestRequestValidation(t *testing.T) {
	h := newTestRouter(fakeTenants{err: domain.ErrNotImplemented}, fakeTasks{})
	tests := []struct {
		name, method, target, body string
	}{
		{"malformed json", http.MethodPost, "/v1/tenants", `{"slug":`},
		{"unknown field", http.MethodPost, "/v1/tenants", `{"slug":"a","nope":1}`},
		{"trailing data", http.MethodPost, "/v1/tenants", `{"slug":"a"}{}`},
		{"missing version", http.MethodPatch, "/v1/tenants/abc", `{"name":"x"}`},
		{"bad limit", http.MethodGet, "/v1/tenants?limit=0", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, body := do(t, h, tt.method, tt.target, tt.body)
			if res.StatusCode != http.StatusBadRequest || body.Error.Code != "invalid_request" {
				t.Errorf("got %d %q, want 400 invalid_request", res.StatusCode, body.Error.Code)
			}
		})
	}
}

func TestStructuredRouteErrors(t *testing.T) {
	h := newTestRouter(fakeTenants{}, fakeTasks{})

	res, body := do(t, h, http.MethodGet, "/nope", "")
	if res.StatusCode != http.StatusNotFound || body.Error.Code != "route_not_found" {
		t.Errorf("unknown route: got %d %q", res.StatusCode, body.Error.Code)
	}

	res, body = do(t, h, http.MethodPut, "/v1/tenants/abc", "")
	if res.StatusCode != http.StatusMethodNotAllowed || body.Error.Code != "method_not_allowed" {
		t.Errorf("wrong method: got %d %q", res.StatusCode, body.Error.Code)
	}
	if allow := res.Header.Get("Allow"); !strings.Contains(allow, http.MethodPatch) {
		t.Errorf("Allow = %q, want it to include PATCH", allow)
	}
}

type panickingTasks struct{ fakeTasks }

func (panickingTasks) Get(context.Context, string) (domain.Task, error) { panic("boom") }

func TestRecoverPanics(t *testing.T) {
	res, body := do(t, newTestRouter(fakeTenants{}, panickingTasks{}), http.MethodGet, "/v1/tasks/x", "")
	if res.StatusCode != http.StatusInternalServerError || body.Error.Code != "internal_error" {
		t.Errorf("got %d %q", res.StatusCode, body.Error.Code)
	}
}

func TestServeShutsDownGracefully(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	srv := NewServer(config.HTTP{ReadHeaderTimeout: time.Second}, newTestRouter(fakeTenants{}, fakeTasks{}))

	done := make(chan error, 1)
	go func() { done <- Serve(ctx, srv, ln, 5*time.Second) }()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request to running server: %v", err)
	}
	_ = res.Body.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
}
