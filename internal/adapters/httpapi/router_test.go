package httpapi

// Unit tests of the HTTP adapter against recording fakes of the use cases:
// request decoding and validation, response shapes, error mapping and the
// middleware. tests/api_test.go runs the same API end to end on PostgreSQL.

import (
	"bytes"
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

var (
	ts0        = time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)
	tenantA    = domain.Tenant{ID: "ten-1", Slug: "acme", Name: "Acme", Status: domain.TenantStatusProvisioning, Version: 1, CreatedAt: ts0, UpdatedAt: ts0}
	deployTask = domain.Task{ID: "task-1", Type: domain.TaskTypeDeploy, TenantID: "ten-1", Status: domain.TaskStatusAccepted, CreatedAt: ts0, UpdatedAt: ts0}
)

// fakeTenants records the input of the last call and returns err, or a
// canned result. With block set it waits for the request context to end.
type fakeTenants struct {
	err   error
	block bool

	calls    int
	createIn application.CreateTenantInput
	id       string
	updateIn application.UpdateTenantInput
	filter   application.TenantFilter
	page     application.PageRequest
}

func (f *fakeTenants) result(ctx context.Context) error {
	f.calls++
	if f.block {
		<-ctx.Done()
		return fmt.Errorf("query: %w", ctx.Err())
	}
	return f.err
}

func (f *fakeTenants) Create(ctx context.Context, in application.CreateTenantInput) (application.TenantMutation, error) {
	f.createIn = in
	return application.TenantMutation{Tenant: tenantA, Task: deployTask}, f.result(ctx)
}

func (f *fakeTenants) Get(ctx context.Context, id string) (domain.Tenant, error) {
	f.id = id
	return tenantA, f.result(ctx)
}

func (f *fakeTenants) List(ctx context.Context, filter application.TenantFilter, page application.PageRequest) (application.Page[domain.Tenant], error) {
	f.filter, f.page = filter, page
	return application.Page[domain.Tenant]{Items: []domain.Tenant{tenantA}, NextCursor: "next"}, f.result(ctx)
}

func (f *fakeTenants) Update(ctx context.Context, id string, in application.UpdateTenantInput) (application.TenantMutation, error) {
	f.id, f.updateIn = id, in
	updated := tenantA
	updated.Status, updated.Version = domain.TenantStatusUpdating, in.ExpectedVersion+1
	task := deployTask
	task.ID, task.Type = "task-2", domain.TaskTypeUpdate
	return application.TenantMutation{Tenant: updated, Task: task}, f.result(ctx)
}

func (f *fakeTenants) Delete(ctx context.Context, id string) (application.TenantMutation, error) {
	f.id = id
	destroying := tenantA
	destroying.Status = domain.TenantStatusDestroying
	task := deployTask
	task.ID, task.Type = "task-3", domain.TaskTypeDestroy
	return application.TenantMutation{Tenant: destroying, Task: task}, f.result(ctx)
}

type fakeTasks struct {
	task   domain.Task
	err    error
	filter application.TaskFilter
	page   application.PageRequest
}

func (f *fakeTasks) Get(context.Context, string) (domain.Task, error) { return f.task, f.err }

func (f *fakeTasks) List(_ context.Context, filter application.TaskFilter, page application.PageRequest) (application.Page[domain.Task], error) {
	f.filter, f.page = filter, page
	return application.Page[domain.Task]{}, f.err
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestRouter(tenants TenantService, tasks TaskService) http.Handler {
	return NewRouter(tenants, tasks, discardLogger(), time.Second)
}

// newRequest builds a request; a non-empty body is sent as application/json.
func newRequest(t *testing.T, method, target, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func do(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	return serve(h, newRequest(t, method, target, body))
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("body %q is not the expected JSON: %v", rec.Body.String(), err)
	}
	return v
}

// wantError asserts a structured error response.
func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code, field string) errorDetail {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	got := decodeBody[errorResponse](t, rec).Error
	if rec.Code != status || got.Code != code || got.Field != field {
		t.Errorf("got %d code=%q field=%q (%q), want %d code=%q field=%q",
			rec.Code, got.Code, got.Field, got.Message, status, code, field)
	}
	if got.Message == "" {
		t.Error("error message is empty")
	}
	return got
}

func TestHealth(t *testing.T) {
	rec := do(t, newTestRouter(&fakeTenants{}, &fakeTasks{}), http.MethodGet, "/healthz", "")
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestCreateTenant(t *testing.T) {
	tenants := &fakeTenants{}
	rec := do(t, newTestRouter(tenants, &fakeTasks{}), http.MethodPost, "/v1/tenants", `{"slug":"acme","name":"Acme"}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/tenants/ten-1" {
		t.Errorf("Location = %q", loc)
	}
	if want := (application.CreateTenantInput{Slug: "acme", Name: "Acme"}); tenants.createIn != want {
		t.Errorf("input = %+v, want %+v", tenants.createIn, want)
	}
	got := decodeBody[tenantMutationResponse](t, rec)
	want := tenantMutationResponse{
		Tenant: tenantResponse{ID: "ten-1", Slug: "acme", Name: "Acme", Status: "provisioning", Version: 1,
			CreatedAt: "2026-01-02T03:04:05.006Z", UpdatedAt: "2026-01-02T03:04:05.006Z"},
		Task: taskResponse{ID: "task-1", Type: "deploy", TenantID: "ten-1", Status: "accepted",
			CreatedAt: "2026-01-02T03:04:05.006Z", UpdatedAt: "2026-01-02T03:04:05.006Z"},
	}
	if got != want {
		t.Errorf("body = %+v\nwant   %+v", got, want)
	}
}

// TestRejectsMalformedBodies checks that the HTTP layer rejects bodies it
// cannot turn into a use-case input, without calling the use case.
func TestRejectsMalformedBodies(t *testing.T) {
	tests := []struct {
		name, method, body, contentType string
		status                          int
		code, field                     string
	}{
		{"malformed json", http.MethodPost, `{"slug":`, "", 400, codeInvalidRequest, ""},
		{"invalid json token", http.MethodPost, `{slug:"acme"}`, "", 400, codeInvalidRequest, ""},
		{"empty body", http.MethodPost, ``, "application/json", 400, codeInvalidRequest, ""},
		{"array body", http.MethodPost, `[]`, "", 400, codeInvalidRequest, ""},
		{"trailing data", http.MethodPost, `{"slug":"acme","name":"A"}{}`, "", 400, codeInvalidRequest, ""},
		{"unsupported field", http.MethodPost, `{"slug":"acme","name":"A","status":"active"}`, "", 400, codeInvalidRequest, "status"},
		{"missing slug", http.MethodPost, `{"name":"A"}`, "", 400, codeInvalidRequest, "slug"},
		{"missing name", http.MethodPost, `{"slug":"acme"}`, "", 400, codeInvalidRequest, "name"},
		{"null name", http.MethodPost, `{"slug":"acme","name":null}`, "", 400, codeInvalidRequest, "name"},
		{"wrong type", http.MethodPost, `{"slug":42,"name":"A"}`, "", 400, codeInvalidRequest, "slug"},
		{"no content type", http.MethodPost, `{"slug":"acme","name":"A"}`, "-", 415, codeUnsupportedMediaType, ""},
		{"form content type", http.MethodPost, `slug=acme`, "application/x-www-form-urlencoded", 415, codeUnsupportedMediaType, ""},
		{"too large", http.MethodPost, `{"name":"` + strings.Repeat("a", maxBodyBytes) + `"}`, "", 413, codePayloadTooLarge, ""},

		{"patch missing version", http.MethodPatch, `{"name":"B"}`, "", 400, codeInvalidRequest, "version"},
		{"patch null version", http.MethodPatch, `{"name":"B","version":null}`, "", 400, codeInvalidRequest, "version"},
		{"patch string version", http.MethodPatch, `{"version":"1"}`, "", 400, codeInvalidRequest, "version"},
		{"patch fractional version", http.MethodPatch, `{"version":1.5}`, "", 400, codeInvalidRequest, "version"},
		{"patch null name", http.MethodPatch, `{"name":null,"version":1}`, "", 400, codeInvalidRequest, "name"},
		{"patch immutable slug", http.MethodPatch, `{"slug":"other","version":1}`, "", 400, codeInvalidRequest, "slug"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenants := &fakeTenants{}
			target := "/v1/tenants"
			if tt.method == http.MethodPatch {
				target += "/ten-1"
			}
			req := newRequest(t, tt.method, target, tt.body)
			switch tt.contentType {
			case "":
			case "-":
				req.Header.Del("Content-Type")
			default:
				req.Header.Set("Content-Type", tt.contentType)
			}
			msg := wantError(t, serve(newTestRouter(tenants, &fakeTasks{}), req), tt.status, tt.code, tt.field).Message
			if tenants.calls != 0 {
				t.Errorf("use case called %d times, want 0", tenants.calls)
			}
			if strings.Contains(msg, "Go") || strings.Contains(msg, "Request") {
				t.Errorf("message %q leaks Go type names", msg)
			}
		})
	}
}

func TestContentTypeWithCharsetIsAccepted(t *testing.T) {
	req := newRequest(t, http.MethodPost, "/v1/tenants", `{"slug":"acme","name":"A"}`)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if rec := serve(newTestRouter(&fakeTenants{}, &fakeTasks{}), req); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
}

// TestUpdateTenantPartialSemantics checks that an omitted field reaches the
// use case as nil (unchanged), never as its zero value.
func TestUpdateTenantPartialSemantics(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantName *string
	}{
		{"name omitted", `{"version":3}`, nil},
		{"name set", `{"name":"Acme 2","version":3}`, ptr("Acme 2")},
		// Present but empty is forwarded; the domain rejects it (see
		// TestDomainValidationErrors), the adapter does not decide.
		{"name empty", `{"name":"","version":3}`, ptr("")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenants := &fakeTenants{}
			rec := do(t, newTestRouter(tenants, &fakeTasks{}), http.MethodPatch, "/v1/tenants/ten-1", tt.body)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
			}
			if tenants.id != "ten-1" || tenants.updateIn.ExpectedVersion != 3 {
				t.Errorf("id=%q expected version=%d", tenants.id, tenants.updateIn.ExpectedVersion)
			}
			if got := tenants.updateIn.Name; (got == nil) != (tt.wantName == nil) || (got != nil && *got != *tt.wantName) {
				t.Errorf("Name = %v, want %v", deref(got), deref(tt.wantName))
			}
			if loc := rec.Header().Get("Location"); loc != "/v1/tasks/task-2" {
				t.Errorf("Location = %q, want the update task", loc)
			}
			got := decodeBody[tenantMutationResponse](t, rec)
			if got.Tenant.Status != "updating" || got.Tenant.Version != 4 || got.Task.Type != "update" {
				t.Errorf("body = %+v", got)
			}
		})
	}
}

func ptr(s string) *string { return &s }

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q", *s)
}

func TestDeleteTenant(t *testing.T) {
	tenants := &fakeTenants{}
	rec := do(t, newTestRouter(tenants, &fakeTasks{}), http.MethodDelete, "/v1/tenants/ten-1", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/tasks/task-3" {
		t.Errorf("Location = %q, want the destroy task", loc)
	}
	got := decodeBody[tenantMutationResponse](t, rec)
	if tenants.id != "ten-1" || got.Tenant.Status != "destroying" || got.Task.Type != "destroy" {
		t.Errorf("id=%q body=%+v", tenants.id, got)
	}
}

// TestContractErrors checks each contract error on the endpoints that
// return it: status, code, and a message that is the sentinel's fixed text
// only, without the IDs and database error wrapped around it.
func TestContractErrors(t *testing.T) {
	dbErr := errors.New(`ERROR: duplicate key value violates unique constraint "tenants_slug_key" (SQLSTATE 23505)`)
	tests := []struct {
		sentinel     error
		method, path string
		body         string
		status       int
		code         string
	}{
		{domain.ErrTenantNotFound, http.MethodGet, "/v1/tenants/ten-9", "", 404, codeTenantNotFound},
		{domain.ErrTenantNotFound, http.MethodPatch, "/v1/tenants/ten-9", `{"name":"B","version":1}`, 404, codeTenantNotFound},
		{domain.ErrTenantNotFound, http.MethodDelete, "/v1/tenants/ten-9", "", 404, codeTenantNotFound},
		{domain.ErrTenantAlreadyExists, http.MethodPost, "/v1/tenants", `{"slug":"acme","name":"A"}`, 409, codeTenantAlreadyExists},
		{domain.ErrTenantUpdateNotAllowed, http.MethodPatch, "/v1/tenants/ten-1", `{"name":"B","version":1}`, 409, codeTenantUpdateNotAllowed},
		{domain.ErrTenantUpdateNotAllowed, http.MethodDelete, "/v1/tenants/ten-1", "", 409, codeTenantUpdateNotAllowed},
		{domain.ErrTenantVersionConflict, http.MethodPatch, "/v1/tenants/ten-1", `{"name":"B","version":1}`, 409, codeTenantVersionConflict},
		{domain.ErrTenantVersionConflict, http.MethodDelete, "/v1/tenants/ten-1", "", 409, codeTenantVersionConflict},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.code, func(t *testing.T) {
			err := fmt.Errorf("op on tenant ten-9: %w: %w", tt.sentinel, dbErr)
			rec := do(t, newTestRouter(&fakeTenants{err: err}, &fakeTasks{}), tt.method, tt.path, tt.body)
			got := wantError(t, rec, tt.status, tt.code, "")
			if got.Message != tt.sentinel.Error() {
				t.Errorf("message = %q, want exactly %q", got.Message, tt.sentinel.Error())
			}
		})
	}

	t.Run("task_not_found", func(t *testing.T) {
		tasks := &fakeTasks{err: fmt.Errorf("get task x: %w", domain.ErrTaskNotFound)}
		rec := do(t, newTestRouter(&fakeTenants{}, tasks), http.MethodGet, "/v1/tasks/x", "")
		if got := wantError(t, rec, 404, codeTaskNotFound, ""); got.Message != domain.ErrTaskNotFound.Error() {
			t.Errorf("message = %q", got.Message)
		}
	})
}

// TestTransitionErrorsMapToUpdateNotAllowed uses the error the domain
// really returns for a forbidden transition, not a bare sentinel.
func TestTransitionErrorsMapToUpdateNotAllowed(t *testing.T) {
	tenant := tenantA // provisioning: neither update nor destroy is allowed
	err := tenant.RequestDestroy(ts0)
	if err == nil {
		t.Fatal("RequestDestroy on a provisioning tenant succeeded")
	}
	rec := do(t, newTestRouter(&fakeTenants{err: err}, &fakeTasks{}), http.MethodDelete, "/v1/tenants/ten-1", "")
	wantError(t, rec, 409, codeTenantUpdateNotAllowed, "")
}

// TestDomainValidationErrors checks that domain validation becomes a 400
// naming the field, with the rule as message but not the wrapping context.
func TestDomainValidationErrors(t *testing.T) {
	_, valErr := domain.NewTenant("ten-1", "Bad Slug", "Acme", ts0)
	if valErr == nil {
		t.Fatal("NewTenant accepted an invalid slug")
	}
	err := fmt.Errorf("create tenant: %w", valErr)
	rec := do(t, newTestRouter(&fakeTenants{err: err}, &fakeTasks{}), http.MethodPost, "/v1/tenants", `{"slug":"Bad Slug","name":"Acme"}`)
	got := wantError(t, rec, 400, codeInvalidRequest, "slug")
	if got.Message != valErr.Error() {
		t.Errorf("message = %q, want %q", got.Message, valErr.Error())
	}
}

// TestInternalErrorsAreOpaque checks that unexpected errors reach the log
// but not the client.
func TestInternalErrorsAreOpaque(t *testing.T) {
	var logs bytes.Buffer
	secret := errors.New(`dial tcp 10.0.0.5:5432: password authentication failed for user "control_plane" (SQLSTATE 28P01)`)
	h := NewRouter(&fakeTenants{err: secret}, &fakeTasks{}, slog.New(slog.NewJSONHandler(&logs, nil)), time.Second)

	rec := do(t, h, http.MethodGet, "/v1/tenants/ten-1", "")
	got := wantError(t, rec, 500, codeInternal, "")
	for _, leak := range []string{"SQLSTATE", "password", "10.0.0.5", "control_plane"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("response leaks %q: %s", leak, rec.Body)
		}
	}
	if got.Message != "internal server error" {
		t.Errorf("message = %q", got.Message)
	}
	if !strings.Contains(logs.String(), "SQLSTATE 28P01") {
		t.Errorf("internal error was not logged: %s", logs.String())
	}
}

func TestListQueryValidation(t *testing.T) {
	tests := []struct {
		name, target, field string
	}{
		{"limit zero", "/v1/tenants?limit=0", "limit"},
		{"limit too large", "/v1/tenants?limit=201", "limit"},
		{"limit not a number", "/v1/tasks?limit=ten", "limit"},
		{"unknown tenant status", "/v1/tenants?status=running", "status"},
		{"task status on tenants", "/v1/tenants?status=in_progress", "status"},
		{"unknown task status", "/v1/tasks?status=active", "status"},
		{"unsupported parameter", "/v1/tenants?stauts=active", "stauts"},
		{"tenant_id on tenants", "/v1/tenants?tenant_id=x", "tenant_id"},
		{"repeated parameter", "/v1/tasks?status=done&status=failed", "status"},
		{"malformed query", "/v1/tasks?status=%zz", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenants := &fakeTenants{}
			rec := do(t, newTestRouter(tenants, &fakeTasks{}), http.MethodGet, tt.target, "")
			wantError(t, rec, 400, codeInvalidRequest, tt.field)
			if tenants.calls != 0 {
				t.Error("use case called despite invalid query")
			}
		})
	}
}

func TestListForwardsFiltersAndPagination(t *testing.T) {
	tenants, tasks := &fakeTenants{}, &fakeTasks{}
	h := newTestRouter(tenants, tasks)

	rec := do(t, h, http.MethodGet, "/v1/tenants?status=active&limit=10&cursor=abc", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if tenants.filter.Status != domain.TenantStatusActive || tenants.page != (application.PageRequest{Limit: 10, Cursor: "abc"}) {
		t.Errorf("filter = %+v page = %+v", tenants.filter, tenants.page)
	}
	if got := decodeBody[listResponse[tenantResponse]](t, rec); len(got.Items) != 1 || got.NextCursor != "next" {
		t.Errorf("body = %+v", got)
	}

	if rec := do(t, h, http.MethodGet, "/v1/tasks?tenant_id=ten-1&status=in_progress", ""); rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if want := (application.TaskFilter{TenantID: "ten-1", Status: domain.TaskStatusInProgress}); tasks.filter != want {
		t.Errorf("filter = %+v, want %+v", tasks.filter, want)
	}
	if tasks.page != (application.PageRequest{}) {
		t.Errorf("page = %+v, want defaults", tasks.page)
	}
}

func TestEmptyListEncodesItemsAsArray(t *testing.T) {
	rec := do(t, newTestRouter(&fakeTenants{}, &fakeTasks{}), http.MethodGet, "/v1/tasks", "")
	if got := strings.TrimSpace(rec.Body.String()); got != `{"items":[]}` {
		t.Errorf("body = %s", got)
	}
}

func TestTaskResponseTimestamps(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 6_789_000, time.FixedZone("CET", 3600))
	h := newTestRouter(&fakeTenants{}, &fakeTasks{task: domain.Task{ID: "t1", CreatedAt: ts, UpdatedAt: ts}})
	got := decodeBody[taskResponse](t, do(t, h, http.MethodGet, "/v1/tasks/t1", ""))
	if want := "2026-01-02T02:04:05.006Z"; got.CreatedAt != want {
		t.Errorf("created_at = %q, want %q", got.CreatedAt, want)
	}
}

func TestRequestTimeout(t *testing.T) {
	h := NewRouter(&fakeTenants{block: true}, &fakeTasks{}, discardLogger(), 20*time.Millisecond)
	rec := do(t, h, http.MethodGet, "/v1/tenants/ten-1", "")
	wantError(t, rec, http.StatusServiceUnavailable, codeRequestTimeout, "")
}

func TestRequestIDAndLogging(t *testing.T) {
	var logs bytes.Buffer
	h := NewRouter(&fakeTenants{}, &fakeTasks{}, slog.New(slog.NewJSONHandler(&logs, nil)), time.Second)

	req := newRequest(t, http.MethodGet, "/v1/tenants/ten-1", "")
	req.Header.Set(requestIDHeader, "client-id.42")
	if got := serve(h, req).Header().Get(requestIDHeader); got != "client-id.42" {
		t.Errorf("valid client request ID not echoed: %q", got)
	}
	var line struct {
		Msg       string `json:"msg"`
		RequestID string `json:"request_id"`
		Method    string `json:"method"`
		Path      string `json:"path"`
		Status    int    `json:"status"`
	}
	if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
		t.Fatalf("log line %q: %v", logs.String(), err)
	}
	if line.Msg != "http request" || line.RequestID != "client-id.42" || line.Method != "GET" ||
		line.Path != "/v1/tenants/ten-1" || line.Status != 200 {
		t.Errorf("log line = %+v", line)
	}

	req = newRequest(t, http.MethodGet, "/healthz", "")
	req.Header.Set(requestIDHeader, "bad id\nwith newline")
	if got := serve(h, req).Header().Get(requestIDHeader); got == "" || strings.ContainsAny(got, " \n") {
		t.Errorf("invalid client request ID not replaced: %q", got)
	}
}

func TestStructuredRouteErrors(t *testing.T) {
	h := newTestRouter(&fakeTenants{}, &fakeTasks{})

	wantError(t, do(t, h, http.MethodGet, "/nope", ""), 404, codeRouteNotFound, "")
	wantError(t, do(t, h, http.MethodGet, "/v1/tenants/ten-1/tasks", ""), 404, codeRouteNotFound, "")

	rec := do(t, h, http.MethodPut, "/v1/tenants/ten-1", "")
	wantError(t, rec, 405, codeMethodNotAllowed, "")
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodPatch) {
		t.Errorf("Allow = %q, want it to include PATCH", allow)
	}
	wantError(t, do(t, h, http.MethodPost, "/v1/tasks", `{}`), 405, codeMethodNotAllowed, "")
}

type panickingTasks struct{ fakeTasks }

func (*panickingTasks) Get(context.Context, string) (domain.Task, error) { panic("boom") }

func TestRecoverPanics(t *testing.T) {
	rec := do(t, newTestRouter(&fakeTenants{}, &panickingTasks{}), http.MethodGet, "/v1/tasks/x", "")
	wantError(t, rec, 500, codeInternal, "")
	if strings.Contains(rec.Body.String(), "boom") || strings.Contains(rec.Body.String(), "goroutine") {
		t.Errorf("panic details leaked: %s", rec.Body)
	}
}

func TestServeShutsDownGracefully(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	srv := NewServer(config.HTTP{ReadHeaderTimeout: time.Second}, newTestRouter(&fakeTenants{}, &fakeTasks{}))

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

// TestShutdownWaitsForInFlightRequests checks that a request already being
// served completes during shutdown instead of being cut off.
func TestShutdownWaitsForInFlightRequests(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		writeJSON(w, http.StatusOK, map[string]string{"status": "done"})
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, NewServer(config.HTTP{ReadHeaderTimeout: time.Second}, slow), ln, 5*time.Second)
	}()

	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- 0
			return
		}
		_ = res.Body.Close()
		status <- res.StatusCode
	}()

	<-started
	cancel() // shutdown begins while the request is in flight
	time.Sleep(50 * time.Millisecond)
	close(release)

	if got := <-status; got != http.StatusOK {
		t.Errorf("in-flight request status = %d, want 200", got)
	}
	if err := <-done; err != nil {
		t.Errorf("Serve returned %v", err)
	}
}
