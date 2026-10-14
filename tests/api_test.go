// Package tests holds end-to-end tests that cross package boundaries.
//
// The tests in this file drive the real HTTP API (router, application
// services, PostgreSQL) over a real connection with httptest.Server. The
// broker is not involved: where the brief has the worker report progress,
// the tests call TaskService.ApplyUpdate directly, which is what the
// update consumer does. Outbound publication through RabbitMQ is covered
// in outbox_test.go, inbound consumption in consumer_test.go.
package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/httpapi"
	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/pgtest"
)

// The JSON shapes below are written out independently of the httpapi
// types, so a change to the public contract breaks these tests.

type tenantJSON struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type taskJSON struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
}

type mutationJSON struct {
	Tenant tenantJSON `json:"tenant"`
	Task   taskJSON   `json:"task"`
}

type listJSON[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor"`
}

type errorJSON struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Field   string `json:"field"`
	} `json:"error"`
}

type api struct {
	t     *testing.T
	url   string
	srv   *httptest.Server
	tasks *application.TaskService
	store *postgres.Store
	pool  *pgxpool.Pool
}

func newAPI(t *testing.T) *api {
	t.Helper()
	return newAPIOn(t, pgtest.NewMigratedDB(t))
}

// newAPIOn serves the API on an existing database pool.
func newAPIOn(t *testing.T, pool *pgxpool.Pool) *api {
	t.Helper()
	store := postgres.NewStore(pool)
	tenants := application.NewTenantService(store, store, uuid.NewString, time.Now)
	tasks := application.NewTaskService(store, store, time.Now)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(httpapi.NewRouter(tenants, tasks, logger, 5*time.Second))
	t.Cleanup(srv.Close)
	return &api{t: t, url: srv.URL, srv: srv, tasks: tasks, store: store, pool: pool}
}

type response struct {
	status int
	header http.Header
	body   []byte
}

// do sends a request. It may run on any goroutine, so it reports failures
// with Errorf (and a zero response) rather than Fatal.
func (a *api) do(method, path string, body any) response {
	a.t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			a.t.Errorf("marshal request: %v", err)
			return response{}
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(a.t.Context(), method, a.url+path, r)
	if err != nil {
		a.t.Errorf("new request: %v", err)
		return response{}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Errorf("%s %s: %v", method, path, err)
		return response{}
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		a.t.Errorf("read response: %v", err)
	}
	return response{status: res.StatusCode, header: res.Header, body: b}
}

// expect asserts the status of res and decodes its body into a T.
func expect[T any](t *testing.T, res response, status int) T {
	t.Helper()
	if res.status != status {
		t.Fatalf("status = %d, want %d; body: %s", res.status, status, res.body)
	}
	var v T
	if err := json.Unmarshal(res.body, &v); err != nil {
		t.Fatalf("decode %s: %v", res.body, err)
	}
	return v
}

func expectError(t *testing.T, res response, status int, code string) errorJSON {
	t.Helper()
	e := expect[errorJSON](t, res, status)
	if e.Error.Code != code {
		t.Fatalf("code = %q, want %q; body: %s", e.Error.Code, code, res.body)
	}
	return e
}

func (a *api) create(slug, name string) mutationJSON {
	a.t.Helper()
	return expect[mutationJSON](a.t, a.do(http.MethodPost, "/v1/tenants", map[string]any{"slug": slug, "name": name}), http.StatusCreated)
}

func (a *api) getTenant(id string) tenantJSON {
	a.t.Helper()
	return expect[tenantJSON](a.t, a.do(http.MethodGet, "/v1/tenants/"+id, nil), http.StatusOK)
}

func (a *api) getTask(id string) taskJSON {
	a.t.Helper()
	return expect[taskJSON](a.t, a.do(http.MethodGet, "/v1/tasks/"+id, nil), http.StatusOK)
}

// finish plays the worker: it reports in_progress and then outcome for
// the task, as separate updates with their own idempotency keys.
func (a *api) finish(taskID string, outcome domain.TaskStatus) {
	a.t.Helper()
	task, err := a.tasks.Get(a.t.Context(), taskID)
	if err != nil {
		a.t.Fatal(err)
	}
	previous := domain.TaskStatusAccepted
	for _, status := range []domain.TaskStatus{domain.TaskStatusInProgress, outcome} {
		update := domain.TaskUpdate{
			ID: uuid.NewString(), TaskID: taskID, TenantID: task.TenantID, TaskType: task.Type,
			PreviousStatus: previous, Status: status, OccurredAt: time.Now(),
		}
		previous = status
		if _, err := a.tasks.ApplyUpdate(a.t.Context(), update); err != nil {
			a.t.Fatalf("apply %s to task %s: %v", status, taskID, err)
		}
	}
}

// activeTenant creates a tenant and completes its deploy.
func (a *api) activeTenant(slug string) tenantJSON {
	a.t.Helper()
	m := a.create(slug, "Tenant "+slug)
	a.finish(m.Task.ID, domain.TaskStatusDone)
	return a.getTenant(m.Tenant.ID)
}

// TestTenantLifecycle walks one tenant through every API-driven
// transition: create, deploy, update, destroy.
func TestTenantLifecycle(t *testing.T) {
	a := newAPI(t)

	// Create: 201, tenant provisioning at version 1, deploy task accepted.
	res := a.do(http.MethodPost, "/v1/tenants", map[string]any{"slug": "acme", "name": "Acme"})
	created := expect[mutationJSON](t, res, http.StatusCreated)
	tenantID := created.Tenant.ID
	if loc := res.header.Get("Location"); loc != "/v1/tenants/"+tenantID {
		t.Errorf("Location = %q", loc)
	}
	if created.Tenant.Status != "provisioning" || created.Tenant.Version != 1 ||
		created.Task.Type != "deploy" || created.Task.Status != "accepted" || created.Task.TenantID != tenantID {
		t.Fatalf("created = %+v", created)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", created.Tenant.CreatedAt); err != nil {
		t.Errorf("created_at %q is not ISO 8601 with milliseconds and Z: %v", created.Tenant.CreatedAt, err)
	}
	if got := a.getTenant(tenantID); got != created.Tenant {
		t.Errorf("GET = %+v, want %+v", got, created.Tenant)
	}

	// Deploy completes: active, version 2 (worker changes count as mutations).
	a.finish(created.Task.ID, domain.TaskStatusDone)
	active := a.getTenant(tenantID)
	if active.Status != "active" || active.Version != 2 {
		t.Fatalf("after deploy: %+v", active)
	}
	if task := a.getTask(created.Task.ID); task.Status != "done" {
		t.Errorf("deploy task = %+v", task)
	}

	// PATCH at the current version: 202, updating, update task linked by Location.
	res = a.do(http.MethodPatch, "/v1/tenants/"+tenantID, map[string]any{"name": "Acme Corp", "version": active.Version})
	updated := expect[mutationJSON](t, res, http.StatusAccepted)
	if loc := res.header.Get("Location"); loc != "/v1/tasks/"+updated.Task.ID {
		t.Errorf("Location = %q", loc)
	}
	if updated.Tenant.Status != "updating" || updated.Tenant.Version != 3 || updated.Tenant.Name != "Acme Corp" ||
		updated.Tenant.Slug != "acme" || updated.Task.Type != "update" {
		t.Fatalf("updated = %+v", updated)
	}
	a.finish(updated.Task.ID, domain.TaskStatusDone)

	// DELETE: 202, destroying, destroy task; once done the tenant is
	// destroyed but still readable.
	active = a.getTenant(tenantID)
	res = a.do(http.MethodDelete, "/v1/tenants/"+tenantID, nil)
	deleted := expect[mutationJSON](t, res, http.StatusAccepted)
	if deleted.Tenant.Status != "destroying" || deleted.Tenant.Version != active.Version+1 || deleted.Task.Type != "destroy" {
		t.Fatalf("deleted = %+v", deleted)
	}
	if loc := res.header.Get("Location"); loc != "/v1/tasks/"+deleted.Task.ID {
		t.Errorf("Location = %q", loc)
	}
	a.finish(deleted.Task.ID, domain.TaskStatusDone)
	if got := a.getTenant(tenantID); got.Status != "destroyed" {
		t.Fatalf("after destroy: %+v", got)
	}

	// A destroyed tenant cannot be deleted again, and its slug stays taken.
	expectError(t, a.do(http.MethodDelete, "/v1/tenants/"+tenantID, nil), http.StatusConflict, "tenant_update_not_allowed")
	expectError(t, a.do(http.MethodPost, "/v1/tenants", map[string]any{"slug": "acme", "name": "Again"}),
		http.StatusConflict, "tenant_already_exists")

	tasks := expect[listJSON[taskJSON]](t, a.do(http.MethodGet, "/v1/tasks?tenant_id="+tenantID, nil), http.StatusOK)
	var types []string
	for _, task := range tasks.Items {
		types = append(types, task.Type+":"+task.Status)
	}
	if fmt.Sprint(types) != "[deploy:done update:done destroy:done]" {
		t.Errorf("tasks = %v", types)
	}
}

func TestDeleteFailedTenant(t *testing.T) {
	a := newAPI(t)
	m := a.create("flaky", "Flaky")
	a.finish(m.Task.ID, domain.TaskStatusFailed)
	if got := a.getTenant(m.Tenant.ID); got.Status != "failed" {
		t.Fatalf("after failed deploy: %+v", got)
	}
	deleted := expect[mutationJSON](t, a.do(http.MethodDelete, "/v1/tenants/"+m.Tenant.ID, nil), http.StatusAccepted)
	if deleted.Tenant.Status != "destroying" {
		t.Errorf("deleted = %+v", deleted)
	}
}

// TestFailureOutcomes: a failed task of any type moves its tenant to
// failed, with a version bump; from failed, DELETE still destroys it.
func TestFailureOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name string
		// start returns a tenant whose open task is of the type under test.
		start func(a *api) mutationJSON
	}{
		{"deploy", func(a *api) mutationJSON { return a.create("deploy-fails", "D") }},
		{"update", func(a *api) mutationJSON {
			tn := a.activeTenant("update-fails")
			return expect[mutationJSON](a.t, a.do(http.MethodPatch, "/v1/tenants/"+tn.ID,
				map[string]any{"name": "New", "version": tn.Version}), http.StatusAccepted)
		}},
		{"destroy", func(a *api) mutationJSON {
			tn := a.activeTenant("destroy-fails")
			return expect[mutationJSON](a.t, a.do(http.MethodDelete, "/v1/tenants/"+tn.ID, nil), http.StatusAccepted)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t)
			m := tt.start(a)
			if m.Task.Type != tt.name {
				t.Fatalf("open task is %s, want %s", m.Task.Type, tt.name)
			}
			a.finish(m.Task.ID, domain.TaskStatusFailed)
			a.wantTask(m.Task.ID, "failed")
			failed := a.wantTenant(m.Tenant.ID, "failed", m.Tenant.Version+1)

			deleted := expect[mutationJSON](t, a.do(http.MethodDelete, "/v1/tenants/"+m.Tenant.ID, nil), http.StatusAccepted)
			if deleted.Tenant.Status != "destroying" || deleted.Tenant.Version != failed.Version+1 || deleted.Task.Type != "destroy" {
				t.Fatalf("deleted = %+v", deleted)
			}
			a.finish(deleted.Task.ID, domain.TaskStatusDone)
			a.wantTenant(m.Tenant.ID, "destroyed", failed.Version+2)
		})
	}
}

func TestDuplicateSlug(t *testing.T) {
	a := newAPI(t)
	a.create("acme", "Acme")
	e := expectError(t, a.do(http.MethodPost, "/v1/tenants", map[string]any{"slug": "acme", "name": "Other"}),
		http.StatusConflict, "tenant_already_exists")
	if e.Error.Message != "tenant already exists" {
		t.Errorf("message = %q", e.Error.Message)
	}
}

func TestConcurrentCreatesWithSameSlug(t *testing.T) {
	a := newAPI(t)
	results := concurrently(16, func(int) response {
		return a.do(http.MethodPost, "/v1/tenants", map[string]any{"slug": "race", "name": "Race"})
	})
	if results["201"] != 1 || results["409 tenant_already_exists"] != 15 || len(results) != 2 {
		t.Errorf("results = %v, want one 201 and fifteen 409 tenant_already_exists", results)
	}
	tenants := expect[listJSON[tenantJSON]](t, a.do(http.MethodGet, "/v1/tenants", nil), http.StatusOK)
	tasks := expect[listJSON[taskJSON]](t, a.do(http.MethodGet, "/v1/tasks", nil), http.StatusOK)
	if len(tenants.Items) != 1 || len(tasks.Items) != 1 || a.pendingEvents() != 1 {
		t.Errorf("tenants, tasks, outbox events = %d, %d, %d; want 1 each (losers leave nothing behind)",
			len(tenants.Items), len(tasks.Items), a.pendingEvents())
	}
}

func TestNotFound(t *testing.T) {
	a := newAPI(t)
	missing := uuid.NewString()
	for _, tt := range []struct {
		method, path string
		body         any
		code         string
	}{
		{http.MethodGet, "/v1/tenants/" + missing, nil, "tenant_not_found"},
		{http.MethodGet, "/v1/tenants/not-a-uuid", nil, "tenant_not_found"},
		{http.MethodPatch, "/v1/tenants/" + missing, map[string]any{"name": "X", "version": 1}, "tenant_not_found"},
		{http.MethodDelete, "/v1/tenants/" + missing, nil, "tenant_not_found"},
		{http.MethodGet, "/v1/tasks/" + missing, nil, "task_not_found"},
		{http.MethodGet, "/v1/tasks/not-a-uuid", nil, "task_not_found"},
	} {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			expectError(t, a.do(tt.method, tt.path, tt.body), http.StatusNotFound, tt.code)
		})
	}
}

func TestValidationLeavesNothingBehind(t *testing.T) {
	a := newAPI(t)
	for _, tt := range []struct {
		name  string
		body  any
		field string
	}{
		{"slug too short", map[string]any{"slug": "ab", "name": "A"}, "slug"},
		{"slug uppercase", map[string]any{"slug": "Acme", "name": "A"}, "slug"},
		{"slug trailing hyphen", map[string]any{"slug": "acme-", "name": "A"}, "slug"},
		{"empty name", map[string]any{"slug": "acme", "name": ""}, "name"},
		{"blank name", map[string]any{"slug": "acme", "name": "   "}, "name"},
		{"missing name", map[string]any{"slug": "acme"}, "name"},
		{"unsupported field", map[string]any{"slug": "acme", "name": "A", "id": "x"}, "id"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := expectError(t, a.do(http.MethodPost, "/v1/tenants", tt.body), http.StatusBadRequest, "invalid_request")
			if e.Error.Field != tt.field {
				t.Errorf("field = %q, want %q (%s)", e.Error.Field, tt.field, e.Error.Message)
			}
		})
	}
	list := expect[listJSON[tenantJSON]](t, a.do(http.MethodGet, "/v1/tenants", nil), http.StatusOK)
	if len(list.Items) != 0 {
		t.Errorf("invalid creates persisted tenants: %+v", list.Items)
	}
}

func TestPatchValidation(t *testing.T) {
	a := newAPI(t)
	tenant := a.activeTenant("acme")
	path := "/v1/tenants/" + tenant.ID
	for _, tt := range []struct {
		name  string
		body  any
		field string
	}{
		{"no changes", map[string]any{"version": tenant.Version}, "body"},
		{"empty name", map[string]any{"name": "", "version": tenant.Version}, "name"},
		{"version zero", map[string]any{"name": "X", "version": 0}, "version"},
		{"missing version", map[string]any{"name": "X"}, "version"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := expectError(t, a.do(http.MethodPatch, path, tt.body), http.StatusBadRequest, "invalid_request")
			if e.Error.Field != tt.field {
				t.Errorf("field = %q, want %q (%s)", e.Error.Field, tt.field, e.Error.Message)
			}
		})
	}
	if got := a.getTenant(tenant.ID); got != tenant {
		t.Errorf("rejected PATCHes changed the tenant: %+v, was %+v", got, tenant)
	}
}

// TestInvalidStateTransitions checks the tenant status guards: PATCH only
// from active, DELETE only from active or failed. Every other status is
// covered for each.
func TestInvalidStateTransitions(t *testing.T) {
	a := newAPI(t)

	provisioning := a.create("prov", "Provisioning").Tenant
	updating := a.activeTenant("upd")
	expect[mutationJSON](t, a.do(http.MethodPatch, "/v1/tenants/"+updating.ID,
		map[string]any{"name": "New", "version": updating.Version}), http.StatusAccepted)
	updating = a.getTenant(updating.ID)
	destroying := a.activeTenant("dest")
	expect[mutationJSON](t, a.do(http.MethodDelete, "/v1/tenants/"+destroying.ID, nil), http.StatusAccepted)
	destroying = a.getTenant(destroying.ID)
	failed := a.create("fail", "Failed")
	a.finish(failed.Task.ID, domain.TaskStatusFailed)
	destroyed := a.activeTenant("gone")
	gone := expect[mutationJSON](t, a.do(http.MethodDelete, "/v1/tenants/"+destroyed.ID, nil), http.StatusAccepted)
	a.finish(gone.Task.ID, domain.TaskStatusDone)
	destroyed = a.getTenant(destroyed.ID)

	for _, tt := range []struct {
		tenant tenantJSON
		method string
	}{
		{provisioning, http.MethodPatch},
		{provisioning, http.MethodDelete},
		{updating, http.MethodPatch},
		{updating, http.MethodDelete},
		{destroying, http.MethodPatch},
		{destroying, http.MethodDelete},
		{a.getTenant(failed.Tenant.ID), http.MethodPatch},
		{destroyed, http.MethodPatch},
		{destroyed, http.MethodDelete},
	} {
		t.Run(tt.tenant.Status+" "+tt.method, func(t *testing.T) {
			var body any
			if tt.method == http.MethodPatch {
				body = map[string]any{"name": "Changed", "version": tt.tenant.Version}
			}
			expectError(t, a.do(tt.method, "/v1/tenants/"+tt.tenant.ID, body), http.StatusConflict, "tenant_update_not_allowed")
			if got := a.getTenant(tt.tenant.ID); got != tt.tenant {
				t.Errorf("rejected request changed the tenant: %+v, was %+v", got, tt.tenant)
			}
		})
	}
}

func TestStaleVersionConflicts(t *testing.T) {
	a := newAPI(t)
	m := a.create("acme", "Acme")
	staleVersion := m.Tenant.Version // read while provisioning
	a.finish(m.Task.ID, domain.TaskStatusDone)

	// The tenant is active, but the worker's change moved the version on.
	e := expectError(t, a.do(http.MethodPatch, "/v1/tenants/"+m.Tenant.ID,
		map[string]any{"name": "New", "version": staleVersion}), http.StatusConflict, "tenant_version_conflict")
	if e.Error.Message != "tenant version conflict" {
		t.Errorf("message = %q", e.Error.Message)
	}
	if got := a.getTenant(m.Tenant.ID); got.Name != "Acme" || got.Status != "active" {
		t.Errorf("conflicting PATCH changed the tenant: %+v", got)
	}
}

// TestConcurrentPatchesWithSameVersion is the brief's concurrency rule:
// two simultaneous PATCHes with the same version must not both succeed.
func TestConcurrentPatchesWithSameVersion(t *testing.T) {
	a := newAPI(t)
	tenant := a.activeTenant("acme")
	results := concurrently(16, func(i int) response {
		return a.do(http.MethodPatch, "/v1/tenants/"+tenant.ID,
			map[string]any{"name": fmt.Sprintf("Name %d", i), "version": tenant.Version})
	})
	// Every loser is told its version is stale: either its conditional
	// write found the version moved on, or it read the winner's commit, and
	// the version check precedes the status check.
	if results["202"] != 1 || results["409 tenant_version_conflict"] != 15 || len(results) != 2 {
		t.Errorf("results = %v, want one 202 and fifteen 409 tenant_version_conflict", results)
	}
	got := a.getTenant(tenant.ID)
	if got.Status != "updating" || got.Version != tenant.Version+1 {
		t.Errorf("tenant = %+v", got)
	}
	open := expect[listJSON[taskJSON]](t, a.do(http.MethodGet, "/v1/tasks?tenant_id="+tenant.ID+"&status=accepted", nil), http.StatusOK)
	if len(open.Items) != 1 {
		t.Errorf("open tasks = %d, want 1", len(open.Items))
	}
}

func TestPaginationAndFilters(t *testing.T) {
	a := newAPI(t)
	for i := range 5 {
		a.create(fmt.Sprintf("tenant-%d", i), "T")
	}
	active := a.activeTenant("tenant-active")

	var slugs []string
	cursor := ""
	for page := 0; ; page++ {
		path := "/v1/tenants?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		got := expect[listJSON[tenantJSON]](t, a.do(http.MethodGet, path, nil), http.StatusOK)
		for _, tenant := range got.Items {
			slugs = append(slugs, tenant.Slug)
		}
		if got.NextCursor == "" {
			break
		}
		if page > 5 {
			t.Fatal("pagination does not terminate")
		}
		cursor = got.NextCursor
	}
	if want := "[tenant-0 tenant-1 tenant-2 tenant-3 tenant-4 tenant-active]"; fmt.Sprint(slugs) != want {
		t.Errorf("slugs = %v, want %s", slugs, want)
	}

	filtered := expect[listJSON[tenantJSON]](t, a.do(http.MethodGet, "/v1/tenants?status=active", nil), http.StatusOK)
	if len(filtered.Items) != 1 || filtered.Items[0].ID != active.ID {
		t.Errorf("status=active returned %+v", filtered.Items)
	}

	done := expect[listJSON[taskJSON]](t, a.do(http.MethodGet, "/v1/tasks?status=done", nil), http.StatusOK)
	if len(done.Items) != 1 || done.Items[0].TenantID != active.ID {
		t.Errorf("status=done returned %+v", done.Items)
	}

	expectError(t, a.do(http.MethodGet, "/v1/tenants?cursor=garbage", nil), http.StatusBadRequest, "invalid_request")
	unknown := expect[listJSON[taskJSON]](t, a.do(http.MethodGet, "/v1/tasks?tenant_id="+uuid.NewString(), nil), http.StatusOK)
	if len(unknown.Items) != 0 {
		t.Errorf("unknown tenant_id returned tasks: %+v", unknown.Items)
	}
}

// concurrently runs n requests at once and counts the results, keyed by
// status, plus the error code for errors: "202", "409 tenant_version_conflict".
func concurrently(n int, request func(i int) response) map[string]int {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = map[string]int{}
		start   = make(chan struct{})
	)
	for i := range n {
		wg.Go(func() {
			<-start
			res := request(i)
			key := strconv.Itoa(res.status)
			if res.status >= 400 {
				var e errorJSON
				_ = json.Unmarshal(res.body, &e)
				key += " " + e.Error.Code
			}
			mu.Lock()
			results[key]++
			mu.Unlock()
		})
	}
	close(start)
	wg.Wait()
	return results
}
