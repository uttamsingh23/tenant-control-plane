# Tenant Provisioning Control Plane

A control-plane service that records tenants, emits provisioning tasks over
RabbitMQ and applies progress reported back by a worker simulator.

> **Status: Stage 1 (scaffolding).** The project compiles, the HTTP server
> starts and shuts down gracefully, and `/healthz` works. Tenant lifecycle,
> persistence and messaging are stubbed: they return `not_implemented`
> (HTTP 501) instead of pretending to succeed.

## Quick start

Requirements: Go 1.25+, Docker with Compose v2, `make`.

```bash
make .env        # create local config from .env.example (git-ignored)
make up          # start PostgreSQL and RabbitMQ, wait until healthy
make run-api     # control plane on :8080
make run-worker  # worker simulator, e.g. make run-worker WORKER_ARGS="--fail-rate=1"
make check       # fmt-check + vet + golangci-lint (incl. gosec) + tests
```

`make help` lists all targets.

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}
```

## Project layout

```
cmd/
  api/              composition root of the control plane
  worker/           composition root of the worker simulator
internal/
  domain/           entities, states and error sentinels; no I/O dependencies
  application/      use cases and the interfaces (ports) they need
  config/           env/flag configuration loading
  adapters/
    httpapi/        REST API: routing, JSON encoding, error mapping, server lifecycle
    postgres/       repository + transaction implementation (pgx)
    rabbitmq/       task/update publishers and consumers
  worker/           provisioning simulator logic
migrations/         SQL schema migrations
tests/              cross-package integration and concurrency tests
```

Dependencies point inwards: `adapters → application → domain`. Interfaces
are declared by the package that consumes them (Go idiom) rather than in a
shared `ports` package:

| Interface                                              | Declared in   | Implemented by                |
| ------------------------------------------------------ | ------------- | ----------------------------- |
| `TenantRepository`, `TaskRepository`, `OutboxRepository`, `ProcessedUpdateRepository`, `Transactor` | `application` | `postgres.Store`              |
| `TaskPublisher`                                        | `application` | `rabbitmq.TaskPublisher`      |
| `TenantService`, `TaskService`                         | `httpapi`     | `application.*Service`        |
| `UpdateApplier`, `TaskHandler`                         | `rabbitmq`    | `application.TaskService`, `worker.Simulator` |
| `UpdatePublisher`                                      | `worker`      | `rabbitmq.UpdatePublisher`    |

## Planned design

- **Transactional outbox.** Each state-changing call writes the tenant, its
  task and an outbox row in one transaction. `OutboxRelay` publishes events
  only after commit and marks them published after broker confirmation
  (at-least-once).
- **Idempotent consumer.** Every worker update carries an ID. It is recorded
  in the same transaction that applies the update, so redeliveries are no-ops.
  Updates for terminal tasks or that would move a task backwards are ignored.
- **Optimistic locking.** `UPDATE ... WHERE id = $1 AND version = $2`. Zero
  affected rows means `tenant_version_conflict`.

## HTTP API

| Method | Path                      | Description                     | Success |
| ------ | ------------------------- | ------------------------------- | ------- |
| GET    | `/healthz`                | Liveness                        | 200     |
| POST   | `/v1/tenants`             | Create tenant (+ deploy task)   | 201     |
| GET    | `/v1/tenants`             | List tenants (`status`, `limit`, `cursor`) | 200 |
| GET    | `/v1/tenants/{id}`        | Get tenant                      | 200     |
| PATCH  | `/v1/tenants/{id}`        | Partial update; body needs `version` | 202 |
| DELETE | `/v1/tenants/{id}`        | Destroy tenant (+ destroy task) | 202     |
| GET    | `/v1/tenants/{id}/tasks`  | Tasks of one tenant             | 200     |
| GET    | `/v1/tasks`               | List tasks (`tenant_id`, `status`, `limit`, `cursor`) | 200 |
| GET    | `/v1/tasks/{id}`          | Get task                        | 200     |

Timestamps are ISO 8601 UTC with milliseconds, e.g. `2026-01-02T03:04:05.006Z`.

### Errors

Every error has the same body:

```json
{ "error": { "code": "tenant_version_conflict", "message": "tenant version conflict" } }
```

| Code                        | Status |
| --------------------------- | ------ |
| `invalid_request`           | 400    |
| `tenant_not_found`          | 404    |
| `task_not_found`            | 404    |
| `route_not_found`           | 404    |
| `method_not_allowed`        | 405    |
| `tenant_already_exists`     | 409    |
| `tenant_update_not_allowed` | 409    |
| `tenant_version_conflict`   | 409    |
| `internal_error`            | 500    |
| `not_implemented`           | 501 (scaffolding only) |

## Configuration

All configuration comes from environment variables (see `.env.example`).
No credentials are committed; `.env` is git-ignored.

| Variable                  | Default | Used by |
| ------------------------- | ------- | ------- |
| `DATABASE_URL`            | required | api |
| `RABBITMQ_URL`            | required | api, worker |
| `HTTP_ADDR`               | `:8080` | api |
| `HTTP_SHUTDOWN_TIMEOUT`   | `15s`   | api |
| `OUTBOX_POLL_INTERVAL`    | `500ms` | api |
| `LOG_LEVEL`               | `info`  | api, worker |
| `WORKER_FAIL_RATE` / `--fail-rate`         | `0`    | worker |
| `WORKER_MIN_DELAY_MS` / `--min-delay-ms`   | `500`  | worker |
| `WORKER_MAX_DELAY_MS` / `--max-delay-ms`   | `2000` | worker |
