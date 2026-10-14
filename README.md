# Tenant Provisioning Control Plane

A control-plane service that records tenants, emits provisioning tasks over
RabbitMQ and applies progress reported back by a worker simulator.

> **Status:** the domain, PostgreSQL persistence, application use cases,
> the HTTP API and outbound messaging (transactional outbox -> RabbitMQ)
> are implemented and tested. The inbound update consumer and the worker
> simulator are still stubs, so task events reach the
> `tenant.tasks.worker` queue but nothing processes them yet, and tenants
> stay in `provisioning` when run locally.

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
  testsupport/
    pgtest/         per-test PostgreSQL databases for integration tests
    amqptest/       RabbitMQ broker and per-test topologies for integration tests
    tcpproxy/       TCP proxy that refuses, cuts or blackholes connections on demand
  worker/           provisioning simulator logic
migrations/         SQL schema migrations
tests/              end-to-end tests: HTTP API (real router, services, PostgreSQL)
                    and outbound messaging (API -> outbox -> relay -> RabbitMQ)
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

## Design

- **Transactional outbox.** See [Outbound messaging](#outbound-messaging).
- **Idempotent consumer** (planned). Every worker update carries an ID. It
  is recorded in the same transaction that applies the update, so
  redeliveries are no-ops. Updates for terminal tasks or that would move a
  task backwards are ignored.
- **Optimistic locking.** `UPDATE ... WHERE id = $1 AND version = $2`. Zero
  affected rows means `tenant_version_conflict`.

## Outbound messaging

Every state-changing API call (create, PATCH, DELETE) publishes exactly one
task event, and only after the change is durably committed.

```
 HTTP request
     |
     v
 TenantService ── one transaction ──────────────────────────────┐
     |   INSERT/UPDATE tenants                                  │
     |   INSERT tasks                                           │
     |   INSERT outbox_events (task_id, status snapshot)        │
     └─ COMMIT ─────────────────────────────────────────────────┘
                     (201/202 returned to the client)

 OutboxRelay (background, one per replica)
     1. claim   UPDATE outbox_events SET available_at = now()+lease ...  (own short statement, committed)
     2. publish PublishTask -> RabbitMQ, wait for publisher confirm      (no transaction open)
     3. mark    UPDATE outbox_events SET published_at = now()            (only after the confirm)
        or on failure: attempts+1, last_error, available_at = now()+backoff

 RabbitMQ
     exchange tenant.tasks (topic, durable) --task.#--> queue tenant.tasks.worker (quorum, durable)
```

### Why an outbox

The database and the broker are two systems with no shared transaction.
Whichever order you write them in, a crash in between leaves them
disagreeing:

| Approach | Failure | Result |
| --- | --- | --- |
| publish, then commit | commit fails or the process dies | the worker provisions a tenant that does not exist (the brief's "classic bug") |
| commit, then publish in the request | broker down, or the process dies after commit | the tenant is stuck in `provisioning` forever; the event is lost |
| **outbox**: commit the event *with* the change, publish afterwards | anything | the event exists if and only if the change committed, and is retried until it reaches the broker |

The outbox row is written by the same transaction as the tenant and task
(`recordTask` in `application/tenant_service.go`), so a rollback removes
it too. The relay only ever sees committed rows: its claim is a separate
statement under READ COMMITTED, so rows of in-flight transactions are
invisible. It selects by `published_at IS NULL`, never "id above the last
one seen", so an event whose transaction commits after a later-numbered
one is not skipped.

### Why it is still not exactly-once

The outbox makes publication *reliable*, not *unique*. Marking the row
published is a second write to the database after the broker has
accepted the message, and nothing can make the two atomic:

- **Crash between confirm and mark.** The broker has the message, the row
  still says unpublished. When the lease expires the event is published
  again.
- **Lost confirmation.** The connection drops after the broker stored the
  message but before the confirm arrives. The outcome is unknown, so the
  relay must retry; if the first attempt landed, that is a duplicate. (The
  transient-failure test regularly logs a few of these.)
- **Expired lease.** A relay stalled past its lease (GC pause, network
  partition) while another relay re-claims and publishes the same event.

The only alternative to "maybe twice" is "maybe never", which the brief
rules out. So delivery is **at-least-once**, and duplicates are made
harmless instead:

- The task ID is the event ID and the AMQP `message-id`. Every copy of
  an event is byte-identical (it is a snapshot taken at commit time).
- Consumers must be idempotent on that ID. The worker can skip task IDs it
  has already handled, and the control plane's inbound side deduplicates
  worker updates by their own IDs (`processed_updates`) and ignores stale
  transitions.

### Relay behaviour

- **Claiming without holding a transaction.** A claim is one committed
  `UPDATE ... RETURNING` that moves `available_at` to now + lease (30s) and
  sets a fresh `claim_token`. `FOR UPDATE SKIP LOCKED` makes concurrent
  claims pick disjoint rows; after the commit, the lease keeps other
  relays off the row. No row lock or transaction is held while waiting for
  the broker. Lease and retry times use the database clock, so relays on
  different hosts agree.
- **Several relays.** Every control-plane replica runs a relay; they share
  the outbox safely through leases (`TestOutboxConcurrentRelays`).
- **Confirm before mark.** `PublishTask` returns only after the broker's
  publisher confirm. A nack, a closed channel, a timeout or a returned
  message all count as failures. The mark runs on a context that survives
  shutdown, so a confirm received during shutdown is still recorded.
- **Retries.** A failed publish increments `attempts`, stores
  `last_error` and makes the event due again after an exponential backoff
  from `OUTBOX_RETRY_MIN` to `OUTBOX_RETRY_MAX` with jitter. The rest of
  the batch is handed back untried (no attempt charged). Database
  failures back the whole loop off the same way. Failures are logged as
  warnings, and as errors from the 5th attempt on.
- **Never discarded.** There is no attempt limit and no dead-letter
  table: an event stays in the outbox until the broker confirms it.
  Stuck events are visible with
  `SELECT task_id, attempts, last_error FROM outbox_events WHERE published_at IS NULL`.
- **Crash recovery.** If a relay dies with a claimed batch, the events
  become claimable again when the lease expires (`TestOutboxAbandonedClaimIsRedelivered`).
  A relay also stops starting new publishes when less than two publish
  timeouts of its lease remain, and hands the rest of the batch back, so
  a slow broker cannot make a lease expire mid-publish.
- **Startup and shutdown.** The broker being down at startup is not fatal:
  the API serves requests, events accumulate, and the publisher connects
  when the broker appears. On SIGINT/SIGTERM the relay stops claiming,
  hands back untried events for immediate pickup, and the publisher's
  connection is closed after the relay has stopped.

### Broker topology and message format

| Object | Name | Properties |
| --- | --- | --- |
| Exchange | `tenant.tasks` | topic, durable |
| Queue | `tenant.tasks.worker` | quorum (replicated, always durable), bound with `task.#` |
| Routing key | `task.<type>` | `task.deploy`, `task.update`, `task.destroy` |

The publisher declares the exchange, queue and binding on every connect
(idempotently), so events published before any worker starts are kept, not
dropped as unroutable. Messages are published with `mandatory`: if no queue
is bound, the broker returns the message, the publish fails and is retried,
and the next connection re-declares the topology.

AMQP properties: `message_id` = task ID, `delivery_mode` = 2 (persistent),
`content_type` = `application/json`, `type` = `task`, `app_id` =
`tenant-control-plane`. Body (the task as committed):

```json
{
  "id": "6f1c...",
  "type": "deploy",
  "tenant_id": "0b7e...",
  "status": "accepted",
  "created_at": "2026-01-02T03:04:05.006Z",
  "updated_at": "2026-01-02T03:04:05.006Z"
}
```

Inspecting it locally (management UI at <http://localhost:15672>,
credentials from `.env`):

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name type durable messages
docker compose exec postgres psql -U control_plane -d control_plane \
  -c "SELECT task_id, published_at, attempts, last_error FROM outbox_events ORDER BY id DESC LIMIT 10"
# Simulate a broker outage: the API keeps accepting work, events pile up
# with growing attempts, and are delivered once the broker is back.
docker compose stop rabbitmq; sleep 10; docker compose start rabbitmq
```

### Tests

| Behaviour | Test |
| --- | --- |
| tenant, task and event commit or roll back together | `postgres/tx_test.go`, `postgres/application_test.go` |
| only committed events are claimable | `TestOutboxClaimSeesOnlyCommittedEvents` |
| concurrent claims are disjoint | `TestOutboxConcurrentClaimsAreDisjoint`, `TestOutboxConcurrentRelays` |
| every mutation publishes its task; rejected calls publish nothing | `TestOutboxPublishesEveryMutation` |
| broker unavailable at startup | `TestOutboxBrokerUnavailableAtStartup`, `TestPublishTaskBrokerUnavailableAtStartup` |
| transient failures (connections cut repeatedly) | `TestOutboxTransientBrokerFailures`, `TestPublishTaskReconnects` |
| crash between confirm and mark: same event redelivered | `TestOutboxCrashWindowRedelivers`, `TestRelayCrashWindowRepublishes` |
| abandoned claim redelivered after the lease | `TestOutboxAbandonedClaimIsRedelivered` |
| broker nack, unroutable message, unresponsive broker | `TestPublishTaskNacked`, `TestPublishTaskUnroutable`, `TestPublishTaskUnresponsiveBroker` |
| durable exchange, quorum queue, persistent messages | `TestTopologyIsDurable`, `TestPublishTaskMessage` |
| backoff, retry-until-published, shutdown mid-publish | `outbox_relay_test.go` |

Integration tests start PostgreSQL and RabbitMQ containers with
testcontainers (or use `TEST_DATABASE_URL` / `TEST_RABBITMQ_URL`) and skip
under `-short` or without Docker. Broker failures are injected with
`testsupport/tcpproxy` between the relay and RabbitMQ.

## HTTP API

All endpoints speak JSON. Request bodies must be sent with
`Content-Type: application/json`. Timestamps are ISO 8601 UTC with
milliseconds, e.g. `2026-01-02T03:04:05.006Z`.

| Method | Path                     | Description                              | Success |
| ------ | ------------------------ | ---------------------------------------- | ------- |
| GET    | `/healthz`               | Liveness (process is serving HTTP)       | 200     |
| POST   | `/v1/tenants`            | Create a tenant and its `deploy` task    | 201     |
| GET    | `/v1/tenants`            | List tenants                             | 200     |
| GET    | `/v1/tenants/{tenantID}` | Get a tenant                             | 200     |
| PATCH  | `/v1/tenants/{tenantID}` | Partially update; creates an `update` task | 202   |
| DELETE | `/v1/tenants/{tenantID}` | Destroy; creates a `destroy` task        | 202     |
| GET    | `/v1/tasks`              | List tasks                               | 200     |
| GET    | `/v1/tasks/{taskID}`     | Get a task                               | 200     |

Tasks are read-only over HTTP. They are created only by tenant calls and
driven only by the worker.

### Resources

```jsonc
// Tenant
{ "id": "7ca4199d-…", "slug": "acme", "name": "Acme", "status": "active",
  "version": 2, "created_at": "2026-10-14T18:24:01.434Z", "updated_at": "2026-10-14T18:24:03.101Z" }

// Task
{ "id": "7b29519c-…", "type": "deploy", "tenant_id": "7ca4199d-…", "status": "done",
  "created_at": "2026-10-14T18:24:01.434Z", "updated_at": "2026-10-14T18:24:03.101Z" }
```

The JSON models are separate types built from the domain model field by
field. Database rows and internal fields are never serialized directly.

POST, PATCH and DELETE all return the tenant after the change together with
the task that tracks the asynchronous work:

```json
{ "tenant": { …tenant… }, "task": { …task… } }
```

### Create

```bash
curl -si localhost:8080/v1/tenants -H 'Content-Type: application/json' \
  -d '{"slug":"acme","name":"Acme"}'
# 201 Created, Location: /v1/tenants/<id>
# tenant.status = "provisioning", tenant.version = 1, task.type = "deploy", task.status = "accepted"
```

Both fields are required:

- `slug`: 3-28 characters matching `^[a-z][-a-z0-9]{1,26}[a-z0-9]$`. It is
  unique forever: a destroyed tenant keeps its slug.
- `name`: must not be blank.

Any other field is rejected.

### Update (PATCH) and optimistic locking

```bash
curl -si -X PATCH localhost:8080/v1/tenants/<id> -H 'Content-Type: application/json' \
  -d '{"name":"Acme Corp","version":2}'
# 202 Accepted, Location: /v1/tasks/<update task id>
```

- **Partial semantics.** Omitted fields are left unchanged. `null` is
  rejected, because no patchable field can be cleared. At least one field
  must be given. The only patchable field is `name`. The slug is immutable,
  so sending it is an "unsupported field" error.
- **`version` is required.** It is the `version` the client last read.
  Every mutation increments it, including status changes applied by the
  worker. If the tenant has changed since that read, the PATCH fails with
  `409 tenant_version_conflict` and changes nothing. The client should GET
  the tenant again and retry. The check is enforced by a conditional write
  (`UPDATE … WHERE version = $n`), so of two concurrent PATCHes carrying the
  same version, at most one succeeds.
- The expected version goes in the body rather than an `If-Match` header.
  That keeps the precondition visible in the same JSON document as the
  change, and the brief's `tenant_version_conflict` maps naturally to 409.
- **Order of checks:** the version is checked before the status. A stale
  version gives `tenant_version_conflict`. A current version on a tenant
  that is not `active` gives `tenant_update_not_allowed`.
- On success the tenant becomes `updating` with an `update` task. It returns
  to `active` (or `failed`) when the worker reports the outcome.

### Delete

```bash
curl -si -X DELETE localhost:8080/v1/tenants/<id>
# 202 Accepted, Location: /v1/tasks/<destroy task id>
```

DELETE **requests** destruction; it does not remove anything synchronously:

- It is allowed only from `active` or `failed`. The tenant becomes
  `destroying` and a `destroy` task is created and published to the worker.
- When the worker reports the destroy as `done`, the tenant becomes
  `destroyed`. If it reports `failed`, the tenant becomes `failed`, and
  DELETE can be retried from there.
- The tenant record is **kept** (soft delete). `GET /v1/tenants/{id}` keeps
  returning it with `status: "destroyed"`, not 404, so its history and tasks
  remain traceable. Its slug is never reusable.
- DELETE on a tenant that is `provisioning`, `updating`, `destroying` or
  already `destroyed` returns `409 tenant_update_not_allowed`. Repeating a
  DELETE therefore never creates a second destroy task.
- DELETE takes no body and no expected version. It is still a conditional
  write, so if the tenant changes between the read and the write (for
  example, a racing PATCH), it fails with `409 tenant_version_conflict`.

Track progress with `GET /v1/tasks/{taskID}` (the `Location` header) or
`GET /v1/tenants/{tenantID}`.

### Listing: pagination and filtering

```bash
curl -s 'localhost:8080/v1/tenants?status=active&limit=20'
curl -s 'localhost:8080/v1/tasks?tenant_id=<id>&status=in_progress'
curl -s 'localhost:8080/v1/tenants?limit=20&cursor=<next_cursor from the previous page>'
```

```json
{ "items": [ … ], "next_cursor": "eyJjIjoi…" }
```

| Parameter   | Endpoints                  | Meaning                                                |
| ----------- | -------------------------- | ------------------------------------------------------ |
| `limit`     | both lists                 | Page size, 1-200, default 50                           |
| `cursor`    | both lists                 | Opaque `next_cursor` of the previous page              |
| `status`    | `/v1/tenants`, `/v1/tasks` | Exact status match; must be a valid status of that resource |
| `tenant_id` | `/v1/tasks`                | Tasks of one tenant                                    |

- Lists are ordered by creation time, oldest first. Pagination is keyset
  (cursor) based rather than offset based, so pages stay stable while new
  rows are inserted.
- `next_cursor` is omitted on the last page.
- Filters combine with AND.
- Unknown parameters, repeated parameters, an out-of-range `limit`, an
  unknown `status` or a malformed `cursor` give `400 invalid_request`. A
  typo is rejected rather than silently returning everything (or nothing).
- An empty value (`?status=`) is the same as omitting the parameter.
- `tenant_id` of a tenant that does not exist returns an empty list.

### Errors

Every error, including unknown routes and panics, has the same body:

```json
{ "error": { "code": "invalid_request", "message": "invalid slug: must be 3-28 characters, got 2", "field": "slug" } }
```

- `code` is stable and machine-readable; branch on it.
- `message` is for humans.
- `field` is present when one body field or query parameter is at fault.

| Code                        | Status | When |
| --------------------------- | ------ | ---- |
| `invalid_request`           | 400    | Malformed JSON, missing/null/mistyped/unsupported field, invalid slug or name, missing `version`, bad query parameter |
| `tenant_not_found`          | 404    | No tenant with that ID (any malformed ID too) |
| `task_not_found`            | 404    | No task with that ID |
| `route_not_found`           | 404    | No such path |
| `method_not_allowed`        | 405    | Path exists, method does not (`Allow` header lists the valid ones) |
| `tenant_already_exists`     | 409    | Slug taken, including by a destroyed tenant |
| `tenant_update_not_allowed` | 409    | PATCH or DELETE not allowed in the tenant's current status |
| `tenant_version_conflict`   | 409    | `version` is stale, or the tenant changed during the request |
| `payload_too_large`         | 413    | Body over 1 MiB |
| `unsupported_media_type`    | 415    | Body not sent as `application/json` |
| `internal_error`            | 500    | Anything unexpected |
| `request_timeout`           | 503    | The request did not complete within `HTTP_REQUEST_TIMEOUT` |

The three 409s share a status because each means "the request conflicts
with the tenant's current state". The code tells them apart.

- **Where the mapping lives.** The whole mapping is one function,
  `classify` in
  [`internal/adapters/httpapi/errors.go`](internal/adapters/httpapi/errors.go).
  Handlers only return errors.
- **What the client sees.** For contract errors the message is the error's
  fixed text. Validation errors show only the rule that was broken.
  Anything else becomes an opaque `internal_error`. Its details (database
  errors, stack traces of recovered panics) go to the log, tagged with the
  request ID, and never reach the client.

### Operational behaviour

- **Request IDs and logging.** Every request gets an `X-Request-ID`. A
  client-supplied ID is used if it is 1-64 characters of `[A-Za-z0-9._-]`;
  otherwise a UUID is generated. The ID is echoed in the response, and one
  JSON log line is written per request with `request_id`, `method`,
  `path`, `status`, `bytes` and `duration_ms`.
- **Timeouts.** `HTTP_REQUEST_TIMEOUT` (default 10s) is a deadline on the
  request context. When it expires, database calls are cancelled and the
  client gets `503 request_timeout`. The server also sets
  `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout` (which must exceed the
  request timeout) and `IdleTimeout`.
- **Graceful shutdown.** On SIGINT/SIGTERM the server stops accepting
  connections, waits up to `HTTP_SHUTDOWN_TIMEOUT` (default 15s) for
  in-flight requests to finish, then stops the background components and
  exits.

## Configuration

All configuration comes from environment variables (see `.env.example`).
No credentials are committed; `.env` is git-ignored.

| Variable                  | Default | Used by |
| ------------------------- | ------- | ------- |
| `DATABASE_URL`            | required | api |
| `RABBITMQ_URL`            | required | api, worker |
| `HTTP_ADDR`               | `:8080` | api |
| `HTTP_REQUEST_TIMEOUT`    | `10s`   | api (must be below `HTTP_WRITE_TIMEOUT`) |
| `HTTP_READ_HEADER_TIMEOUT` / `HTTP_READ_TIMEOUT` / `HTTP_WRITE_TIMEOUT` / `HTTP_IDLE_TIMEOUT` | `5s` / `15s` / `15s` / `60s` | api |
| `HTTP_SHUTDOWN_TIMEOUT`   | `15s`   | api |
| `OUTBOX_POLL_INTERVAL`    | `500ms` | api: wait between polls when nothing is due |
| `OUTBOX_BATCH_SIZE`       | `100`   | api: events claimed per poll |
| `OUTBOX_LEASE`            | `30s`   | api: claim duration; must exceed 2 x `OUTBOX_PUBLISH_TIMEOUT` |
| `OUTBOX_PUBLISH_TIMEOUT`  | `5s`    | api: bound on one publish incl. confirm, and on the mark |
| `OUTBOX_RETRY_MIN` / `OUTBOX_RETRY_MAX` | `500ms` / `30s` | api: retry backoff bounds |
| `LOG_LEVEL`               | `info`  | api, worker |
| `WORKER_FAIL_RATE` / `--fail-rate`         | `0`    | worker |
| `WORKER_MIN_DELAY_MS` / `--min-delay-ms`   | `500`  | worker |
| `WORKER_MAX_DELAY_MS` / `--max-delay-ms`   | `2000` | worker |
