package tests

// End-to-end tests of inbound messaging: worker update -> RabbitMQ ->
// UpdateConsumer -> TaskService.ApplyUpdate -> PostgreSQL, observed
// through the HTTP API. Where the database or broker has to fail, the
// consumer reaches it through a tcpproxy.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/tcpproxy"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

func inboundOptions() rabbitmq.UpdateConsumerOptions {
	return rabbitmq.UpdateConsumerOptions{
		Concurrency:     4,
		MaxAttempts:     5,
		RetryMin:        10 * time.Millisecond,
		RetryMax:        50 * time.Millisecond,
		ApplyTimeout:    2 * time.Second,
		ShutdownTimeout: 2 * time.Second,
	}
}

// outcomeLog wraps an UpdateApplier and records what each call returned.
type outcomeLog struct {
	inner rabbitmq.UpdateApplier
	// after, if set, runs after each successful call; n counts from 1.
	after func(n int)

	mu       sync.Mutex
	outcomes map[string][]application.UpdateOutcome // by update ID
	errs     []error
	calls    int
}

func newOutcomeLog(inner rabbitmq.UpdateApplier) *outcomeLog {
	return &outcomeLog{inner: inner, outcomes: map[string][]application.UpdateOutcome{}}
}

func (l *outcomeLog) ApplyUpdate(ctx context.Context, u domain.TaskUpdate) (application.UpdateOutcome, error) {
	outcome, err := l.inner.ApplyUpdate(ctx, u)
	l.mu.Lock()
	l.calls++
	n := l.calls
	if err != nil {
		l.errs = append(l.errs, err)
	} else {
		l.outcomes[u.ID] = append(l.outcomes[u.ID], outcome)
	}
	l.mu.Unlock()
	if err == nil && l.after != nil {
		l.after(n)
	}
	return outcome, err
}

// of returns the outcomes recorded for update id.
func (l *outcomeLog) of(id string) []application.UpdateOutcome {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]application.UpdateOutcome(nil), l.outcomes[id]...)
}

// count returns how many calls ended with outcome.
func (l *outcomeLog) count(outcome application.UpdateOutcome) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, os := range l.outcomes {
		for _, o := range os {
			if o == outcome {
				n++
			}
		}
	}
	return n
}

func (l *outcomeLog) failures() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.errs...)
}

// startConsumer runs an UpdateConsumer until the test ends; the returned
// function stops it early.
func startConsumer(t *testing.T, brokerURL string, topo rabbitmq.Topology, applier rabbitmq.UpdateApplier, opts rabbitmq.UpdateConsumerOptions) (stop func()) {
	t.Helper()
	c := rabbitmq.NewUpdateConsumer(brokerURL, topo, applier, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("update consumer Run = %v, want context.Canceled", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("update consumer did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// report is the update a worker sends for task: the IDs are the worker's
// (worker.ProgressUpdateID, worker.OutcomeUpdateID) unless id is set.
func report(task taskJSON, status domain.TaskStatus, id string) domain.TaskUpdate {
	u := domain.TaskUpdate{
		ID: worker.OutcomeUpdateID(task.ID), TaskID: task.ID, TenantID: task.TenantID, TaskType: domain.TaskType(task.Type),
		PreviousStatus: domain.TaskStatusInProgress, Status: status, OccurredAt: time.Now(),
	}
	if status == domain.TaskStatusInProgress {
		u.ID, u.PreviousStatus = worker.ProgressUpdateID(task.ID), domain.TaskStatusAccepted
	}
	if status == domain.TaskStatusFailed {
		u.Reason = "simulated failure"
	}
	if id != "" {
		u.ID = id
	}
	return u
}

// send publishes each update with the worker's UpdatePublisher, so with
// publisher confirms: when send returns, the broker has them.
func send(t *testing.T, topo rabbitmq.Topology, updates ...domain.TaskUpdate) {
	t.Helper()
	pub := rabbitmq.NewUpdatePublisher(amqptest.URL(t), topo, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer func() { _ = pub.Close() }()
	for _, u := range updates {
		if err := pub.PublishUpdate(t.Context(), u); err != nil {
			t.Fatalf("publish update %s: %v", u.ID, err)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(deliveryTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// wait waits until the applier has been called n times. Waiting on the
// queue instead would not do: a prefetched, unacked update is not counted
// as a message in it.
func (l *outcomeLog) wait(t *testing.T, n int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d update deliveries", n), func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.calls >= n
	})
}

// assertDrained stops the consumers and checks that every update was
// acked: an unacked one would be back in the queue once they are gone.
func assertDrained(t *testing.T, topo rabbitmq.Topology, stops ...func()) {
	t.Helper()
	for _, stop := range stops {
		stop()
	}
	ch := amqptest.Channel(t)
	for _, queue := range []string{topo.UpdateQueue, topo.DeadLetterQueue} {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if q.Messages != 0 {
			t.Errorf("%d messages left in %s, want 0", q.Messages, queue)
		}
	}
}

func (a *api) processedKeys() int {
	a.t.Helper()
	var n int
	if err := a.pool.QueryRow(a.t.Context(), `SELECT count(*) FROM processed_updates`).Scan(&n); err != nil {
		a.t.Fatal(err)
	}
	return n
}

func (a *api) wantTenant(id, status string, version int64) tenantJSON {
	a.t.Helper()
	tn := a.getTenant(id)
	if tn.Status != status || tn.Version != version {
		a.t.Fatalf("tenant %s is %s v%d, want %s v%d", id, tn.Status, tn.Version, status, version)
	}
	return tn
}

func (a *api) wantTask(id, status string) {
	a.t.Helper()
	if got := a.getTask(id).Status; got != status {
		a.t.Fatalf("task %s is %s, want %s", id, got, status)
	}
}

// TestInboundLifecycle: worker updates published to the broker drive
// tasks and tenants through every outcome mapping: deploy done -> active,
// update done -> active, destroy done -> destroyed, any failure -> failed.
// Every tenant change increments the version.
func TestInboundLifecycle(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stop := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	complete := func(task taskJSON, outcome domain.TaskStatus) {
		send(t, topo, report(task, domain.TaskStatusInProgress, ""), report(task, outcome, ""))
		waitFor(t, "task "+task.ID+" to finish", func() bool { return a.getTask(task.ID).Status == string(outcome) })
	}

	created := a.create("lifecycle", "Lifecycle")
	complete(created.Task, domain.TaskStatusDone)
	a.wantTenant(created.Tenant.ID, "active", 2)

	updated := expect[mutationJSON](t, a.do(http.MethodPatch, "/v1/tenants/"+created.Tenant.ID,
		map[string]any{"name": "Renamed", "version": 2}), http.StatusAccepted)
	complete(updated.Task, domain.TaskStatusDone)
	a.wantTenant(created.Tenant.ID, "active", 4)

	deleted := expect[mutationJSON](t, a.do(http.MethodDelete, "/v1/tenants/"+created.Tenant.ID, nil), http.StatusAccepted)
	complete(deleted.Task, domain.TaskStatusDone)
	a.wantTenant(created.Tenant.ID, "destroyed", 6)

	failing := a.create("failing", "Failing")
	complete(failing.Task, domain.TaskStatusFailed)
	a.wantTenant(failing.Tenant.ID, "failed", 2)

	log.wait(t, 8)
	assertDrained(t, topo, stop)
	if n := a.processedKeys(); n != 8 {
		t.Errorf("processed_updates holds %d keys, want 8 (two per task)", n)
	}
}

// TestInboundDeployStepByStep observes the deploy one report at a time
// through the API: in_progress moves only the task (the tenant is not
// mutated, so its version stays), and done moves both.
func TestInboundDeployStepByStep(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stop := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	m := a.create("steps", "Steps")
	a.wantTask(m.Task.ID, "accepted")
	created := a.wantTenant(m.Tenant.ID, "provisioning", 1)

	send(t, topo, report(m.Task, domain.TaskStatusInProgress, ""))
	log.wait(t, 1)
	a.wantTask(m.Task.ID, "in_progress")
	if got := a.getTenant(m.Tenant.ID); got != created {
		t.Errorf("in_progress changed the tenant: %+v, was %+v", got, created)
	}

	send(t, topo, report(m.Task, domain.TaskStatusDone, ""))
	log.wait(t, 2)
	a.wantTask(m.Task.ID, "done")
	a.wantTenant(m.Tenant.ID, "active", 2)
	if got := log.count(application.UpdateApplied); got != 2 {
		t.Errorf("%d updates applied, want 2", got)
	}
	assertDrained(t, topo, stop)
}

// TestInboundDuplicateDelivery: the same updates delivered several times
// are applied once; every repeat is acked as a duplicate with no effect.
func TestInboundDuplicateDelivery(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stop := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	m := a.create("dupes", "Dupes")
	progress, done := report(m.Task, domain.TaskStatusInProgress, ""), report(m.Task, domain.TaskStatusDone, "")
	send(t, topo, progress, progress, done, done, done)
	log.wait(t, 5)

	a.wantTask(m.Task.ID, "done")
	tn := a.wantTenant(m.Tenant.ID, "active", 2)
	if got := log.of(done.ID); len(got) != 3 || got[0] != application.UpdateApplied || got[1] != application.UpdateDuplicate || got[2] != application.UpdateDuplicate {
		t.Errorf("outcome deliveries = %v, want applied then 2 duplicates", got)
	}
	if got := log.count(application.UpdateApplied); got != 2 {
		t.Errorf("%d updates applied, want 2", got)
	}
	if n := a.processedKeys(); n != 2 {
		t.Errorf("processed_updates holds %d keys, want 2", n)
	}

	// A later redelivery still changes nothing, not even updated_at.
	send(t, topo, done)
	log.wait(t, 6)
	if got := a.getTenant(m.Tenant.ID); got != tn {
		t.Errorf("redelivery changed the tenant: %+v, was %+v", got, tn)
	}
	assertDrained(t, topo, stop)
}

// TestInboundStaleDelivery: the outcome overtakes its in_progress report.
// The outcome is applied; the late in_progress is acked as stale and does
// not move the task back.
func TestInboundStaleDelivery(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stop := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	m := a.create("stale", "Stale")
	send(t, topo, report(m.Task, domain.TaskStatusDone, ""))
	log.wait(t, 1)
	a.wantTask(m.Task.ID, "done")
	tn := a.wantTenant(m.Tenant.ID, "active", 2)
	before := a.getTask(m.Task.ID)

	late := report(m.Task, domain.TaskStatusInProgress, "")
	send(t, topo, late)
	log.wait(t, 2)
	if got := log.of(late.ID); len(got) != 1 || got[0] != application.UpdateStale {
		t.Errorf("late in_progress = %v, want stale", got)
	}
	if got := a.getTask(m.Task.ID); got != before {
		t.Errorf("stale update changed the task: %+v, was %+v", got, before)
	}
	if got := a.getTenant(m.Tenant.ID); got != tn {
		t.Errorf("stale update changed the tenant: %+v, was %+v", got, tn)
	}
	assertDrained(t, topo, stop)
}

// TestInboundTerminalProtection: once a task is terminal nothing moves it
// or its tenant again: not a second outcome (same or contradicting, same
// key or new key), and not a late outcome after the tenant has moved on
// to its next task.
func TestInboundTerminalProtection(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stop := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	m := a.create("terminal", "Terminal")
	send(t, topo, report(m.Task, domain.TaskStatusInProgress, ""), report(m.Task, domain.TaskStatusDone, ""))
	log.wait(t, 2)
	a.wantTenant(m.Tenant.ID, "active", 2)

	rerun := report(m.Task, domain.TaskStatusFailed, "")              // a worker rerun drew failure: same outcome key
	conflicting := report(m.Task, domain.TaskStatusFailed, "other-1") // a different key reporting failure
	again := report(m.Task, domain.TaskStatusDone, "other-2")         // a different key reporting done again
	send(t, topo, rerun, conflicting, again)
	log.wait(t, 5)
	for _, tt := range []struct {
		update domain.TaskUpdate
		want   application.UpdateOutcome
	}{
		{rerun, application.UpdateDuplicate},
		{conflicting, application.UpdateConflict},
		{again, application.UpdateStale},
	} {
		// The rerun shares its key with the done applied above, so look at
		// the latest delivery of each key.
		if got := log.of(tt.update.ID); len(got) == 0 || got[len(got)-1] != tt.want {
			t.Errorf("update %s (%s) = %v, want %s", tt.update.ID, tt.update.Status, got, tt.want)
		}
	}
	a.wantTask(m.Task.ID, "done")
	a.wantTenant(m.Tenant.ID, "active", 2)

	// The tenant moves on to an update task; a late deploy outcome must
	// not drag it back to active (or failed).
	updated := expect[mutationJSON](t, a.do(http.MethodPatch, "/v1/tenants/"+m.Tenant.ID,
		map[string]any{"name": "Next", "version": 2}), http.StatusAccepted)
	send(t, topo, report(m.Task, domain.TaskStatusFailed, "late-1"), report(m.Task, domain.TaskStatusDone, "late-2"))
	log.wait(t, 7)
	a.wantTenant(m.Tenant.ID, "updating", 3)
	a.wantTask(updated.Task.ID, "accepted")
	if errs := log.failures(); len(errs) != 0 {
		t.Errorf("updates failed: %v", errs)
	}
	assertDrained(t, topo, stop)
}

// TestInboundTaskNotFound: the poison-message policy. An update for a
// task that does not exist, or that contradicts the task on record, is
// dead-lettered after one attempt with nothing recorded; so is a message
// that does not decode. The consumer goes on with the next update.
func TestInboundTaskNotFound(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stop := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	m := a.create("poison", "Poison")
	ghost := report(taskJSON{ID: uuid.NewString(), Type: "deploy", TenantID: m.Tenant.ID}, domain.TaskStatusDone, "")
	wrongTenant := report(m.Task, domain.TaskStatusDone, "wrong-tenant")
	wrongTenant.TenantID = uuid.NewString()
	send(t, topo, ghost, wrongTenant)
	// Bytes that are not an update at all never reach the application.
	err := amqptest.Channel(t).PublishWithContext(t.Context(), topo.UpdateExchange,
		rabbitmq.UpdateRoutingKey(domain.TaskStatusDone), true, false, amqp.Publishing{MessageId: "garbage", Body: []byte("{not json")})
	if err != nil {
		t.Fatal(err)
	}

	dead := amqptest.WaitForMessages(t, topo.DeadLetterQueue, 3, deliveryTimeout)
	if len(dead) != 3 {
		t.Fatalf("dead-letter queue holds %d messages, want 3", len(dead))
	}
	for _, d := range dead {
		if d.MessageId != ghost.ID && d.MessageId != wrongTenant.ID && d.MessageId != "garbage" {
			t.Errorf("unexpected dead letter %s", d.MessageId)
		}
		if reason := d.Headers["x-first-death-reason"]; reason != "rejected" {
			t.Errorf("message %s dead-lettered for %v, want rejected", d.MessageId, reason)
		}
	}
	if n := len(log.failures()); n != 2 {
		t.Errorf("%d failed attempts, want 2: permanent failures are not retried", n)
	}
	if n := a.processedKeys(); n != 0 {
		t.Errorf("processed_updates holds %d keys, want 0", n)
	}
	a.wantTask(m.Task.ID, "accepted")
	a.wantTenant(m.Tenant.ID, "provisioning", 1)

	// The consumer is still alive, and the real task's updates apply.
	send(t, topo, report(m.Task, domain.TaskStatusInProgress, ""), report(m.Task, domain.TaskStatusDone, ""))
	log.wait(t, 4)
	a.wantTenant(m.Tenant.ID, "active", 2)
	assertDrained(t, topo, stop)
}

// proxiedPool returns a pool on the same database as pool, reached through
// proxy.
func proxiedPool(t *testing.T, pool *pgxpool.Pool, proxy *tcpproxy.Proxy) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config().Copy()
	host, port, err := net.SplitHostPort(proxy.Addr())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Host, cfg.ConnConfig.Port = host, uint16(p)
	cfg.ConnConfig.ConnectTimeout = time.Second
	proxied, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxied.Close)
	return proxied
}

// TestInboundTransientDatabaseFailure: while PostgreSQL is unreachable
// the update is retried with backoff, neither dropped nor dead-lettered,
// and it is applied once the database is back.
func TestInboundTransientDatabaseFailure(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	cfg := a.pool.Config().ConnConfig
	proxy := tcpproxy.Start(t, net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))), tcpproxy.Refuse)
	store := postgres.NewStore(proxiedPool(t, a.pool, proxy))
	log := newOutcomeLog(application.NewTaskService(store, store, time.Now))
	opts := inboundOptions()
	opts.MaxAttempts = 3 // a few in-process attempts, then requeue and try again
	stop := startConsumer(t, amqptest.URL(t), topo, log, opts)

	m := a.create("db-down", "DB down")
	send(t, topo, report(m.Task, domain.TaskStatusInProgress, ""), report(m.Task, domain.TaskStatusDone, ""))
	waitFor(t, "failed attempts while the database is down", func() bool { return len(log.failures()) >= 6 })
	for _, err := range log.failures() {
		if application.IsPermanentUpdateError(err) {
			t.Fatalf("database outage classified as permanent: %v", err)
		}
	}
	a.wantTenant(m.Tenant.ID, "provisioning", 1)

	proxy.SetMode(tcpproxy.Forward)
	waitFor(t, "both updates to be applied", func() bool { return log.count(application.UpdateApplied)+log.count(application.UpdateStale) == 2 })
	a.wantTask(m.Task.ID, "done")
	a.wantTenant(m.Tenant.ID, "active", 2)
	// Nothing dead-lettered during the outage, nothing left behind.
	assertDrained(t, topo, stop)
}

// TestInboundCrashAfterCommitBeforeAck: the consumer loses its broker
// connection after the database commit but before the ack, as if the
// process had crashed. The broker redelivers; the redelivery is found to
// be a duplicate and acked, and the tenant changed exactly once.
func TestInboundCrashAfterCommitBeforeAck(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), tcpproxy.Forward)
	log := newOutcomeLog(a.tasks)
	log.after = func(n int) {
		if n == 1 {
			proxy.Cut() // committed; the ack never reaches the broker
		}
	}
	opts := inboundOptions()
	opts.Concurrency = 1
	stop := startConsumer(t, amqptest.ProxiedURL(t, proxy.Addr()), topo, log, opts)

	m := a.create("crash", "Crash")
	done := report(m.Task, domain.TaskStatusDone, "")
	send(t, topo, done)
	waitFor(t, "the redelivery", func() bool { return len(log.of(done.ID)) == 2 })
	assertDrained(t, topo, stop)

	if got := log.of(done.ID); got[0] != application.UpdateApplied || got[1] != application.UpdateDuplicate {
		t.Errorf("deliveries = %v, want applied then duplicate", got)
	}
	a.wantTask(m.Task.ID, "done")
	a.wantTenant(m.Tenant.ID, "active", 2)
	if n := a.processedKeys(); n != 1 {
		t.Errorf("processed_updates holds %d keys, want 1", n)
	}
}

// TestInboundConcurrentDeliveries: two control-plane consumers, each
// applying updates in parallel, receive a shuffled stream of updates for
// many tenants: every update three times, plus for half the tenants a
// racing contradicting outcome under another key. Each task ends in
// exactly one terminal state, its tenant matches it, and every tenant was
// changed exactly once.
func TestInboundConcurrentDeliveries(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	log := newOutcomeLog(a.tasks)
	stopA := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())
	stopB := startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())

	const tenants = 20
	var created []mutationJSON
	var updates []domain.TaskUpdate
	keys := map[string]bool{}
	for i := range tenants {
		m := a.create(fmt.Sprintf("tenant-%02d", i), "T")
		created = append(created, m)
		batch := []domain.TaskUpdate{report(m.Task, domain.TaskStatusInProgress, ""), report(m.Task, domain.TaskStatusDone, "")}
		if i%2 == 1 {
			batch = append(batch, report(m.Task, domain.TaskStatusFailed, "rival-"+m.Task.ID))
		}
		for _, u := range batch {
			keys[u.ID] = true
			updates = append(updates, u, u, u)
		}
	}
	rand.Shuffle(len(updates), func(i, j int) { updates[i], updates[j] = updates[j], updates[i] })

	var wg sync.WaitGroup
	for chunk := range 4 { // publish from several connections at once, too
		wg.Go(func() {
			for i := chunk; i < len(updates); i += 4 {
				send(t, topo, updates[i])
			}
		})
	}
	wg.Wait()
	log.wait(t, len(updates))
	assertDrained(t, topo, stopA, stopB)

	if errs := log.failures(); len(errs) != 0 {
		t.Fatalf("%d updates failed, first: %v", len(errs), errs[0])
	}
	for _, m := range created {
		task, tn := a.getTask(m.Task.ID), a.getTenant(m.Tenant.ID)
		want := map[string]string{"done": "active", "failed": "failed"}[task.Status]
		if want == "" || tn.Status != want || tn.Version != 2 {
			t.Errorf("tenant %s: task %s, tenant %s v%d; want terminal task, matching tenant at v2", m.Tenant.Slug, task.Status, tn.Status, tn.Version)
		}
	}
	if n := a.processedKeys(); n != len(keys) {
		t.Errorf("processed_updates holds %d keys, want %d (one per distinct update)", n, len(keys))
	}
	if dup := log.count(application.UpdateDuplicate); dup != 2*len(keys) {
		t.Errorf("%d duplicates, want %d (two repeats per update)", dup, 2*len(keys))
	}
}

// startWorker runs the worker simulator, with no delays and the given
// fail rate, until the test ends.
func startWorker(t *testing.T, topo rabbitmq.Topology, failRate float64) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pub := rabbitmq.NewUpdatePublisher(amqptest.URL(t), topo, logger)
	t.Cleanup(func() { _ = pub.Close() })
	sim, err := worker.NewSimulator(config.Simulation{FailRate: failRate}, pub, rand.New(rand.NewPCG(1, 2)), logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- rabbitmq.NewTaskConsumer(amqptest.URL(t), topo, sim, rabbitmq.TaskConsumerOptions{}, logger).Run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-workerDone })
}

// TestInboundFullLoop: the whole system with nothing simulated but the
// provisioning: API -> outbox -> relay -> broker -> worker simulator ->
// broker -> update consumer -> database. --fail-rate=0 ends active,
// --fail-rate=1 ends failed.
func TestInboundFullLoop(t *testing.T) {
	for _, tt := range []struct {
		failRate   float64
		wantTask   string
		wantTenant string
	}{
		{0, "done", "active"},
		{1, "failed", "failed"},
	} {
		t.Run(fmt.Sprintf("fail-rate=%v", tt.failRate), func(t *testing.T) {
			a := newAPI(t)
			topo := workerTopology(t)
			startRelay(t, a.store.Outbox(), amqptest.URL(t), topo, relayOptions())
			startConsumer(t, amqptest.URL(t), topo, a.tasks, inboundOptions())
			startWorker(t, topo, tt.failRate)

			m := a.create("full-loop", "Full loop")
			waitFor(t, "the deploy task to finish", func() bool { return a.getTask(m.Task.ID).Status == tt.wantTask })
			a.wantTenant(m.Tenant.ID, tt.wantTenant, 2)
		})
	}
}
