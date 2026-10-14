package tests

// End-to-end tests of outbound messaging: HTTP API -> PostgreSQL (tenant,
// task and outbox event in one transaction) -> OutboxRelay -> RabbitMQ.
// Where the broker has to fail, the relay reaches it through a tcpproxy.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/tcpproxy"
)

const deliveryTimeout = 15 * time.Second

// workerTopology returns a test-unique topology, declared as the worker
// does when it starts: the queue exists before anything is published.
func workerTopology(t *testing.T) rabbitmq.Topology {
	t.Helper()
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	return topo
}

func relayOptions() application.OutboxRelayOptions {
	return application.OutboxRelayOptions{
		PollInterval:   20 * time.Millisecond,
		BatchSize:      10,
		Lease:          2 * time.Second,
		PublishTimeout: 500 * time.Millisecond,
		RetryMin:       20 * time.Millisecond,
		RetryMax:       200 * time.Millisecond,
	}
}

// startRelay runs a relay with its own publisher until the returned stop
// function is called (or the test ends). stop returns Run's error.
func startRelay(t *testing.T, outbox application.OutboxRepository, brokerURL string, topo rabbitmq.Topology, opts application.OutboxRelayOptions) (stop func() error) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	publisher := rabbitmq.NewTaskPublisher(brokerURL, topo, logger)
	relay, err := application.NewOutboxRelay(outbox, publisher, opts, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(10 * time.Second):
				t.Error("relay did not stop after cancellation")
			}
			_ = publisher.Close()
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// The task event as a worker sees it, decoded independently of the
// rabbitmq package.
type taskEvent struct {
	MessageID string
	Task      taskJSON
}

func decodeEvents(t *testing.T, msgs []amqp.Delivery) []taskEvent {
	t.Helper()
	out := make([]taskEvent, len(msgs))
	for i, m := range msgs {
		out[i].MessageID = m.MessageId
		if err := json.Unmarshal(m.Body, &out[i].Task); err != nil {
			t.Fatalf("message %d body %s: %v", i, m.Body, err)
		}
		if m.DeliveryMode != amqp.Persistent {
			t.Errorf("message %s is not persistent", m.MessageId)
		}
	}
	return out
}

// deliveries counts messages per message ID: the deduplication a consumer
// does with the event ID.
func deliveries(events []taskEvent) map[string]int {
	n := map[string]int{}
	for _, e := range events {
		n[e.MessageID]++
	}
	return n
}

// outboxState reads one event's delivery state.
func (a *api) outboxState(taskID string) (published bool, attempts int, lastError *string) {
	a.t.Helper()
	err := a.pool.QueryRow(a.t.Context(),
		`SELECT published_at IS NOT NULL, attempts, last_error FROM outbox_events WHERE task_id = $1`, taskID).
		Scan(&published, &attempts, &lastError)
	if err != nil {
		a.t.Fatal(err)
	}
	return published, attempts, lastError
}

func (a *api) pendingEvents() int {
	a.t.Helper()
	var n int
	if err := a.pool.QueryRow(a.t.Context(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
		a.t.Fatal(err)
	}
	return n
}

func (a *api) waitAllPublished() {
	a.t.Helper()
	deadline := time.Now().Add(deliveryTimeout)
	for a.pendingEvents() > 0 {
		if time.Now().After(deadline) {
			a.t.Fatalf("%d outbox events still unpublished after %v", a.pendingEvents(), deliveryTimeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestOutboxPublishesEveryMutation: each state-changing API call publishes
// its task, carrying id, type, tenant_id and status, with the task ID as
// message ID; a rejected call publishes nothing.
func TestOutboxPublishesEveryMutation(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	startRelay(t, a.store.Outbox(), amqptest.URL(t), topo, relayOptions())

	created := a.create("acme", "Acme")
	a.finish(created.Task.ID, domain.TaskStatusDone)
	tn := a.getTenant(created.Tenant.ID)
	updated := expect[mutationJSON](t, a.do(http.MethodPatch, "/v1/tenants/"+tn.ID,
		map[string]any{"name": "Acme Corp", "version": tn.Version}), http.StatusAccepted)
	a.finish(updated.Task.ID, domain.TaskStatusDone)
	deleted := expect[mutationJSON](t, a.do(http.MethodDelete, "/v1/tenants/"+tn.ID, nil), http.StatusAccepted)
	// Rejected: rolled back, so nothing may be published for them.
	expectError(t, a.do(http.MethodPost, "/v1/tenants", map[string]any{"slug": "acme", "name": "Dup"}),
		http.StatusConflict, "tenant_already_exists")
	expectError(t, a.do(http.MethodDelete, "/v1/tenants/"+tn.ID, nil), http.StatusConflict, "tenant_update_not_allowed")

	events := decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, 3, deliveryTimeout))
	want := []taskJSON{created.Task, updated.Task, deleted.Task}
	if len(events) != len(want) {
		t.Fatalf("received %d events, want %d", len(events), len(want))
	}
	for i, w := range want {
		if events[i].Task != w || events[i].MessageID != w.ID || w.Status != "accepted" {
			t.Errorf("event %d = %+v, want task %+v with message ID %s", i, events[i], w, w.ID)
		}
	}
	a.waitAllPublished()
	time.Sleep(200 * time.Millisecond) // several polls: nothing else arrives
	if extra := amqptest.Drain(t, topo.Queue); len(extra) != 0 {
		t.Errorf("%d unexpected extra messages", len(extra))
	}
}

// TestOutboxBrokerUnavailableAtStartup: with the broker down, the API keeps
// accepting work, the relay keeps retrying with backoff (recording each
// failure), and the backlog is delivered once the broker is back.
func TestOutboxBrokerUnavailableAtStartup(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), tcpproxy.Refuse)
	startRelay(t, a.store.Outbox(), amqptest.ProxiedURL(t, proxy.Addr()), topo, relayOptions())

	first := a.create("tenant-a", "A")
	second := a.create("tenant-b", "B")
	deadline := time.Now().Add(deliveryTimeout)
	for {
		_, attempts, lastErr := a.outboxState(first.Task.ID)
		if attempts >= 3 && lastErr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay recorded %d failed attempts, want it to keep retrying", attempts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if published, _, _ := a.outboxState(first.Task.ID); published {
		t.Fatal("event marked published while the broker was unreachable")
	}

	proxy.SetMode(tcpproxy.Forward)
	events := decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, 2, deliveryTimeout))
	got := deliveries(events)
	if len(got) != 2 || got[first.Task.ID] == 0 || got[second.Task.ID] == 0 {
		t.Errorf("delivered %v, want both tasks", got)
	}
	a.waitAllPublished()
}

// TestOutboxTransientBrokerFailures: connections are cut repeatedly while
// tenants are created concurrently. Every event is still delivered (at
// least once), and every outbox row ends up published.
func TestOutboxTransientBrokerFailures(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), tcpproxy.Forward)
	startRelay(t, a.store.Outbox(), amqptest.ProxiedURL(t, proxy.Addr()), topo, relayOptions())

	var chaos sync.WaitGroup
	stopChaos := make(chan struct{})
	chaos.Go(func() {
		for {
			select {
			case <-stopChaos:
				return
			case <-time.After(25 * time.Millisecond):
				proxy.Cut()
			}
		}
	})
	const tenants = 20
	taskIDs := make([]string, tenants)
	var wg sync.WaitGroup
	for i := range tenants {
		wg.Go(func() { taskIDs[i] = a.create(fmt.Sprintf("tenant-%02d", i), "T").Task.ID })
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	close(stopChaos)
	chaos.Wait()

	a.waitAllPublished()
	got := deliveries(decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, tenants, deliveryTimeout)))
	duplicates := 0
	for _, id := range taskIDs {
		if got[id] == 0 {
			t.Errorf("task %s was never delivered", id)
		}
		duplicates += got[id] - 1
	}
	if len(got) != tenants {
		t.Errorf("delivered %d distinct events, want %d", len(got), tenants)
	}
	t.Logf("%d events delivered, %d duplicates after connection cuts", tenants, duplicates)
}

// crashAfterConfirm is an outbox whose first MarkPublished fails, as if
// the relay crashed after the broker confirmed but before the mark.
type crashAfterConfirm struct {
	application.OutboxRepository
	crashed atomic.Bool
}

func (c *crashAfterConfirm) MarkPublished(ctx context.Context, taskID string) error {
	if c.crashed.CompareAndSwap(false, true) {
		return errors.New("simulated crash before marking published")
	}
	return c.OutboxRepository.MarkPublished(ctx, taskID)
}

// TestOutboxCrashWindowRedelivers: the event is delivered, the mark is
// lost, and after the lease the event is delivered again with the same
// message ID. A consumer deduplicating on the ID sees it once.
func TestOutboxCrashWindowRedelivers(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	opts := relayOptions()
	opts.Lease = 1500 * time.Millisecond
	startRelay(t, &crashAfterConfirm{OutboxRepository: a.store.Outbox()}, amqptest.URL(t), topo, opts)

	created := a.create("acme", "Acme")
	start := time.Now()
	first := decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, 1, deliveryTimeout))
	if published, _, _ := a.outboxState(created.Task.ID); published {
		t.Fatal("event marked published although the mark was lost")
	}
	second := decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, 1, deliveryTimeout))
	if took := time.Since(start); took < opts.Lease-200*time.Millisecond {
		t.Errorf("redelivered after %v, before the %v lease expired", took, opts.Lease)
	}
	a.waitAllPublished()

	all := append(first, second...)
	if len(all) != 2 || all[0] != all[1] || all[0].MessageID != created.Task.ID {
		t.Fatalf("deliveries = %+v, want the same event twice", all)
	}
	if got := deliveries(all); len(got) != 1 {
		t.Errorf("distinct event IDs = %d, want 1 after deduplication", len(got))
	}
}

// TestOutboxAbandonedClaimIsRedelivered: a relay that claimed events and
// died (no publish, no hand-back) delays them by at most the lease.
func TestOutboxAbandonedClaimIsRedelivered(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	created := a.create("acme", "Acme")
	const lease = time.Second
	claimed, err := a.store.Outbox().ClaimPending(t.Context(), 10, lease) // the relay that dies
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d events, %v", len(claimed), err)
	}
	claimedAt := time.Now()

	startRelay(t, a.store.Outbox(), amqptest.URL(t), topo, relayOptions())
	events := decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, 1, deliveryTimeout))
	if len(events) != 1 || events[0].MessageID != created.Task.ID {
		t.Fatalf("events = %+v, want the abandoned task", events)
	}
	if took := time.Since(claimedAt); took < lease-100*time.Millisecond {
		t.Errorf("delivered %v after the claim, before its %v lease expired", took, lease)
	}
	a.waitAllPublished()
}

// TestOutboxConcurrentRelays: several relays (as with several control-
// plane replicas) share one outbox. Claims are disjoint, so with a healthy
// broker each event is delivered exactly once.
func TestOutboxConcurrentRelays(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	opts := relayOptions()
	opts.BatchSize = 2
	for range 3 {
		startRelay(t, a.store.Outbox(), amqptest.URL(t), topo, opts)
	}

	const tenants = 30
	taskIDs := make([]string, tenants)
	var wg sync.WaitGroup
	for i := range tenants {
		wg.Go(func() { taskIDs[i] = a.create(fmt.Sprintf("tenant-%02d", i), "T").Task.ID })
	}
	wg.Wait()
	a.waitAllPublished()

	got := deliveries(decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, tenants, deliveryTimeout)))
	for _, id := range taskIDs {
		if got[id] != 1 {
			t.Errorf("task %s delivered %d times, want 1", id, got[id])
		}
	}
}

// TestOutboxRelayRestart: events created while no relay runs stay in the
// outbox, nothing is lost on shutdown, and the next relay delivers them.
func TestOutboxRelayRestart(t *testing.T) {
	a := newAPI(t)
	topo := workerTopology(t)
	stop := startRelay(t, a.store.Outbox(), amqptest.URL(t), topo, relayOptions())
	before := a.create("tenant-a", "A")
	amqptest.WaitForMessages(t, topo.Queue, 1, deliveryTimeout)

	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run after shutdown = %v, want context.Canceled", err)
	}
	during := a.create("tenant-b", "B")
	time.Sleep(200 * time.Millisecond)
	if published, _, _ := a.outboxState(during.Task.ID); published {
		t.Fatal("event published with no relay running")
	}

	startRelay(t, a.store.Outbox(), amqptest.URL(t), topo, relayOptions())
	events := decodeEvents(t, amqptest.WaitForMessages(t, topo.Queue, 1, deliveryTimeout))
	if len(events) != 1 || events[0].MessageID != during.Task.ID {
		t.Errorf("after restart delivered %+v, want only %s (not %s again)", events, during.Task.ID, before.Task.ID)
	}
	a.waitAllPublished()
}
