package rabbitmq_test

// Integration tests: the control plane's UpdateConsumer against a real
// RabbitMQ (see amqptest), with a scripted UpdateApplier. They check how
// each kind of result settles a delivery. The same consumer against
// PostgreSQL is tested end to end in tests/consumer_test.go.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/tcpproxy"
)

var fastConsumer = rabbitmq.UpdateConsumerOptions{
	Concurrency:     2,
	MaxAttempts:     5,
	RetryMin:        5 * time.Millisecond,
	RetryMax:        20 * time.Millisecond,
	ApplyTimeout:    5 * time.Second,
	ShutdownTimeout: 5 * time.Second,
}

var errTransient = errors.New("connection reset by peer")

// scriptedApplier records the updates it is given; fn, if set, decides
// the result of each call (n counts from 1). Without fn every update is
// applied.
type scriptedApplier struct {
	mu      sync.Mutex
	updates []domain.TaskUpdate
	fn      func(ctx context.Context, n int, u domain.TaskUpdate) (application.UpdateOutcome, error)
}

func (a *scriptedApplier) ApplyUpdate(ctx context.Context, u domain.TaskUpdate) (application.UpdateOutcome, error) {
	a.mu.Lock()
	a.updates = append(a.updates, u)
	n := len(a.updates)
	a.mu.Unlock()
	if a.fn != nil {
		return a.fn(ctx, n, u)
	}
	return application.UpdateApplied, nil
}

func (a *scriptedApplier) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.updates)
}

func (a *scriptedApplier) ids() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.updates))
	for i, u := range a.updates {
		out[i] = u.ID
	}
	return out
}

// runUpdateConsumer starts a consumer and returns a function that stops
// it and returns Run's error.
func runUpdateConsumer(t *testing.T, url string, topo rabbitmq.Topology, a rabbitmq.UpdateApplier, opts rabbitmq.UpdateConsumerOptions) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := rabbitmq.NewUpdateConsumer(url, topo, a, opts, discard())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(workerTimeout):
				t.Fatal("update consumer did not stop")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// progressUpdate is a valid in_progress update for task n.
func progressUpdate(n int) domain.TaskUpdate {
	id := fmt.Sprintf("task-%d", n)
	return domain.TaskUpdate{
		ID: id + ":in_progress", TaskID: id, TenantID: "tenant-" + id, TaskType: domain.TaskTypeDeploy,
		PreviousStatus: domain.TaskStatusAccepted, Status: domain.TaskStatusInProgress, OccurredAt: created,
	}
}

// publishUpdate publishes u as the worker's UpdatePublisher would.
func publishUpdate(t *testing.T, topo rabbitmq.Topology, u domain.TaskUpdate) {
	t.Helper()
	publishRawUpdate(t, topo, amqp.Publishing{
		MessageId: u.ID, CorrelationId: u.TaskID, ContentType: "application/json",
		DeliveryMode: amqp.Persistent, Body: marshal(t, rabbitmq.NewTaskUpdateMessage(u)),
	})
}

func publishRawUpdate(t *testing.T, topo rabbitmq.Topology, msg amqp.Publishing) {
	t.Helper()
	err := amqptest.Channel(t).PublishWithContext(t.Context(), topo.UpdateExchange, "task_update.in_progress", true, false, msg)
	if err != nil {
		t.Fatal(err)
	}
}

func updateTopology(t *testing.T) rabbitmq.Topology {
	t.Helper()
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	return topo
}

// assertDeadLettered checks that the dead-letter queue holds exactly the
// given message IDs, each dead-lettered from the update queue for reason.
func assertDeadLettered(t *testing.T, topo rabbitmq.Topology, reason string, ids ...string) {
	t.Helper()
	dead := amqptest.WaitForMessages(t, topo.DeadLetterQueue, len(ids), workerTimeout)
	if len(dead) != len(ids) {
		t.Fatalf("dead-letter queue holds %d messages, want %d (%v)", len(dead), len(ids), ids)
	}
	got := map[string]bool{}
	for _, d := range dead {
		got[d.MessageId] = true
		if r, q := d.Headers["x-first-death-reason"], d.Headers["x-first-death-queue"]; r != reason || q != topo.UpdateQueue {
			t.Errorf("message %q dead-lettered from %v for %v, want %s from %s", d.MessageId, q, r, reason, topo.UpdateQueue)
		}
	}
	for _, id := range ids {
		if !got[id] {
			t.Errorf("message %q not dead-lettered; got %v", id, got)
		}
	}
}

// TestConsumerAppliesAndAcks: a valid update is decoded, applied and
// acknowledged. Every outcome is acked: a duplicate, stale or conflicting
// update has been dealt with as surely as an applied one.
func TestConsumerAppliesAndAcks(t *testing.T) {
	topo := updateTopology(t)
	outcomes := []application.UpdateOutcome{
		application.UpdateApplied, application.UpdateDuplicate, application.UpdateStale, application.UpdateConflict,
	}
	a := &scriptedApplier{fn: func(_ context.Context, n int, _ domain.TaskUpdate) (application.UpdateOutcome, error) {
		return outcomes[(n-1)%len(outcomes)], nil
	}}
	stop := runUpdateConsumer(t, amqptest.URL(t), topo, a, fastConsumer)

	// Published by the worker's real UpdatePublisher.
	pub := newUpdatePublisher(t, topo)
	for i := range outcomes {
		if err := pub.PublishUpdate(t.Context(), progressUpdate(i)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "every update to be applied", func() bool { return a.calls() == len(outcomes) })
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	// Anything unacked would be back in the queue once the consumer is gone.
	if n := depth(t, topo.UpdateQueue); n != 0 {
		t.Errorf("%d updates left in the queue, want 0", n)
	}
	if n := depth(t, topo.DeadLetterQueue); n != 0 {
		t.Errorf("%d updates dead-lettered, want 0", n)
	}
	if got := a.updates[0]; got.ID != progressUpdate(0).ID || got.TenantID != progressUpdate(0).TenantID || !got.OccurredAt.Equal(created) {
		t.Errorf("applier got %+v, want %+v", got, progressUpdate(0))
	}
}

// TestConsumerDeadLettersMalformedUpdates: an update that cannot be
// decoded is rejected straight to the dead-letter queue, never applied
// or retried, and the consumer goes on with the next message.
func TestConsumerDeadLettersMalformedUpdates(t *testing.T) {
	topo := updateTopology(t)
	a := &scriptedApplier{}
	runUpdateConsumer(t, amqptest.URL(t), topo, a, fastConsumer)

	bad := progressUpdate(1)
	bad.PreviousStatus = domain.TaskStatusDone // not a transition
	publishRawUpdate(t, topo, amqp.Publishing{MessageId: "garbage", Body: []byte("not json")})
	publishRawUpdate(t, topo, amqp.Publishing{MessageId: "v2", Body: []byte(`{"schema_version":2}`)})
	publishRawUpdate(t, topo, amqp.Publishing{MessageId: bad.ID, Body: marshal(t, rabbitmq.NewTaskUpdateMessage(bad))})
	publishRawUpdate(t, topo, amqp.Publishing{MessageId: "mismatch", Body: marshal(t, rabbitmq.NewTaskUpdateMessage(progressUpdate(2)))})
	publishUpdate(t, topo, progressUpdate(3))

	eventually(t, "the valid update to be applied", func() bool { return a.calls() >= 1 })
	assertDeadLettered(t, topo, "rejected", "garbage", "v2", bad.ID, "mismatch")
	if ids := a.ids(); len(ids) != 1 || ids[0] != progressUpdate(3).ID {
		t.Errorf("applier got %v, want only %s", ids, progressUpdate(3).ID)
	}
}

// TestConsumerDeadLettersPermanentFailures: an update the application
// can never apply (here: task not found, the poison-message policy) is
// dead-lettered after one attempt, not retried.
func TestConsumerDeadLettersPermanentFailures(t *testing.T) {
	topo := updateTopology(t)
	permanent := map[string]error{
		progressUpdate(1).ID: fmt.Errorf("apply update: %w", domain.ErrTaskNotFound),
		progressUpdate(2).ID: fmt.Errorf("apply update: %w", &domain.ValidationError{Field: "tenant_id", Reason: "mismatch"}),
		progressUpdate(3).ID: fmt.Errorf("apply update: %w", &domain.TransitionError{Entity: "tenant", From: "active", Trigger: "deploy_done"}),
	}
	a := &scriptedApplier{fn: func(_ context.Context, _ int, u domain.TaskUpdate) (application.UpdateOutcome, error) {
		if err, ok := permanent[u.ID]; ok {
			return "", err
		}
		return application.UpdateApplied, nil
	}}
	runUpdateConsumer(t, amqptest.URL(t), topo, a, fastConsumer)

	for i := 1; i <= 4; i++ {
		publishUpdate(t, topo, progressUpdate(i))
	}
	assertDeadLettered(t, topo, "rejected", progressUpdate(1).ID, progressUpdate(2).ID, progressUpdate(3).ID)
	eventually(t, "the valid update to be applied", func() bool { return a.calls() == 4 })
	time.Sleep(100 * time.Millisecond) // a retry, if any, would show up now
	if n := a.calls(); n != 4 {
		t.Errorf("applier called %d times, want 4 (one attempt each)", n)
	}
}

// TestConsumerRetriesTransientFailures: a retryable failure is retried
// in-process with backoff, holding the delivery, until it succeeds.
func TestConsumerRetriesTransientFailures(t *testing.T) {
	topo := updateTopology(t)
	var mu sync.Mutex
	var at []time.Time
	a := &scriptedApplier{fn: func(_ context.Context, n int, _ domain.TaskUpdate) (application.UpdateOutcome, error) {
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		if n <= 3 {
			return "", fmt.Errorf("apply update: %w", errTransient)
		}
		return application.UpdateApplied, nil
	}}
	opts := fastConsumer
	opts.RetryMin, opts.RetryMax = 20*time.Millisecond, 40*time.Millisecond
	stop := runUpdateConsumer(t, amqptest.URL(t), topo, a, opts)

	publishUpdate(t, topo, progressUpdate(1))
	eventually(t, "the update to be applied", func() bool { return a.calls() == 4 })
	_ = stop()
	if n := depth(t, topo.UpdateQueue) + depth(t, topo.DeadLetterQueue); n != 0 {
		t.Errorf("%d messages left in the update and dead-letter queues, want 0", n)
	}
	// Backoff 20ms, 40ms, 40ms (capped): no hot loop.
	for i, want := range []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond} {
		if gap := at[i+1].Sub(at[i]); gap < want {
			t.Errorf("attempt %d came %v after the previous one, want at least %v", i+2, gap, want)
		}
	}
}

// TestConsumerRequeuesAfterMaxAttempts: once MaxAttempts are used up the
// delivery goes back to the queue rather than holding a slot forever, and
// the redelivery gets a fresh set of attempts.
func TestConsumerRequeuesAfterMaxAttempts(t *testing.T) {
	topo := updateTopology(t)
	a := &scriptedApplier{fn: func(_ context.Context, n int, _ domain.TaskUpdate) (application.UpdateOutcome, error) {
		if n <= 3 {
			return "", errTransient
		}
		return application.UpdateApplied, nil
	}}
	opts := fastConsumer
	opts.MaxAttempts = 2
	stop := runUpdateConsumer(t, amqptest.URL(t), topo, a, opts)

	publishUpdate(t, topo, progressUpdate(1))
	eventually(t, "the update to be applied", func() bool { return a.calls() == 4 })
	_ = stop()
	if n := depth(t, topo.UpdateQueue) + depth(t, topo.DeadLetterQueue); n != 0 {
		t.Errorf("%d messages left in the update and dead-letter queues, want 0", n)
	}
}

// TestConsumerFailingForeverIsBounded: an update that fails every attempt
// is not retried forever. The quorum queue's delivery limit dead-letters
// it, and the consumer goes on with the next message.
func TestConsumerFailingForeverIsBounded(t *testing.T) {
	topo := updateTopology(t)
	stuck := progressUpdate(1)
	a := &scriptedApplier{fn: func(_ context.Context, _ int, u domain.TaskUpdate) (application.UpdateOutcome, error) {
		if u.ID == stuck.ID {
			return "", errTransient
		}
		return application.UpdateApplied, nil
	}}
	opts := fastConsumer
	opts.MaxAttempts = 1
	runUpdateConsumer(t, amqptest.URL(t), topo, a, opts)

	publishUpdate(t, topo, stuck)
	publishUpdate(t, topo, progressUpdate(2))
	assertDeadLettered(t, topo, "delivery_limit", stuck.ID)
	tries := 0
	for _, id := range a.ids() {
		if id == stuck.ID {
			tries++
		}
	}
	// RabbitMQ 4's default delivery limit is 20 redeliveries.
	if tries < 20 || tries > 21 {
		t.Errorf("stuck update tried %d times, want 20-21 (the delivery limit)", tries)
	}
	eventually(t, "the next update to be applied", func() bool {
		for _, id := range a.ids() {
			if id == progressUpdate(2).ID {
				return true
			}
		}
		return false
	})
}

// TestConsumerShutdownFinishesInFlightUpdate: on shutdown the update being
// applied is allowed to finish and is acked before the connection closes.
func TestConsumerShutdownFinishesInFlightUpdate(t *testing.T) {
	topo := updateTopology(t)
	started, release := make(chan struct{}), make(chan struct{})
	var workErr error
	a := &scriptedApplier{fn: func(ctx context.Context, _ int, _ domain.TaskUpdate) (application.UpdateOutcome, error) {
		close(started)
		<-release
		workErr = ctx.Err()
		return application.UpdateApplied, nil
	}}
	stop := runUpdateConsumer(t, amqptest.URL(t), topo, a, fastConsumer)

	publishUpdate(t, topo, progressUpdate(1))
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-stopped:
		t.Fatalf("Run returned (%v) while an update was still being applied", err)
	default:
	}
	close(release)
	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
	if workErr != nil {
		t.Errorf("in-flight update's context was cancelled: %v", workErr)
	}
	if n := depth(t, topo.UpdateQueue); n != 0 {
		t.Errorf("%d updates back in the queue, want 0: the finished update must be acked", n)
	}
}

// TestConsumerShutdownTimeoutRequeues: an update still running after
// ShutdownTimeout is cancelled and goes back to the queue unacked.
func TestConsumerShutdownTimeoutRequeues(t *testing.T) {
	topo := updateTopology(t)
	started := make(chan struct{})
	a := &scriptedApplier{fn: func(ctx context.Context, _ int, _ domain.TaskUpdate) (application.UpdateOutcome, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	}}
	opts := fastConsumer
	opts.ShutdownTimeout = 100 * time.Millisecond
	stop := runUpdateConsumer(t, amqptest.URL(t), topo, a, opts)

	publishUpdate(t, topo, progressUpdate(1))
	<-started
	begin := time.Now()
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
	if took := time.Since(begin); took > 3*time.Second {
		t.Errorf("shutdown took %v, want about the 100ms timeout", took)
	}
	eventually(t, "the update to be requeued", func() bool { return depth(t, topo.UpdateQueue) == 1 })
	if n := depth(t, topo.DeadLetterQueue); n != 0 {
		t.Errorf("%d updates dead-lettered, want 0", n)
	}
}

// TestConsumerShutdownDuringBackoff: a shutdown cuts a retry backoff short
// and the update goes back to the queue.
func TestConsumerShutdownDuringBackoff(t *testing.T) {
	topo := updateTopology(t)
	a := &scriptedApplier{fn: func(context.Context, int, domain.TaskUpdate) (application.UpdateOutcome, error) {
		return "", errTransient
	}}
	opts := fastConsumer
	opts.RetryMin, opts.RetryMax = time.Hour, time.Hour
	stop := runUpdateConsumer(t, amqptest.URL(t), topo, a, opts)

	publishUpdate(t, topo, progressUpdate(1))
	eventually(t, "the first attempt", func() bool { return a.calls() == 1 })
	begin := time.Now()
	_ = stop()
	if took := time.Since(begin); took > 3*time.Second {
		t.Errorf("shutdown took %v during an hour-long backoff", took)
	}
	eventually(t, "the update to be requeued", func() bool { return depth(t, topo.UpdateQueue) == 1 })
}

// TestConsumerAppliesConcurrently: with Concurrency n, n updates are in
// flight at once.
func TestConsumerAppliesConcurrently(t *testing.T) {
	topo := updateTopology(t)
	const n = 4
	var inFlight sync.WaitGroup
	inFlight.Add(n)
	all := make(chan struct{})
	go func() { inFlight.Wait(); close(all) }()
	a := &scriptedApplier{fn: func(context.Context, int, domain.TaskUpdate) (application.UpdateOutcome, error) {
		inFlight.Done()
		select {
		case <-all:
			return application.UpdateApplied, nil
		case <-time.After(workerTimeout):
			return "", errors.New("updates were not applied concurrently")
		}
	}}
	opts := fastConsumer
	opts.Concurrency, opts.MaxAttempts = n, 1
	runUpdateConsumer(t, amqptest.URL(t), topo, a, opts)

	for i := range n {
		publishUpdate(t, topo, progressUpdate(i))
	}
	select {
	case <-all:
	case <-time.After(workerTimeout):
		t.Fatalf("only some of %d updates were in flight at once", n)
	}
}

// TestConsumerReconnects: a broker that is down at startup, and a
// connection lost later, only delay consumption.
func TestConsumerReconnects(t *testing.T) {
	topo := updateTopology(t)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), tcpproxy.Refuse)
	a := &scriptedApplier{}
	runUpdateConsumer(t, amqptest.ProxiedURL(t, proxy.Addr()), topo, a, fastConsumer)

	publishUpdate(t, topo, progressUpdate(1))
	time.Sleep(100 * time.Millisecond)
	if a.calls() != 0 {
		t.Fatal("consumed through a refusing proxy")
	}
	proxy.SetMode(tcpproxy.Forward)
	eventually(t, "the first update after the broker came up", func() bool { return a.calls() == 1 })

	proxy.Cut()
	publishUpdate(t, topo, progressUpdate(2))
	eventually(t, "the second update after reconnecting", func() bool { return a.calls() == 2 })
}
