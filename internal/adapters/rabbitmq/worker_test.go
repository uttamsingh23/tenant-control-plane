package rabbitmq_test

// Integration tests: the worker's TaskConsumer and UpdatePublisher against
// a real RabbitMQ (see amqptest). They check the acknowledgement contract:
// a task leaves the queue only once its updates are published.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/tcpproxy"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

const workerTimeout = 10 * time.Second

var fastRetry = rabbitmq.TaskConsumerOptions{
	ShutdownTimeout: 5 * time.Second,
	RetryMin:        20 * time.Millisecond,
	RetryMax:        200 * time.Millisecond,
	RequeueDelay:    10 * time.Millisecond,
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordingHandler records the tasks it is given; fn, if set, decides the
// result of each call (n counts from 1).
type recordingHandler struct {
	mu    sync.Mutex
	tasks []domain.Task
	fn    func(ctx context.Context, n int, task domain.Task) error
}

func (h *recordingHandler) Process(ctx context.Context, task domain.Task) error {
	h.mu.Lock()
	h.tasks = append(h.tasks, task)
	n := len(h.tasks)
	h.mu.Unlock()
	if h.fn != nil {
		return h.fn(ctx, n, task)
	}
	return nil
}

func (h *recordingHandler) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.tasks)
}

// runConsumer starts a consumer and returns a function that stops it and
// returns Run's error.
func runConsumer(t *testing.T, url string, topo rabbitmq.Topology, h rabbitmq.TaskHandler, opts rabbitmq.TaskConsumerOptions) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := rabbitmq.NewTaskConsumer(url, topo, h, opts, discard())
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
				t.Fatal("consumer did not stop")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(workerTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// depth returns the number of ready messages in queue. Unacked messages
// are not counted; they become ready again when their consumer goes away.
func depth(t *testing.T, queue string) int {
	t.Helper()
	q, err := amqptest.Channel(t).QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspect %s: %v", queue, err)
	}
	return q.Messages
}

func publishRaw(t *testing.T, topo rabbitmq.Topology, messageID string, body []byte) {
	t.Helper()
	err := amqptest.Channel(t).PublishWithContext(t.Context(), topo.Exchange, "task.deploy", true, false,
		amqp.Publishing{MessageId: messageID, ContentType: "application/json", Body: body})
	if err != nil {
		t.Fatal(err)
	}
}

func newSimulator(t *testing.T, failRate float64, pub worker.UpdatePublisher) *worker.Simulator {
	t.Helper()
	sim, err := worker.NewSimulator(config.Simulation{FailRate: failRate}, pub, rand.New(rand.NewPCG(1, 1)), discard())
	if err != nil {
		t.Fatal(err)
	}
	return sim
}

func newUpdatePublisher(t *testing.T, topo rabbitmq.Topology) *rabbitmq.UpdatePublisher {
	t.Helper()
	p := rabbitmq.NewUpdatePublisher(amqptest.URL(t), topo, discard())
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func decodeUpdate(t *testing.T, d amqp.Delivery) rabbitmq.TaskUpdateMessage {
	t.Helper()
	var m rabbitmq.TaskUpdateMessage
	if err := json.Unmarshal(d.Body, &m); err != nil {
		t.Fatalf("decode update %s: %v", d.Body, err)
	}
	return m
}

// TestWorkerReportsProgress: a task event is consumed, in_progress and
// then the outcome are published with the documented properties, and the
// task is acknowledged. --fail-rate=1 turns every outcome into failed.
func TestWorkerReportsProgress(t *testing.T) {
	for _, tt := range []struct {
		failRate float64
		want     domain.TaskStatus
	}{{0, domain.TaskStatusDone}, {1, domain.TaskStatusFailed}} {
		t.Run(string(tt.want), func(t *testing.T) {
			topo := amqptest.Topology(t)
			sim := newSimulator(t, tt.failRate, newUpdatePublisher(t, topo))
			stop := runConsumer(t, amqptest.URL(t), topo, sim, fastRetry)
			task := newTask(t, domain.TaskTypeDeploy)
			if err := publish(t, newPublisher(t, amqptest.URL(t), topo), task); err != nil {
				t.Fatal(err)
			}

			got := amqptest.WaitForMessages(t, topo.UpdateQueue, 2, workerTimeout)
			if len(got) != 2 {
				t.Fatalf("received %d updates, want 2", len(got))
			}
			for i, want := range []struct {
				id     string
				status domain.TaskStatus
			}{
				{worker.ProgressUpdateID(task.ID), domain.TaskStatusInProgress},
				{worker.OutcomeUpdateID(task.ID), tt.want},
			} {
				d, m := got[i], decodeUpdate(t, got[i])
				if m.MessageID != want.id || m.Status != string(want.status) || m.TaskID != task.ID || m.TenantID != task.TenantID {
					t.Errorf("update %d = %+v, want id %s, status %s for task %s", i, m, want.id, want.status, task.ID)
				}
				if d.MessageId != m.MessageID || d.CorrelationId != task.ID || d.Type != rabbitmq.TaskUpdateEventType ||
					d.ContentType != "application/json" || d.DeliveryMode != amqp.Persistent ||
					d.RoutingKey != rabbitmq.UpdateRoutingKey(want.status) {
					t.Errorf("update %d properties: message_id %q, correlation_id %q, type %q, content_type %q, delivery_mode %d, routing_key %q",
						i, d.MessageId, d.CorrelationId, d.Type, d.ContentType, d.DeliveryMode, d.RoutingKey)
				}
			}
			if m := decodeUpdate(t, got[1]); (tt.want == domain.TaskStatusFailed) != (m.Reason != "") {
				t.Errorf("outcome %s has reason %q", m.Status, m.Reason)
			}

			if err := stop(); !errors.Is(err, context.Canceled) {
				t.Errorf("Run = %v, want context.Canceled", err)
			}
			// Stopping the consumer would have requeued an unacked task.
			if n := depth(t, topo.Queue); n != 0 {
				t.Errorf("task queue holds %d messages after processing; the task was not acked", n)
			}
		})
	}
}

// TestWorkerDeadLettersMalformedMessages: a message that cannot be decoded
// goes to the dead-letter queue, and the consumer goes on with the next.
func TestWorkerDeadLettersMalformedMessages(t *testing.T) {
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	h := &recordingHandler{}
	runConsumer(t, amqptest.URL(t), topo, h, fastRetry)

	publishRaw(t, topo, "garbage-1", []byte("not json"))
	publishRaw(t, topo, "", []byte(`{"id":"t-2","type":"resize","tenant_id":"tn","status":"accepted"}`))
	task := newTask(t, domain.TaskTypeDeploy)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), task); err != nil {
		t.Fatal(err)
	}

	eventually(t, "the valid task to be processed", func() bool { return h.calls() >= 1 })
	dead := amqptest.WaitForMessages(t, topo.DeadLetterQueue, 2, workerTimeout)
	if len(dead) != 2 || string(dead[0].Body) != "not json" {
		t.Fatalf("dead-letter queue holds %d messages, want both malformed ones", len(dead))
	}
	// Rejected on first sight, not requeued until the queue's delivery
	// limit dead-lettered it ("delivery_limit").
	for _, d := range dead {
		if reason, queue := d.Headers["x-first-death-reason"], d.Headers["x-first-death-queue"]; reason != "rejected" || queue != topo.Queue {
			t.Errorf("message %q dead-lettered from %v for %v, want rejected from %s", d.Body, queue, reason, topo.Queue)
		}
	}
	if h.calls() != 1 || h.tasks[0].ID != task.ID {
		t.Errorf("handler got %+v, want only task %s", h.tasks, task.ID)
	}
}

// TestWorkerRequeuesFailedTask: a handler error requeues the task, which
// is processed again and then acknowledged.
func TestWorkerRequeuesFailedTask(t *testing.T) {
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	h := &recordingHandler{fn: func(_ context.Context, n int, _ domain.Task) error {
		if n == 1 {
			return errors.New("publish failed")
		}
		return nil
	}}
	stop := runConsumer(t, amqptest.URL(t), topo, h, fastRetry)
	task := newTask(t, domain.TaskTypeUpdate)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), task); err != nil {
		t.Fatal(err)
	}

	eventually(t, "the task to be retried", func() bool { return h.calls() >= 2 })
	_ = stop()
	if h.tasks[0] != h.tasks[1] {
		t.Errorf("retry got %+v, want the same task %+v", h.tasks[1], h.tasks[0])
	}
	if n := depth(t, topo.Queue); n != 0 {
		t.Errorf("task queue holds %d messages after the retry succeeded", n)
	}
}

// TestWorkerShutdownFinishesInFlightTask: on shutdown, the task being
// processed keeps a live context and is acked once it completes.
func TestWorkerShutdownFinishesInFlightTask(t *testing.T) {
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	started, release := make(chan struct{}), make(chan struct{})
	var cancelledEarly bool
	h := &recordingHandler{fn: func(ctx context.Context, _ int, _ domain.Task) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			cancelledEarly = true
			return ctx.Err()
		}
		return nil
	}}
	stop := runConsumer(t, amqptest.URL(t), topo, h, fastRetry)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	time.Sleep(100 * time.Millisecond) // shutdown has begun
	close(release)
	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
	if cancelledEarly {
		t.Fatal("the in-flight task's context was cancelled before the shutdown timeout")
	}
	if n := depth(t, topo.Queue); n != 0 {
		t.Errorf("task queue holds %d messages; the finished task was not acked", n)
	}
}

// TestWorkerShutdownTimeoutRequeuesTask: a task still running when the
// shutdown timeout expires is cancelled and goes back to the queue.
func TestWorkerShutdownTimeoutRequeuesTask(t *testing.T) {
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	started := make(chan struct{})
	h := &recordingHandler{fn: func(ctx context.Context, _ int, _ domain.Task) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	opts := fastRetry
	opts.ShutdownTimeout = 50 * time.Millisecond
	stop := runConsumer(t, amqptest.URL(t), topo, h, opts)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}
	<-started

	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
	eventually(t, "the abandoned task to be requeued", func() bool { return depth(t, topo.Queue) == 1 })
}

// TestWorkerBrokerUnavailableAtStartup: the consumer keeps retrying until
// the broker is reachable, then processes the waiting task.
func TestWorkerBrokerUnavailableAtStartup(t *testing.T) {
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), tcpproxy.Refuse)
	h := &recordingHandler{}
	runConsumer(t, amqptest.ProxiedURL(t, proxy.Addr()), topo, h, fastRetry)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond) // several failed connection attempts
	if h.calls() != 0 {
		t.Fatal("task processed with the broker unreachable")
	}
	proxy.SetMode(tcpproxy.Forward)
	eventually(t, "the task to be processed after the broker came up", func() bool { return h.calls() == 1 })
}

// TestWorkerRedeliveryRepeatsUpdateIDs: the worker "crashes" after
// publishing both updates but before acking (its connection is cut). The
// broker redelivers the task and the rerun publishes updates with the same
// IDs, which is what lets the control plane discard them.
func TestWorkerRedeliveryRepeatsUpdateIDs(t *testing.T) {
	topo := amqptest.Topology(t)
	amqptest.Declare(t, topo)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), tcpproxy.Forward)
	sim := newSimulator(t, 0, newUpdatePublisher(t, topo))
	h := &recordingHandler{fn: func(ctx context.Context, n int, task domain.Task) error {
		if err := sim.Process(ctx, task); err != nil {
			return err
		}
		if n == 1 {
			proxy.Cut() // the ack that follows is lost
		}
		return nil
	}}
	stop := runConsumer(t, amqptest.ProxiedURL(t, proxy.Addr()), topo, h, fastRetry)
	task := newTask(t, domain.TaskTypeDestroy)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), task); err != nil {
		t.Fatal(err)
	}

	got := amqptest.WaitForMessages(t, topo.UpdateQueue, 4, workerTimeout)
	_ = stop()
	if len(got) != 4 {
		t.Fatalf("received %d updates, want 4 (two runs of two)", len(got))
	}
	ids := make([]string, len(got))
	for i, d := range got {
		ids[i] = d.MessageId
	}
	want := []string{
		worker.ProgressUpdateID(task.ID), worker.OutcomeUpdateID(task.ID),
		worker.ProgressUpdateID(task.ID), worker.OutcomeUpdateID(task.ID),
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("update IDs = %v, want %v", ids, want)
		}
	}
	if h.calls() != 2 {
		t.Errorf("handler ran %d times, want 2", h.calls())
	}
	if n := depth(t, topo.Queue); n != 0 {
		t.Errorf("task queue holds %d messages; the rerun was not acked", n)
	}
}
