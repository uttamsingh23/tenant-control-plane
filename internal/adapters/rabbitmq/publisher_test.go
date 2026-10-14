package rabbitmq_test

// Integration tests: TaskPublisher against a real RabbitMQ (see amqptest),
// with a tcpproxy in between where the network has to fail.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/tcpproxy"
)

func TestMain(m *testing.M) { amqptest.Main(m) }

var created = time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)

func newTask(t *testing.T, typ domain.TaskType) domain.Task {
	t.Helper()
	task, err := domain.NewTask(uuid.NewString(), typ, uuid.NewString(), created)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func newPublisher(t *testing.T, url string, topo rabbitmq.Topology) *rabbitmq.TaskPublisher {
	t.Helper()
	p := rabbitmq.NewTaskPublisher(url, topo, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func publish(t *testing.T, p *rabbitmq.TaskPublisher, task domain.Task) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return p.PublishTask(ctx, task)
}

// proxied returns a publisher that reaches the broker through a proxy.
func proxied(t *testing.T, mode tcpproxy.Mode) (*rabbitmq.TaskPublisher, *tcpproxy.Proxy, rabbitmq.Topology) {
	t.Helper()
	topo := amqptest.Topology(t)
	proxy := tcpproxy.Start(t, amqptest.Addr(t), mode)
	return newPublisher(t, amqptest.ProxiedURL(t, proxy.Addr()), topo), proxy, topo
}

// The wire shape is written out independently of rabbitmq.TaskMessage so
// a change to the contract breaks this test.
type wireTask struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func TestPublishTaskMessage(t *testing.T) {
	topo := amqptest.Topology(t)
	p := newPublisher(t, amqptest.URL(t), topo)
	task := newTask(t, domain.TaskTypeDeploy)

	if err := publish(t, p, task); err != nil {
		t.Fatalf("PublishTask: %v", err)
	}
	msgs := amqptest.Drain(t, topo.Queue)
	if len(msgs) != 1 {
		t.Fatalf("queue holds %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.MessageId != task.ID || m.DeliveryMode != amqp.Persistent || m.ContentType != "application/json" ||
		m.Type != rabbitmq.TaskEventType || m.Exchange != topo.Exchange || m.RoutingKey != "task.deploy" {
		t.Errorf("properties: message_id=%q delivery_mode=%d content_type=%q type=%q exchange=%q routing_key=%q",
			m.MessageId, m.DeliveryMode, m.ContentType, m.Type, m.Exchange, m.RoutingKey)
	}
	var body wireTask
	if err := json.Unmarshal(m.Body, &body); err != nil {
		t.Fatalf("body %s: %v", m.Body, err)
	}
	want := wireTask{
		ID: task.ID, Type: "deploy", TenantID: task.TenantID, Status: "accepted",
		CreatedAt: "2026-01-02T03:04:05.006Z", UpdatedAt: "2026-01-02T03:04:05.006Z",
	}
	if body != want {
		t.Errorf("body = %+v, want %+v", body, want)
	}
}

// TestTopologyIsDurable: redeclaring the exchange as transient, or the
// queue as a classic queue, is refused because they exist as declared.
func TestTopologyIsDurable(t *testing.T) {
	topo := amqptest.Topology(t)
	if err := publish(t, newPublisher(t, amqptest.URL(t), topo), newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}

	err := amqptest.Channel(t).ExchangeDeclare(topo.Exchange, amqp.ExchangeTopic, false, false, false, false, nil)
	assertPreconditionFailed(t, "transient exchange", err)
	_, err = amqptest.Channel(t).QueueDeclare(topo.Queue, true, false, false, false,
		amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeClassic})
	assertPreconditionFailed(t, "classic queue", err)
	q, err := amqptest.Channel(t).QueueDeclarePassive(topo.Queue, true, false, false, false, nil)
	if err != nil || q.Messages != 1 {
		t.Errorf("queue %s: %d messages, %v; want the published message kept", topo.Queue, q.Messages, err)
	}
}

func assertPreconditionFailed(t *testing.T, what string, err error) {
	t.Helper()
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.PreconditionFailed {
		t.Errorf("redeclaring as %s: err = %v, want PRECONDITION_FAILED", what, err)
	}
}

// TestPublishTaskBrokerUnavailableAtStartup: with the broker unreachable,
// publishing fails (it is the relay's job to retry); once the broker is
// reachable the same publisher connects and publishes.
func TestPublishTaskBrokerUnavailableAtStartup(t *testing.T) {
	p, proxy, topo := proxied(t, tcpproxy.Refuse)
	task := newTask(t, domain.TaskTypeDeploy)

	if err := publish(t, p, task); err == nil {
		t.Fatal("PublishTask succeeded with the broker unreachable")
	}
	proxy.SetMode(tcpproxy.Forward)
	if err := publish(t, p, task); err != nil {
		t.Fatalf("PublishTask after the broker came up: %v", err)
	}
	if msgs := amqptest.Drain(t, topo.Queue); len(msgs) != 1 || msgs[0].MessageId != task.ID {
		t.Errorf("queue holds %d messages, want the task", len(msgs))
	}
}

// TestPublishTaskReconnects: after the connection is cut, the publisher
// opens a new one; at most the publish that discovers the cut fails.
func TestPublishTaskReconnects(t *testing.T) {
	p, proxy, topo := proxied(t, tcpproxy.Forward)
	if err := publish(t, p, newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}
	proxy.Cut()

	task := newTask(t, domain.TaskTypeUpdate)
	var err error
	for range 2 {
		if err = publish(t, p, task); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("PublishTask after reconnect: %v", err)
	}
	if msgs := amqptest.Drain(t, topo.Queue); len(msgs) < 2 {
		t.Errorf("queue holds %d messages, want both tasks", len(msgs))
	}
}

// TestPublishTaskUnresponsiveBroker: a broker that accepts the connection
// but never answers cannot hold a publish past its context.
func TestPublishTaskUnresponsiveBroker(t *testing.T) {
	p, _, _ := proxied(t, tcpproxy.Blackhole)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := p.PublishTask(ctx, newTask(t, domain.TaskTypeDeploy)); err == nil {
		t.Fatal("PublishTask succeeded against an unresponsive broker")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("PublishTask took %v, want it bounded by the 300ms context", took)
	}
}

// TestPublishTaskNacked: a broker nack is an error. The queue is limited
// to one message and set to reject publishes beyond that. Quorum queues
// enforce the limit slightly late, so a publish or two past it may be
// accepted before the nacks start.
func TestPublishTaskNacked(t *testing.T) {
	topo := amqptest.Topology(t)
	policy := "limit-" + topo.Queue
	amqptest.Ctl(t, "set_policy", "--apply-to", "queues", policy, "^"+regexp.QuoteMeta(topo.Queue)+"$",
		`{"max-length":1,"overflow":"reject-publish"}`)
	t.Cleanup(func() { amqptest.Ctl(t, "clear_policy", policy) })
	p := newPublisher(t, amqptest.URL(t), topo)

	var err error
	for range 20 {
		if err = publish(t, p, newTask(t, domain.TaskTypeDeploy)); err != nil {
			break
		}
	}
	if err == nil || !strings.Contains(err.Error(), "nack") {
		t.Fatalf("PublishTask into a full queue: err = %v, want a nack", err)
	}
	// The channel stays usable: once there is room again, publishes succeed.
	amqptest.Drain(t, topo.Queue)
	deadline := time.Now().Add(5 * time.Second)
	for err = publish(t, p, newTask(t, domain.TaskTypeDeploy)); err != nil && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
		err = publish(t, p, newTask(t, domain.TaskTypeDeploy))
	}
	if err != nil {
		t.Errorf("PublishTask after draining: %v", err)
	}
}

// TestPublishTaskUnroutable: a message no queue receives is returned by
// the broker and reported, not silently dropped. The next publish
// redeclares the topology and succeeds.
func TestPublishTaskUnroutable(t *testing.T) {
	topo := amqptest.Topology(t)
	p := newPublisher(t, amqptest.URL(t), topo)
	if err := publish(t, p, newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}
	if _, err := amqptest.Channel(t).QueueDelete(topo.Queue, false, false, false); err != nil {
		t.Fatal(err)
	}

	task := newTask(t, domain.TaskTypeDeploy)
	if err := publish(t, p, task); err == nil || !strings.Contains(err.Error(), "unroutable") {
		t.Fatalf("PublishTask with no queue bound: err = %v, want unroutable", err)
	}
	if err := publish(t, p, task); err != nil {
		t.Fatalf("PublishTask retry: %v", err)
	}
	if msgs := amqptest.Drain(t, topo.Queue); len(msgs) != 1 || msgs[0].MessageId != task.ID {
		t.Errorf("queue holds %d messages, want the retried task", len(msgs))
	}
}

func TestPublisherClose(t *testing.T) {
	p := newPublisher(t, amqptest.URL(t), amqptest.Topology(t))
	if err := publish(t, p, newTask(t, domain.TaskTypeDeploy)); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := publish(t, p, newTask(t, domain.TaskTypeDeploy)); !errors.Is(err, rabbitmq.ErrPublisherClosed) {
		t.Errorf("PublishTask after Close: err = %v, want ErrPublisherClosed", err)
	}
}
