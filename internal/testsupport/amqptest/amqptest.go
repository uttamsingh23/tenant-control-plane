// Package amqptest provides a RabbitMQ broker for integration tests.
//
// The broker comes from one of two places:
//
//  1. TEST_RABBITMQ_URL, if set: an existing broker (for example the one
//     started by `make up`).
//  2. Otherwise a disposable rabbitmq container (the image docker compose
//     uses) started with testcontainers-go on first use and removed by
//     Teardown.
//
// Tests skip under -short, and when neither source is available.
//
// Every test gets its own exchange and queue names (Topology), so tests
// sharing the broker never see each other's messages.
package amqptest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
)

const rabbitmqImage = "rabbitmq:4.1-management-alpine"

var harness struct {
	once      sync.Once
	err       error
	url       string
	container *tcrabbitmq.RabbitMQContainer
	counter   atomic.Int64
}

// Main runs the tests of a package and removes the broker it started.
func Main(m *testing.M) {
	code := m.Run()
	Teardown()
	os.Exit(code)
}

// Teardown removes the broker container, if one was started.
func Teardown() {
	if harness.container != nil {
		_ = testcontainers.TerminateContainer(harness.container)
	}
}

// URL returns the broker's AMQP URL, starting the broker on first use, or
// skips the test when no broker is available.
func URL(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	external := os.Getenv("TEST_RABBITMQ_URL")
	if external == "" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
	}
	harness.once.Do(func() { harness.err = setup(external) })
	if harness.err != nil {
		t.Fatalf("set up test broker: %v", harness.err)
	}
	return harness.url
}

func setup(external string) error {
	if external != "" {
		harness.url = external
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := tcrabbitmq.Run(ctx, rabbitmqImage)
	harness.container = c // set even on error so Teardown removes it
	if err != nil {
		return fmt.Errorf("start %s container: %w", rabbitmqImage, err)
	}
	harness.url, err = c.AmqpURL(ctx)
	return err
}

// Topology returns exchange and queue names unique to the test, for every
// object of the topology. They are deleted when the test ends.
func Topology(t *testing.T) rabbitmq.Topology {
	t.Helper()
	brokerURL := URL(t)
	n := harness.counter.Add(1)
	prefix := fmt.Sprintf("test.%d.%d.", os.Getpid(), n)
	topo := rabbitmq.Topology{
		Exchange:           prefix + "tasks",
		Queue:              prefix + "tasks.worker",
		UpdateExchange:     prefix + "task-updates",
		UpdateQueue:        prefix + "task-updates.control-plane",
		DeadLetterExchange: prefix + "dead-letter",
		DeadLetterQueue:    prefix + "dead-letter",
	}
	t.Cleanup(func() {
		conn, err := amqp.Dial(brokerURL)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if ch, err := conn.Channel(); err == nil {
			for _, q := range []string{topo.Queue, topo.UpdateQueue, topo.DeadLetterQueue} {
				_, _ = ch.QueueDelete(q, false, false, false)
			}
			for _, x := range []string{topo.Exchange, topo.UpdateExchange, topo.DeadLetterExchange} {
				_ = ch.ExchangeDelete(x, false, false)
			}
		}
	})
	return topo
}

// Channel opens an administrative channel, closed when the test ends.
func Channel(t *testing.T) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(URL(t))
	if err != nil {
		t.Fatalf("connect to test broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// Declare declares topo, as a consumer starting before any publisher
// would.
func Declare(t *testing.T, topo rabbitmq.Topology) {
	t.Helper()
	if err := topo.Declare(Channel(t)); err != nil {
		t.Fatal(err)
	}
}

// Drain removes and returns every message currently in queue.
func Drain(t *testing.T, queue string) []amqp.Delivery {
	t.Helper()
	return drain(t, Channel(t), queue)
}

func drain(t *testing.T, ch *amqp.Channel, queue string) []amqp.Delivery {
	t.Helper()
	var out []amqp.Delivery
	for {
		d, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get from %s: %v", queue, err)
		}
		if !ok {
			return out
		}
		out = append(out, d)
	}
}

// WaitForMessages drains queue until it has received at least n messages
// or timeout passes, and returns what it received.
func WaitForMessages(t *testing.T, queue string, n int, timeout time.Duration) []amqp.Delivery {
	t.Helper()
	ch := Channel(t)
	var out []amqp.Delivery
	deadline := time.Now().Add(timeout)
	for len(out) < n && time.Now().Before(deadline) {
		out = append(out, drain(t, ch, queue)...)
		if len(out) < n {
			time.Sleep(20 * time.Millisecond)
		}
	}
	return out
}

// Addr returns the broker's host:port, the target for a tcpproxy.
func Addr(t *testing.T) string {
	t.Helper()
	uri, err := amqp.ParseURI(URL(t))
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
}

// ProxiedURL returns the broker URL with its address replaced by
// hostport, for pointing a client at a tcpproxy in front of the broker.
func ProxiedURL(t *testing.T, hostport string) string {
	t.Helper()
	u, err := url.Parse(URL(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Host = hostport
	return u.String()
}

// Ctl runs rabbitmqctl in the broker container. It skips the test when
// the broker is external, since there is no container to exec into.
func Ctl(t *testing.T, args ...string) string {
	t.Helper()
	URL(t)
	if harness.container == nil {
		t.Skip("needs the test broker container (TEST_RABBITMQ_URL is set)")
	}
	// Not t.Context(): Ctl is also used from cleanups, when it is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	code, out, err := harness.container.Exec(ctx, append([]string{"rabbitmqctl"}, args...))
	if err != nil {
		t.Fatalf("rabbitmqctl %s: %v", strings.Join(args, " "), err)
	}
	b, _ := io.ReadAll(out)
	if code != 0 {
		t.Fatalf("rabbitmqctl %s exited %d: %s", strings.Join(args, " "), code, b)
	}
	return string(b)
}
