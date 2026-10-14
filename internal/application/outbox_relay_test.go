package application

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// fakeRelayOutbox is an in-memory outbox with the lease semantics of the
// PostgreSQL implementation: claims move available_at, Reschedule checks
// the claim token, MarkPublished is idempotent.
type fakeRelayOutbox struct {
	mu     sync.Mutex
	rows   []*fakeOutboxRow
	tokens int
	// failClaims and failMarks make the next n calls fail.
	failClaims int
	failMarks  int
}

type fakeOutboxRow struct {
	task        domain.Task
	availableAt time.Time
	attempts    int
	token       string
	published   bool
	marks       int
	lastErr     string
}

func newFakeRelayOutbox(t *testing.T, n int) (*fakeRelayOutbox, []domain.Task) {
	t.Helper()
	o := &fakeRelayOutbox{}
	tasks := make([]domain.Task, n)
	for i := range n {
		task, err := domain.NewTask(fmt.Sprintf("task-%d", i), domain.TaskTypeDeploy, fmt.Sprintf("tenant-%d", i), t0)
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = task
		o.rows = append(o.rows, &fakeOutboxRow{task: task})
	}
	return o, tasks
}

func (o *fakeRelayOutbox) Enqueue(context.Context, domain.Task) error {
	return domain.ErrNotImplemented
}

func (o *fakeRelayOutbox) ClaimPending(_ context.Context, limit int, lease time.Duration) ([]OutboxEvent, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failClaims > 0 {
		o.failClaims--
		return nil, errInjected
	}
	now := time.Now()
	var events []OutboxEvent
	for _, row := range o.rows {
		if len(events) == limit {
			break
		}
		if row.published || row.availableAt.After(now) {
			continue
		}
		o.tokens++
		row.token = fmt.Sprintf("claim-%d", o.tokens)
		row.availableAt = now.Add(lease)
		events = append(events, OutboxEvent{Task: row.task, Attempts: row.attempts, ClaimToken: row.token})
	}
	return events, nil
}

func (o *fakeRelayOutbox) MarkPublished(_ context.Context, taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failMarks > 0 {
		o.failMarks--
		return errInjected
	}
	row := o.find(taskID)
	row.published = true
	row.marks++
	return nil
}

func (o *fakeRelayOutbox) Reschedule(_ context.Context, e OutboxEvent, delay time.Duration, failure error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	row := o.find(e.Task.ID)
	if row.published || row.token != e.ClaimToken {
		return nil
	}
	row.token = ""
	row.availableAt = time.Now().Add(delay)
	if failure != nil {
		row.attempts++
		row.lastErr = failure.Error()
	}
	return nil
}

func (o *fakeRelayOutbox) find(taskID string) *fakeOutboxRow {
	for _, row := range o.rows {
		if row.task.ID == taskID {
			return row
		}
	}
	panic("unknown task " + taskID)
}

// row returns a copy of the row for taskID.
func (o *fakeRelayOutbox) row(taskID string) fakeOutboxRow {
	o.mu.Lock()
	defer o.mu.Unlock()
	return *o.find(taskID)
}

func (o *fakeRelayOutbox) allPublished() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, row := range o.rows {
		if !row.published {
			return false
		}
	}
	return true
}

// fakePublisher records publishes. fail, if set, decides each outcome.
type fakePublisher struct {
	mu        sync.Mutex
	published []string
	fail      func(ctx context.Context, task domain.Task, call int) error
	calls     int
}

func (p *fakePublisher) PublishTask(ctx context.Context, task domain.Task) error {
	p.mu.Lock()
	p.calls++
	call, fail := p.calls, p.fail
	p.mu.Unlock()
	if fail != nil {
		if err := fail(ctx, task, call); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, task.ID)
	return nil
}

func (p *fakePublisher) publishedIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.published...)
}

var errBrokerDown = errors.New("broker down")

func fastOptions() OutboxRelayOptions {
	return OutboxRelayOptions{
		PollInterval:   5 * time.Millisecond,
		BatchSize:      10,
		Lease:          time.Second,
		PublishTimeout: 100 * time.Millisecond,
		RetryMin:       2 * time.Millisecond,
		RetryMax:       10 * time.Millisecond,
	}
}

func newTestRelay(t *testing.T, outbox OutboxRepository, pub TaskPublisher, opts OutboxRelayOptions) *OutboxRelay {
	t.Helper()
	r, err := NewOutboxRelay(outbox, pub, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r.jitter = func(d time.Duration) time.Duration { return d }
	return r
}

// runRelay runs r in the background and returns a function that stops it
// and returns Run's error.
func runRelay(t *testing.T, r *OutboxRelay) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not stop after cancellation")
			return nil
		}
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRelayPublishesInOrderAndMarks(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 3)
	pub := &fakePublisher{}
	r := newTestRelay(t, outbox, pub, fastOptions())

	n, err := r.RelayBatch(t.Context())
	if err != nil || n != 3 {
		t.Fatalf("RelayBatch = %d, %v; want 3, nil", n, err)
	}
	got := pub.publishedIDs()
	for i, task := range tasks {
		if got[i] != task.ID {
			t.Errorf("published[%d] = %s, want %s", i, got[i], task.ID)
		}
		if row := outbox.row(task.ID); !row.published || row.marks != 1 {
			t.Errorf("%s: published=%v marks=%d, want marked once", task.ID, row.published, row.marks)
		}
	}
	if n, err := r.RelayBatch(t.Context()); err != nil || n != 0 {
		t.Errorf("second RelayBatch = %d, %v; want nothing left to publish", n, err)
	}
}

// TestRelayFailureIsRecordedNotMarked: a failed publish is not marked; it
// counts as an attempt with a backoff, and the untried rest of the batch
// is handed back without an attempt being charged.
func TestRelayFailureIsRecordedNotMarked(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 3)
	pub := &fakePublisher{fail: func(_ context.Context, task domain.Task, _ int) error {
		if task.ID == tasks[1].ID {
			return errBrokerDown
		}
		return nil
	}}
	opts := fastOptions()
	opts.RetryMin, opts.RetryMax = time.Minute, time.Hour
	r := newTestRelay(t, outbox, pub, opts)

	before := time.Now()
	n, err := r.RelayBatch(t.Context())
	if n != 1 || !errors.Is(err, errBrokerDown) {
		t.Fatalf("RelayBatch = %d, %v; want 1, errBrokerDown", n, err)
	}
	if row := outbox.row(tasks[0].ID); !row.published {
		t.Error("first event not marked published")
	}
	failed := outbox.row(tasks[1].ID)
	if failed.published || failed.attempts != 1 || !strings.Contains(failed.lastErr, "broker down") || failed.token != "" {
		t.Errorf("failed event = %+v; want unpublished, 1 attempt, error recorded, claim ended", failed)
	}
	if retry := failed.availableAt.Sub(before); retry < time.Minute {
		t.Errorf("failed event retries after %v, want the backoff (1m)", retry)
	}
	untried := outbox.row(tasks[2].ID)
	if untried.published || untried.attempts != 0 || untried.token != "" || untried.availableAt.Before(before.Add(time.Minute)) {
		t.Errorf("untried event = %+v; want handed back with no attempt, after the same backoff", untried)
	}
	if got := pub.publishedIDs(); len(got) != 1 {
		t.Errorf("published %v, want only the first event", got)
	}
}

// TestRelayRetriesUntilPublished: transient failures never drop an event;
// it is retried with backoff until the broker accepts it.
func TestRelayRetriesUntilPublished(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 1)
	const failures = 8
	pub := &fakePublisher{fail: func(_ context.Context, _ domain.Task, call int) error {
		if call <= failures {
			return errBrokerDown
		}
		return nil
	}}
	stop := runRelay(t, newTestRelay(t, outbox, pub, fastOptions()))

	eventually(t, "event published", outbox.allPublished)
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
	row := outbox.row(tasks[0].ID)
	if row.attempts != failures || row.marks != 1 {
		t.Errorf("attempts = %d, marks = %d; want %d failed attempts, then one mark", row.attempts, row.marks, failures)
	}
}

// TestRelaySurvivesClaimFailures: a database outage backs the relay off;
// it does not stop.
func TestRelaySurvivesClaimFailures(t *testing.T) {
	outbox, _ := newFakeRelayOutbox(t, 2)
	outbox.failClaims = 5
	stop := runRelay(t, newTestRelay(t, outbox, &fakePublisher{}, fastOptions()))
	eventually(t, "events published", outbox.allPublished)
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
}

// TestRelayCrashWindowRepublishes covers the window between the broker's
// confirmation and the mark: the mark fails (as if the relay crashed), so
// the event stays claimed, is not retried before its lease expires, and
// is then published a second time with the same ID.
func TestRelayCrashWindowRepublishes(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 1)
	outbox.failMarks = 1
	pub := &fakePublisher{}
	opts := fastOptions()
	opts.Lease, opts.PublishTimeout = 150*time.Millisecond, 20*time.Millisecond
	r := newTestRelay(t, outbox, pub, opts)

	if _, err := r.RelayBatch(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("RelayBatch err = %v, want the mark failure", err)
	}
	if row := outbox.row(tasks[0].ID); row.published || row.attempts != 0 || row.token == "" {
		t.Fatalf("after failed mark row = %+v; want unpublished, no failed attempt, still claimed", row)
	}
	// Still leased: not claimable yet, so not published again yet.
	if n, err := r.RelayBatch(t.Context()); n != 0 || err != nil {
		t.Fatalf("RelayBatch during lease = %d, %v; want 0, nil", n, err)
	}

	stop := runRelay(t, r)
	eventually(t, "event marked published", outbox.allPublished)
	_ = stop()
	if got := pub.publishedIDs(); len(got) != 2 || got[0] != tasks[0].ID || got[1] != tasks[0].ID {
		t.Errorf("published %v, want the same event ID twice", got)
	}
}

// TestRelayRedeliversAbandonedClaim: events claimed by a relay that then
// died are published by another relay once the lease expires.
func TestRelayRedeliversAbandonedClaim(t *testing.T) {
	outbox, _ := newFakeRelayOutbox(t, 3)
	opts := fastOptions()
	opts.Lease, opts.PublishTimeout = 100*time.Millisecond, 20*time.Millisecond
	// The crashed relay: claims everything, publishes nothing.
	if events, err := outbox.ClaimPending(t.Context(), 10, opts.Lease); err != nil || len(events) != 3 {
		t.Fatalf("claim = %d events, %v", len(events), err)
	}
	claimedAt := time.Now()

	pub := &fakePublisher{}
	stop := runRelay(t, newTestRelay(t, outbox, pub, opts))
	eventually(t, "abandoned events published", outbox.allPublished)
	_ = stop()
	if waited := time.Since(claimedAt); waited < opts.Lease {
		t.Errorf("republished after %v, before the %v lease expired", waited, opts.Lease)
	}
	if got := pub.publishedIDs(); len(got) != 3 {
		t.Errorf("published %v, want each event once", got)
	}
}

// TestRelayShutdownMidPublish: cancellation interrupts a publish waiting
// for the broker. The event is handed back for immediate pickup, without
// a failed attempt being charged, and Run returns promptly.
func TestRelayShutdownMidPublish(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 2)
	inFlight := make(chan struct{})
	pub := &fakePublisher{fail: func(ctx context.Context, _ domain.Task, _ int) error {
		close(inFlight)
		<-ctx.Done() // a broker that never confirms
		return ctx.Err()
	}}
	opts := fastOptions()
	opts.PublishTimeout, opts.Lease = time.Minute, 3*time.Minute
	stop := runRelay(t, newTestRelay(t, outbox, pub, opts))

	<-inFlight
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
	for _, task := range tasks {
		row := outbox.row(task.ID)
		if row.published || row.attempts != 0 || row.token != "" || row.availableAt.After(time.Now()) {
			t.Errorf("%s = %+v; want handed back, claimable now, no attempt charged", task.ID, row)
		}
	}
}

// TestRelayMarksConfirmedEventDuringShutdown: a confirmation that arrives
// as shutdown begins is still recorded, avoiding a needless duplicate.
func TestRelayMarksConfirmedEventDuringShutdown(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	pub := &fakePublisher{fail: func(context.Context, domain.Task, int) error {
		cancel() // shutdown begins after the broker confirmed
		return nil
	}}
	r := newTestRelay(t, outbox, pub, fastOptions())
	if n, err := r.RelayBatch(ctx); n != 1 || err != nil {
		t.Fatalf("RelayBatch = %d, %v; want 1, nil", n, err)
	}
	if !outbox.row(tasks[0].ID).published {
		t.Error("confirmed event was not marked published")
	}
}

// TestRelayStopsBeforeLeaseRunsOut: when a slow broker eats into the
// lease, the rest of the batch is handed back rather than published under
// a lease that might expire mid-publish.
func TestRelayStopsBeforeLeaseRunsOut(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 3)
	pub := &fakePublisher{fail: func(context.Context, domain.Task, int) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}}
	opts := fastOptions()
	// Budget for starting a publish: 100ms - 2*30ms = 40ms after claiming.
	opts.Lease, opts.PublishTimeout = 100*time.Millisecond, 30*time.Millisecond
	r := newTestRelay(t, outbox, pub, opts)

	n, err := r.RelayBatch(t.Context())
	if n != 1 || err != nil {
		t.Fatalf("RelayBatch = %d, %v; want 1, nil", n, err)
	}
	for _, task := range tasks[1:] {
		if row := outbox.row(task.ID); row.published || row.token != "" || row.attempts != 0 {
			t.Errorf("%s = %+v; want handed back untried", task.ID, row)
		}
	}
}

// TestRelaysClaimDisjointEvents runs several relays on one outbox; each
// event is published exactly once because leases outlast the test.
func TestRelaysClaimDisjointEvents(t *testing.T) {
	outbox, tasks := newFakeRelayOutbox(t, 50)
	pub := &fakePublisher{}
	opts := fastOptions()
	opts.BatchSize = 3
	for range 4 {
		runRelay(t, newTestRelay(t, outbox, pub, opts))
	}
	eventually(t, "all events published", outbox.allPublished)
	seen := map[string]int{}
	for _, id := range pub.publishedIDs() {
		seen[id]++
	}
	for _, task := range tasks {
		if seen[task.ID] != 1 {
			t.Errorf("%s published %d times, want 1", task.ID, seen[task.ID])
		}
	}
}

func TestRelayBackoff(t *testing.T) {
	r := newTestRelay(t, &fakeRelayOutbox{}, &fakePublisher{}, OutboxRelayOptions{
		PollInterval: time.Second, BatchSize: 1, Lease: time.Minute, PublishTimeout: time.Second,
		RetryMin: 100 * time.Millisecond, RetryMax: time.Second,
	})
	want := []time.Duration{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := r.backoff(i + 1); got != w*time.Millisecond {
			t.Errorf("backoff(%d) = %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
	if got := r.backoff(1 << 20); got != time.Second {
		t.Errorf("backoff(huge) = %v, want the cap", got)
	}

	for range 1000 {
		if d := equalJitter(time.Second); d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("equalJitter(1s) = %v, want within [500ms, 1s]", d)
		}
	}
}

func TestNewOutboxRelayValidatesOptions(t *testing.T) {
	bad := fastOptions()
	bad.BatchSize = 0
	bad.Lease = bad.PublishTimeout // not more than twice the timeout
	bad.RetryMin = time.Hour
	_, err := NewOutboxRelay(&fakeRelayOutbox{}, &fakePublisher{}, bad, slog.Default())
	if err == nil {
		t.Fatal("NewOutboxRelay accepted invalid options")
	}
	for _, want := range []string{"batch size", "lease", "minimum retry delay"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
