package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// enqueuedTask inserts a tenant, its deploy task and the task's event.
func enqueuedTask(t *testing.T, store *postgres.Store, slug string) domain.Task {
	t.Helper()
	tn := insertTenant(t, store, slug)
	task := insertTask(t, store, domain.TaskTypeDeploy, tn.ID)
	if err := store.Outbox().Enqueue(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	return task
}

// testLease outlasts every test: a claimed event stays claimed.
const testLease = time.Minute

func claim(t *testing.T, store *postgres.Store, limit int, lease time.Duration) []application.OutboxEvent {
	t.Helper()
	events, err := store.Outbox().ClaimPending(t.Context(), limit, lease)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func eventIDs(events []application.OutboxEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Task.ID
	}
	return out
}

// outboxRow is the delivery state of one event, read with raw SQL.
type outboxRow struct {
	published   bool
	claimed     bool
	available   bool // claimable now
	attempts    int
	lastError   *string
	availableAt time.Time
}

func readOutboxRow(t *testing.T, pool *pgxpool.Pool, taskID string) outboxRow {
	t.Helper()
	var r outboxRow
	err := pool.QueryRow(t.Context(), `
		SELECT published_at IS NOT NULL, claim_token IS NOT NULL, available_at <= now(), attempts, last_error, available_at
		FROM outbox_events WHERE task_id = $1`, taskID).
		Scan(&r.published, &r.claimed, &r.available, &r.attempts, &r.lastError, &r.availableAt)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// assertPendingEvents checks the unpublished events, in claim order,
// without claiming them.
func assertPendingEvents(t *testing.T, pool *pgxpool.Pool, wantTaskIDs ...string) {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT task_id::text FROM outbox_events WHERE published_at IS NULL ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, wantTaskIDs) {
		t.Errorf("pending events = %v, want %v", got, wantTaskIDs)
	}
}

// TestOutboxEventIsTaskSnapshot checks the claimed event is the task as it
// was when enqueued, even after the worker has moved the task on.
func TestOutboxEventIsTaskSnapshot(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	want := enqueuedTask(t, store, "acme")

	moved := want
	if err := moved.TransitionTo(domain.TaskStatusInProgress, base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Tasks().Update(ctx, moved); err != nil {
		t.Fatal(err)
	}

	got := claim(t, store, 10, testLease)
	if len(got) != 1 || got[0].Task != want || got[0].Attempts != 0 || got[0].ClaimToken == "" {
		t.Errorf("claimed %+v, want [%+v] with no attempts and a claim token", got, want)
	}
}

// TestOutboxClaimLeases: a claim takes due events oldest first, up to the
// limit, and keeps them from later claims until the lease expires.
func TestOutboxClaimLeases(t *testing.T) {
	store, pool := newStore(t)
	a := enqueuedTask(t, store, "tenant-a")
	b := enqueuedTask(t, store, "tenant-b")
	c := enqueuedTask(t, store, "tenant-c")

	first := claim(t, store, 2, testLease)
	if got := eventIDs(first); !slices.Equal(got, []string{a.ID, b.ID}) {
		t.Errorf("first claim = %v, want [a b]", got)
	}
	if got := eventIDs(claim(t, store, 10, testLease)); !slices.Equal(got, []string{c.ID}) {
		t.Errorf("second claim = %v, want only c", got)
	}
	if got := claim(t, store, 10, testLease); len(got) != 0 {
		t.Errorf("third claim = %v, want nothing: all events are leased", eventIDs(got))
	}
	// Claimed is not published.
	assertPendingEvents(t, pool, a.ID, b.ID, c.ID)

	for _, args := range []struct {
		limit int
		lease time.Duration
	}{{0, time.Second}, {-1, time.Second}, {1, 0}, {1, -time.Second}} {
		if _, err := store.Outbox().ClaimPending(t.Context(), args.limit, args.lease); err == nil {
			t.Errorf("ClaimPending(%d, %v) succeeded, want error", args.limit, args.lease)
		}
	}
}

// TestOutboxExpiredLeaseIsReclaimed: the events of a relay that died are
// claimed again once the lease expires, with a new claim token.
func TestOutboxExpiredLeaseIsReclaimed(t *testing.T) {
	store, _ := newStore(t)
	task := enqueuedTask(t, store, "acme")

	const lease = 200 * time.Millisecond
	first := claim(t, store, 10, lease)
	if len(first) != 1 {
		t.Fatalf("claimed %d events, want 1", len(first))
	}
	if got := claim(t, store, 10, lease); len(got) != 0 {
		t.Fatalf("claimed %v while leased", eventIDs(got))
	}
	time.Sleep(lease + 50*time.Millisecond)
	second := claim(t, store, 10, lease)
	if len(second) != 1 || second[0].Task.ID != task.ID || second[0].ClaimToken == first[0].ClaimToken {
		t.Errorf("after expiry claimed %+v, want the event under a new claim token", second)
	}
}

// TestOutboxClaimSeesOnlyCommittedEvents: an event is claimable only once
// the transaction that wrote it has committed, and never if it rolls back.
func TestOutboxClaimSeesOnlyCommittedEvents(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()

	for _, commit := range []bool{true, false} {
		tn := newTenant(t, fmt.Sprintf("tenant-%v", commit), base)
		task := newTask(t, domain.TaskTypeDeploy, tn.ID, base)
		written, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			done <- store.WithinTx(ctx, createInTx(ctx, tn, task, func() error {
				close(written)
				<-finish
				if !commit {
					return errBoom
				}
				return nil
			}))
		}()
		<-written
		if got := claim(t, store, 10, testLease); len(got) != 0 {
			t.Errorf("claimed uncommitted event %v", eventIDs(got))
		}
		close(finish)
		if err := <-done; commit != (err == nil) {
			t.Fatalf("commit=%v: WithinTx err = %v", commit, err)
		}
		got := claim(t, store, 10, testLease)
		if commit && (len(got) != 1 || got[0].Task.ID != task.ID) {
			t.Errorf("after commit claimed %v, want the event", eventIDs(got))
		}
		if !commit && len(got) != 0 {
			t.Errorf("after rollback claimed %v, want nothing", eventIDs(got))
		}
	}
}

// TestOutboxConcurrentClaimsAreDisjoint races many claimers (as several
// relays would) over one outbox: every event is claimed exactly once.
func TestOutboxConcurrentClaimsAreDisjoint(t *testing.T) {
	store, _ := newStore(t)
	const events, claimers = 40, 16
	want := map[string]bool{}
	for i := range events {
		want[enqueuedTask(t, store, fmt.Sprintf("tenant-%02d", i)).ID] = true
	}

	claimed := make([][]application.OutboxEvent, claimers)
	errs := parallel(claimers, func(i int) error {
		// Bounded: if claims stopped excluding leased events, this would
		// otherwise never see an empty claim.
		for range events {
			got, err := store.Outbox().ClaimPending(context.Background(), 3, testLease)
			if err != nil || len(got) == 0 {
				return err
			}
			claimed[i] = append(claimed[i], got...)
		}
		return nil
	})
	seen := map[string]int{}
	for i := range claimers {
		if errs[i] != nil {
			t.Fatalf("claimer %d: %v", i, errs[i])
		}
		for _, e := range claimed[i] {
			seen[e.Task.ID]++
		}
	}
	for id := range want {
		if seen[id] != 1 {
			t.Errorf("event %s claimed %d times, want 1", id[:8], seen[id])
		}
	}
	if len(seen) != events {
		t.Errorf("claimed %d distinct events, want %d", len(seen), events)
	}
}

func TestOutboxMarkPublished(t *testing.T) {
	store, pool := newStore(t)
	ctx := t.Context()
	a := enqueuedTask(t, store, "tenant-a")
	b := enqueuedTask(t, store, "tenant-b")

	if err := store.Outbox().MarkPublished(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	assertPendingEvents(t, pool, b.ID)
	if got := eventIDs(claim(t, store, 10, testLease)); !slices.Equal(got, []string{b.ID}) {
		t.Errorf("claimed %v, want only the unpublished event", got)
	}

	// Idempotent: a repeat keeps the first publication time.
	var first, second time.Time
	publishedAt := `SELECT published_at FROM outbox_events WHERE task_id = $1`
	if err := pool.QueryRow(ctx, publishedAt, a.ID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := store.Outbox().MarkPublished(ctx, a.ID); err != nil {
		t.Fatalf("second MarkPublished: %v", err)
	}
	if err := pool.QueryRow(ctx, publishedAt, a.ID).Scan(&second); err != nil || !second.Equal(first) {
		t.Errorf("published_at changed from %v to %v (%v)", first, second, err)
	}

	for _, id := range []string{uuid.NewString(), "not-a-uuid"} {
		if err := store.Outbox().MarkPublished(ctx, id); !errors.Is(err, domain.ErrTaskNotFound) {
			t.Errorf("MarkPublished(%q) err = %v, want ErrTaskNotFound", id, err)
		}
	}
}

// TestOutboxMarkPublishedAfterLeaseLost: a relay whose lease expired (and
// whose event was claimed again) still records a confirmed publication.
func TestOutboxMarkPublishedAfterLeaseLost(t *testing.T) {
	store, pool := newStore(t)
	task := enqueuedTask(t, store, "acme")
	claim(t, store, 1, 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	claim(t, store, 1, testLease) // another relay takes over

	if err := store.Outbox().MarkPublished(t.Context(), task.ID); err != nil {
		t.Fatal(err)
	}
	if !readOutboxRow(t, pool, task.ID).published {
		t.Error("event not marked published")
	}
}

func TestOutboxRescheduleFailure(t *testing.T) {
	store, pool := newStore(t)
	ctx := t.Context()
	task := enqueuedTask(t, store, "acme")

	event := claim(t, store, 1, testLease)[0]
	if err := store.Outbox().Reschedule(ctx, event, time.Hour, errors.New("connection refused")); err != nil {
		t.Fatal(err)
	}
	row := readOutboxRow(t, pool, task.ID)
	if row.published || row.claimed || row.available || row.attempts != 1 || row.lastError == nil || *row.lastError != "connection refused" {
		t.Errorf("after failure row = %+v; want unpublished, unclaimed, not yet due, 1 attempt, error kept", row)
	}
	if got := claim(t, store, 10, testLease); len(got) != 0 {
		t.Errorf("claimed %v before its retry time", eventIDs(got))
	}

	// Due again: the attempt count travels with the next claim.
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET available_at = now() WHERE task_id = $1`, task.ID); err != nil {
		t.Fatal(err)
	}
	event = claim(t, store, 1, testLease)[0]
	if event.Attempts != 1 {
		t.Errorf("reclaimed event attempts = %d, want 1", event.Attempts)
	}

	// Error text is bounded.
	if err := store.Outbox().Reschedule(ctx, event, 0, errors.New(strings.Repeat("x", 5000))); err != nil {
		t.Fatal(err)
	}
	row = readOutboxRow(t, pool, task.ID)
	if row.attempts != 2 || row.lastError == nil || len(*row.lastError) != 1000 || !row.available {
		t.Errorf("after second failure: attempts = %d, len(last_error) = %d, available = %v; want 2, 1000, true",
			row.attempts, len(*row.lastError), row.available)
	}
}

// TestOutboxRescheduleHandBack: an untried event is handed back without a
// failed attempt being charged, and claimable at once with delay 0.
func TestOutboxRescheduleHandBack(t *testing.T) {
	store, pool := newStore(t)
	task := enqueuedTask(t, store, "acme")
	event := claim(t, store, 1, testLease)[0]

	if err := store.Outbox().Reschedule(t.Context(), event, 0, nil); err != nil {
		t.Fatal(err)
	}
	if row := readOutboxRow(t, pool, task.ID); row.attempts != 0 || row.lastError != nil || row.claimed || !row.available {
		t.Errorf("after hand-back row = %+v; want no attempt, no error, unclaimed, due", row)
	}
	if got := claim(t, store, 1, testLease); len(got) != 1 {
		t.Error("handed-back event not claimable")
	}
}

// TestOutboxRescheduleIgnoresStaleClaims: a relay whose claim is no longer
// current cannot move the new claimant's lease, and a published event is
// never rescheduled.
func TestOutboxRescheduleIgnoresStaleClaims(t *testing.T) {
	store, pool := newStore(t)
	ctx := t.Context()
	task := enqueuedTask(t, store, "acme")

	stale := claim(t, store, 1, 50*time.Millisecond)[0]
	time.Sleep(80 * time.Millisecond)
	current := claim(t, store, 1, testLease)[0]
	before := readOutboxRow(t, pool, task.ID)

	if err := store.Outbox().Reschedule(ctx, stale, 0, errors.New("late failure")); err != nil {
		t.Fatal(err)
	}
	if after := readOutboxRow(t, pool, task.ID); after != before {
		t.Errorf("stale reschedule changed row from %+v to %+v", before, after)
	}

	if err := store.Outbox().MarkPublished(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Outbox().Reschedule(ctx, current, 0, errors.New("late failure")); err != nil {
		t.Fatal(err)
	}
	if row := readOutboxRow(t, pool, task.ID); !row.published || row.attempts != 0 {
		t.Errorf("published event rescheduled: %+v", row)
	}

	bad := current
	bad.ClaimToken = "not-a-uuid"
	if err := store.Outbox().Reschedule(ctx, bad, 0, nil); err == nil {
		t.Error("Reschedule with an invalid claim token succeeded")
	}
}

func TestOutboxOneEventPerTask(t *testing.T) {
	store, _ := newStore(t)
	task := enqueuedTask(t, store, "acme")

	err := store.Outbox().Enqueue(t.Context(), task)
	pgError(t, err, "23505", "outbox_events_task_id_key")

	err = store.Outbox().Enqueue(t.Context(), newTask(t, domain.TaskTypeDeploy, task.TenantID, base))
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("event for unknown task: err = %v, want ErrTaskNotFound", err)
	}
}

func TestMarkProcessed(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	id := uuid.NewString()

	if fresh, err := store.ProcessedUpdates().MarkProcessed(ctx, id); err != nil || !fresh {
		t.Errorf("first MarkProcessed = %v, %v; want true", fresh, err)
	}
	if fresh, err := store.ProcessedUpdates().MarkProcessed(ctx, id); err != nil || fresh {
		t.Errorf("repeat MarkProcessed = %v, %v; want false", fresh, err)
	}
}

// TestMarkProcessedRolledBack shows a key recorded in a rolled-back
// transaction is not consumed, so the redelivered update is applied.
func TestMarkProcessedRolledBack(t *testing.T) {
	store, _ := newStore(t)
	ctx := t.Context()
	id := uuid.NewString()

	err := store.WithinTx(ctx, func(ctx context.Context, r application.Repositories) error {
		if _, err := r.ProcessedUpdates().MarkProcessed(ctx, id); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	if fresh, err := store.ProcessedUpdates().MarkProcessed(ctx, id); err != nil || !fresh {
		t.Errorf("MarkProcessed after rollback = %v, %v; want true", fresh, err)
	}
}

func TestMarkProcessedConcurrent(t *testing.T) {
	store, _ := newStore(t)
	id := uuid.NewString()
	const n = 16
	fresh := make([]bool, n)
	errs := parallel(n, func(i int) error {
		return store.WithinTx(context.Background(), func(ctx context.Context, r application.Repositories) error {
			var err error
			fresh[i], err = r.ProcessedUpdates().MarkProcessed(ctx, id)
			return err
		})
	})
	var wins int
	for i := range n {
		if errs[i] != nil {
			t.Errorf("caller %d: %v", i, errs[i])
		}
		if fresh[i] {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("%d callers saw the update as new, want exactly 1", wins)
	}
}

func TestMarkProcessedRejectsInvalidKeys(t *testing.T) {
	store, _ := newStore(t)
	for _, id := range []string{"", strings.Repeat("x", 201)} {
		if _, err := store.ProcessedUpdates().MarkProcessed(t.Context(), id); err == nil {
			t.Errorf("MarkProcessed(len %d) succeeded, want check violation", len(id))
		}
	}
}
