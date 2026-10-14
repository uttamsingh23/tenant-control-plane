package tests

// End-to-end recovery tests: the whole loop (API -> outbox -> relay ->
// worker simulator -> update consumer -> database) after a publisher crash
// and after a control-plane restart. They check the domain state the
// system ends in, not only that messages were (re)delivered.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

// TestRecoveryPublisherCrashRedelivery: the relay publishes a task and
// crashes before marking it published, so after the lease the task is
// published again and the worker runs it twice. The second run reports
// under the same idempotency keys, so it changes nothing: the task ends
// in one terminal state, its tenant matches it, and the tenant version
// moved exactly once. With fail-rate 0.5 the rerun may draw the opposite
// outcome; the first one recorded still wins.
func TestRecoveryPublisherCrashRedelivery(t *testing.T) {
	for _, failRate := range []float64{0, 0.5} {
		t.Run(fmt.Sprintf("fail-rate=%v", failRate), func(t *testing.T) {
			a := newAPI(t)
			topo := workerTopology(t)
			opts := relayOptions()
			opts.Lease = 1500 * time.Millisecond
			startRelay(t, &crashAfterConfirm{OutboxRepository: a.store.Outbox()}, amqptest.URL(t), topo, opts)
			log := newOutcomeLog(a.tasks)
			startConsumer(t, amqptest.URL(t), topo, log, inboundOptions())
			startWorker(t, topo, failRate)

			m := a.create("crash-loop", "Crash loop")
			// Two worker runs, each reporting in_progress and an outcome.
			log.wait(t, 4)
			a.waitAllPublished()

			if errs := log.failures(); len(errs) != 0 {
				t.Fatalf("updates failed: %v", errs)
			}
			for _, id := range []string{worker.ProgressUpdateID(m.Task.ID), worker.OutcomeUpdateID(m.Task.ID)} {
				got := log.of(id)
				// The first delivery of a key is applied (or stale, if the
				// outcome overtook in_progress); the rerun's is a duplicate.
				if len(got) != 2 || got[0] == application.UpdateDuplicate || got[1] != application.UpdateDuplicate {
					t.Errorf("deliveries of %s = %v, want a first application then a duplicate", id, got)
				}
			}
			task := a.getTask(m.Task.ID)
			want := map[string]string{"done": "active", "failed": "failed"}[task.Status]
			if want == "" {
				t.Fatalf("task is %s, want terminal", task.Status)
			}
			a.wantTenant(m.Tenant.ID, want, 2)
			if n := a.processedKeys(); n != 2 {
				t.Errorf("processed_updates holds %d keys, want 2", n)
			}
		})
	}
}

// TestRecoveryRestart: the control plane stops with work in every state:
// a tenant that is active, one whose update is committed but not yet
// published, and a new tenant whose deploy is not yet published. A fresh
// process on the same database serves the same tenants, publishes the
// pending events and completes the work. The deploy event published before
// the restart is still in the worker queue; when the worker runs it again
// after the restart, its outcome is stale and moves nothing back.
func TestRecoveryRestart(t *testing.T) {
	topo := workerTopology(t)

	// Process 1: no worker or consumer, so nothing completes on its own.
	first := newAPI(t)
	connString := first.pool.Config().ConnString()
	stopRelay := startRelay(t, first.store.Outbox(), amqptest.URL(t), topo, relayOptions())
	updated := first.create("restart-a", "A")
	first.waitAllPublished()
	first.finish(updated.Task.ID, domain.TaskStatusDone)
	if err := stopRelay(); !errors.Is(err, context.Canceled) {
		t.Fatalf("relay Run after shutdown = %v, want context.Canceled", err)
	}
	tn := first.getTenant(updated.Tenant.ID)
	expect[mutationJSON](t, first.do(http.MethodPatch, "/v1/tenants/"+tn.ID,
		map[string]any{"name": "A2", "version": tn.Version}), http.StatusAccepted)
	created := first.create("restart-b", "B")
	before := map[string]tenantJSON{}
	for _, id := range []string{updated.Tenant.ID, created.Tenant.ID} {
		before[id] = first.getTenant(id)
	}
	if n := first.pendingEvents(); n != 2 {
		t.Fatalf("%d pending outbox events before the restart, want 2", n)
	}
	first.srv.Close()
	first.pool.Close()

	// Process 2: new connections, nothing carried over in memory.
	pool, err := pgxpool.New(t.Context(), connString)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	second := newAPIOn(t, pool)
	for id, want := range before {
		if got := second.getTenant(id); got != want {
			t.Errorf("after restart tenant = %+v, want %+v", got, want)
		}
	}
	if n := second.pendingEvents(); n != 2 {
		t.Fatalf("%d pending outbox events after the restart, want 2", n)
	}

	startRelay(t, second.store.Outbox(), amqptest.URL(t), topo, relayOptions())
	startConsumer(t, amqptest.URL(t), topo, second.tasks, inboundOptions())
	startWorker(t, topo, 0)
	waitFor(t, "the pending work to complete", func() bool {
		return second.getTenant(updated.Tenant.ID).Status == "active" && second.getTenant(created.Tenant.ID).Status == "active"
	})
	second.waitAllPublished()
	// Keys: 2 for the deploy finished before the restart, 2 for the
	// worker's stale rerun of it, 2 for the update, 2 for the new deploy.
	// Wait for all of them, then check the rerun moved nothing.
	waitFor(t, "every worker report to be processed", func() bool { return second.processedKeys() == 8 })
	if got := second.wantTenant(updated.Tenant.ID, "active", 4); got.Name != "A2" {
		t.Errorf("name = %q, want A2", got.Name)
	}
	second.wantTenant(created.Tenant.ID, "active", 2)
}
