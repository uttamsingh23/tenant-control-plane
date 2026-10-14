package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

var now = time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)

// scriptedRandom returns queued values and records the bounds it was
// asked for. Running out of values fails the test.
type scriptedRandom struct {
	t      *testing.T
	ints   []int64
	floats []float64
	bounds []int64
}

func (r *scriptedRandom) Int64N(n int64) int64 {
	r.bounds = append(r.bounds, n)
	if len(r.ints) == 0 {
		r.t.Fatal("unexpected Int64N call")
	}
	v := r.ints[0]
	r.ints = r.ints[1:]
	if v < 0 || v >= n {
		r.t.Fatalf("scripted Int64N value %d outside [0,%d)", v, n)
	}
	return v
}

func (r *scriptedRandom) Float64() float64 {
	if len(r.floats) == 0 {
		r.t.Fatal("unexpected Float64 call")
	}
	v := r.floats[0]
	r.floats = r.floats[1:]
	return v
}

// recorder captures, in order, every sleep and publish of a simulator.
type recorder struct {
	events  []string
	updates []domain.TaskUpdate
	delays  []time.Duration
	// failOn makes the publish of that status fail.
	failOn domain.TaskStatus
}

func (r *recorder) PublishUpdate(_ context.Context, u domain.TaskUpdate) error {
	r.events = append(r.events, "publish "+string(u.Status))
	if u.Status == r.failOn {
		return errors.New("broker unavailable")
	}
	r.updates = append(r.updates, u)
	return nil
}

func newTestSimulator(t *testing.T, cfg config.Simulation, rnd Random) (*Simulator, *recorder) {
	t.Helper()
	rec := &recorder{}
	s, err := NewSimulator(cfg, rec, rnd, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	s.sleep = func(ctx context.Context, d time.Duration) error {
		rec.events = append(rec.events, "sleep")
		rec.delays = append(rec.delays, d)
		return ctx.Err()
	}
	return s, rec
}

func newTask(t *testing.T, typ domain.TaskType) domain.Task {
	t.Helper()
	task, err := domain.NewTask("task-1", typ, "tenant-1", now)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// TestProcessLifecycle: one task yields exactly in_progress then done,
// each after its delay, each naming the transition from the status the
// task was in, and each a valid step of the domain state machine.
func TestProcessLifecycle(t *testing.T) {
	rnd := &scriptedRandom{t: t, ints: []int64{0, 0}, floats: []float64{0.5}}
	s, rec := newTestSimulator(t, config.Simulation{FailRate: 0}, rnd)
	task := newTask(t, domain.TaskTypeDeploy)

	if err := s.Process(t.Context(), task); err != nil {
		t.Fatalf("Process: %v", err)
	}

	wantEvents := []string{"sleep", "publish in_progress", "sleep", "publish done"}
	if !slices.Equal(rec.events, wantEvents) {
		t.Errorf("events = %v, want %v", rec.events, wantEvents)
	}
	want := []domain.TaskUpdate{
		{
			ID: "task-1:in_progress", TaskID: "task-1", TenantID: "tenant-1", TaskType: domain.TaskTypeDeploy,
			PreviousStatus: domain.TaskStatusAccepted, Status: domain.TaskStatusInProgress, OccurredAt: now,
		},
		{
			ID: "task-1:outcome", TaskID: "task-1", TenantID: "tenant-1", TaskType: domain.TaskTypeDeploy,
			PreviousStatus: domain.TaskStatusInProgress, Status: domain.TaskStatusDone, OccurredAt: now,
		},
	}
	if !slices.Equal(rec.updates, want) {
		t.Fatalf("updates =\n%+v\nwant\n%+v", rec.updates, want)
	}
	status := task.Status
	for _, u := range rec.updates {
		if u.PreviousStatus != status || !status.CanTransitionTo(u.Status) {
			t.Errorf("update %s reports %s -> %s; task was %s", u.ID, u.PreviousStatus, u.Status, status)
		}
		status = u.Status
	}
	if !status.Terminal() {
		t.Errorf("task ends in %s, want a terminal status", status)
	}
}

// TestProcessAllTaskTypes: every task type goes through the same lifecycle.
func TestProcessAllTaskTypes(t *testing.T) {
	for _, typ := range []domain.TaskType{domain.TaskTypeDeploy, domain.TaskTypeUpdate, domain.TaskTypeDestroy} {
		t.Run(string(typ), func(t *testing.T) {
			s, rec := newTestSimulator(t, config.Simulation{}, rand.New(rand.NewPCG(1, 1)))
			if err := s.Process(t.Context(), newTask(t, typ)); err != nil {
				t.Fatal(err)
			}
			if len(rec.updates) != 2 || rec.updates[0].TaskType != typ || rec.updates[1].TaskType != typ {
				t.Errorf("updates = %+v, want two updates of type %s", rec.updates, typ)
			}
		})
	}
}

// TestProcessFailRate: the outcome is failed exactly when the draw is
// below the fail rate, so 0 never fails and 1 always does.
func TestProcessFailRate(t *testing.T) {
	tests := []struct {
		rate, draw float64
		want       domain.TaskStatus
	}{
		{0, 0, domain.TaskStatusDone},
		{0, 0.999999, domain.TaskStatusDone},
		{1, 0, domain.TaskStatusFailed},
		{1, 0.999999, domain.TaskStatusFailed},
		{0.3, 0.299, domain.TaskStatusFailed},
		{0.3, 0.3, domain.TaskStatusDone},
	}
	for _, tt := range tests {
		rnd := &scriptedRandom{t: t, ints: []int64{0, 0}, floats: []float64{tt.draw}}
		s, rec := newTestSimulator(t, config.Simulation{FailRate: tt.rate}, rnd)
		if err := s.Process(t.Context(), newTask(t, domain.TaskTypeUpdate)); err != nil {
			t.Fatal(err)
		}
		got := rec.updates[1]
		if got.Status != tt.want {
			t.Errorf("rate %v, draw %v: outcome %s, want %s", tt.rate, tt.draw, got.Status, tt.want)
		}
		if failed := got.Status == domain.TaskStatusFailed; failed != (got.Reason != "") {
			t.Errorf("rate %v, draw %v: outcome %s with reason %q; want a reason exactly for failed",
				tt.rate, tt.draw, got.Status, got.Reason)
		}
		if got.ID != OutcomeUpdateID("task-1") {
			t.Errorf("outcome ID = %q, want %q", got.ID, OutcomeUpdateID("task-1"))
		}
	}
}

// TestProcessDelays: each delay is MinDelay plus a draw over the whole
// inclusive range, so both bounds are reachable.
func TestProcessDelays(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name      string
		min, max  time.Duration
		draws     []int64
		wantBound int64
		want      []time.Duration
	}{
		{"range, both bounds", 100 * ms, 300 * ms, []int64{0, int64(200 * ms)}, int64(200*ms) + 1, []time.Duration{100 * ms, 300 * ms}},
		{"range, midpoint", 100 * ms, 300 * ms, []int64{int64(50 * ms), int64(150 * ms)}, int64(200*ms) + 1, []time.Duration{150 * ms, 250 * ms}},
		{"fixed delay", 250 * ms, 250 * ms, []int64{0, 0}, 1, []time.Duration{250 * ms, 250 * ms}},
		{"no delay", 0, 0, []int64{0, 0}, 1, []time.Duration{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rnd := &scriptedRandom{t: t, ints: tt.draws, floats: []float64{0.5}}
			s, rec := newTestSimulator(t, config.Simulation{MinDelay: tt.min, MaxDelay: tt.max}, rnd)
			if err := s.Process(t.Context(), newTask(t, domain.TaskTypeDeploy)); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(rec.delays, tt.want) {
				t.Errorf("delays = %v, want %v", rec.delays, tt.want)
			}
			for _, b := range rnd.bounds {
				if b != tt.wantBound {
					t.Errorf("Int64N bound = %d, want %d", b, tt.wantBound)
				}
			}
		})
	}
}

// TestProcessRealDelaysStayInRange: with a real random source, every
// delay lies within the configured bounds.
func TestProcessRealDelaysStayInRange(t *testing.T) {
	cfg := config.Simulation{MinDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	s, rec := newTestSimulator(t, cfg, rand.New(rand.NewPCG(7, 7)))
	for range 200 {
		if err := s.Process(t.Context(), newTask(t, domain.TaskTypeDeploy)); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range rec.delays {
		if d < cfg.MinDelay || d > cfg.MaxDelay {
			t.Fatalf("delay %v outside [%v, %v]", d, cfg.MinDelay, cfg.MaxDelay)
		}
	}
}

// TestProcessStableIDs: processing a task again (as after a redelivery)
// yields the same update IDs, even when the rerun draws a different
// outcome, and different tasks never share an ID.
func TestProcessStableIDs(t *testing.T) {
	s, rec := newTestSimulator(t, config.Simulation{FailRate: 0.5},
		&scriptedRandom{t: t, ints: []int64{0, 0, 0, 0, 0, 0}, floats: []float64{0.9, 0.1, 0.9}})
	task := newTask(t, domain.TaskTypeDeploy)
	other, err := domain.NewTask("task-2", domain.TaskTypeDeploy, "tenant-1", now)
	if err != nil {
		t.Fatal(err)
	}
	for _, tk := range []domain.Task{task, task, other} {
		if err := s.Process(t.Context(), tk); err != nil {
			t.Fatal(err)
		}
	}
	first, rerun, second := rec.updates[0:2], rec.updates[2:4], rec.updates[4:6]
	if first[1].Status == rerun[1].Status {
		t.Fatalf("test setup: rerun should draw a different outcome, both are %s", first[1].Status)
	}
	for i := range first {
		if first[i].ID != rerun[i].ID {
			t.Errorf("rerun update %d ID = %q, want %q", i, rerun[i].ID, first[i].ID)
		}
	}
	ids := map[string]bool{}
	for _, u := range slices.Concat(first, second) {
		if ids[u.ID] {
			t.Errorf("ID %q used by two logical updates", u.ID)
		}
		ids[u.ID] = true
	}
}

// TestProcessPublishFailure: a failed publish stops processing and is
// returned, so the caller requeues the task instead of acking it.
func TestProcessPublishFailure(t *testing.T) {
	for _, status := range []domain.TaskStatus{domain.TaskStatusInProgress, domain.TaskStatusDone} {
		t.Run(string(status), func(t *testing.T) {
			s, rec := newTestSimulator(t, config.Simulation{}, rand.New(rand.NewPCG(1, 1)))
			rec.failOn = status
			if err := s.Process(t.Context(), newTask(t, domain.TaskTypeDeploy)); err == nil {
				t.Fatal("Process succeeded although publishing failed")
			}
			if last := rec.events[len(rec.events)-1]; last != "publish "+string(status) {
				t.Errorf("events = %v: processing went on after the failed publish", rec.events)
			}
		})
	}
}

// TestProcessCancelled: cancelling the context interrupts a delay with
// the context's error and publishes nothing further.
func TestProcessCancelled(t *testing.T) {
	cfg := config.Simulation{MinDelay: time.Hour, MaxDelay: time.Hour}
	rec := &recorder{}
	s, err := NewSimulator(cfg, rec, rand.New(rand.NewPCG(1, 1)), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)

	start := time.Now()
	err = s.Process(ctx, newTask(t, domain.TaskTypeDeploy))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Process returned after %v; the delay was not interrupted", elapsed)
	}
	if len(rec.updates) != 0 {
		t.Errorf("published %d updates after cancellation", len(rec.updates))
	}
}

// TestProcessSeedIsReproducible: two simulators seeded alike produce the
// same delays and outcomes for the same tasks, and the outcome frequency
// tracks the fail rate.
func TestProcessSeedIsReproducible(t *testing.T) {
	cfg := config.Simulation{FailRate: 0.3, MinDelay: 0, MaxDelay: time.Second}
	run := func() ([]time.Duration, []domain.TaskStatus) {
		s, rec := newTestSimulator(t, cfg, rand.New(rand.NewPCG(42, 42)))
		var outcomes []domain.TaskStatus
		for range 1000 {
			if err := s.Process(t.Context(), newTask(t, domain.TaskTypeDeploy)); err != nil {
				t.Fatal(err)
			}
			outcomes = append(outcomes, rec.updates[len(rec.updates)-1].Status)
		}
		return rec.delays, outcomes
	}
	delays1, outcomes1 := run()
	delays2, outcomes2 := run()
	if !slices.Equal(delays1, delays2) || !slices.Equal(outcomes1, outcomes2) {
		t.Fatal("same seed produced different runs")
	}
	failed := 0
	for _, o := range outcomes1 {
		if o == domain.TaskStatusFailed {
			failed++
		}
	}
	if rate := float64(failed) / float64(len(outcomes1)); math.Abs(rate-cfg.FailRate) > 0.05 {
		t.Errorf("observed fail rate %.3f, want about %v", rate, cfg.FailRate)
	}
}

func TestNewSimulatorRejectsInvalidConfig(t *testing.T) {
	tests := map[string]config.Simulation{
		"NaN fail rate":      {FailRate: math.NaN()},
		"negative fail rate": {FailRate: -0.1},
		"fail rate above 1":  {FailRate: 1.1},
		"negative min delay": {MinDelay: -time.Millisecond, MaxDelay: time.Millisecond},
		"min above max":      {MinDelay: 2 * time.Second, MaxDelay: time.Second},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewSimulator(cfg, &recorder{}, rand.New(rand.NewPCG(1, 1)), slog.Default()); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
