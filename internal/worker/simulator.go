// Package worker implements the provisioning simulator: it receives task
// events and reports progress (in_progress, then done or failed) after
// configurable delays.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// UpdatePublisher sends progress updates back to the control plane. It
// returns nil only once the broker has accepted the update.
type UpdatePublisher interface {
	PublishUpdate(ctx context.Context, update domain.TaskUpdate) error
}

// Random is the simulator's source of randomness. *rand.Rand from
// math/rand/v2 satisfies it; tests supply scripted values.
type Random interface {
	// Float64 returns a number in [0, 1).
	Float64() float64
	// Int64N returns a number in [0, n); n > 0.
	Int64N(n int64) int64
}

// ProgressUpdateID is the idempotency key of the in_progress update for a
// task, and OutcomeUpdateID that of its outcome (done or failed). They are
// derived from the task ID, not random, so a task redelivered to a worker
// (after a crash before the ack, say) yields the same IDs and the control
// plane discards the repeats. The outcome has one ID whatever it is, so a
// rerun that draws a different outcome is discarded too: the first
// outcome recorded wins.
func ProgressUpdateID(taskID string) string { return taskID + ":in_progress" }

// OutcomeUpdateID is the idempotency key of a task's outcome update; see
// ProgressUpdateID.
func OutcomeUpdateID(taskID string) string { return taskID + ":outcome" }

// Simulator fakes the provisioning work for a task. It is safe for
// concurrent use.
type Simulator struct {
	cfg       config.Simulation
	publisher UpdatePublisher
	logger    *slog.Logger

	mu  sync.Mutex // guards rnd, which need not be safe for concurrent use
	rnd Random

	// Replaced by tests.
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time
}

// NewSimulator wires a Simulator. It rejects a configuration that would
// make it misbehave, even though config.LoadWorker validates it too.
func NewSimulator(cfg config.Simulation, publisher UpdatePublisher, rnd Random, logger *slog.Logger) (*Simulator, error) {
	switch {
	case math.IsNaN(cfg.FailRate) || cfg.FailRate < 0 || cfg.FailRate > 1:
		return nil, fmt.Errorf("fail rate must be within [0,1], got %v", cfg.FailRate)
	case cfg.MinDelay < 0 || cfg.MinDelay > cfg.MaxDelay:
		return nil, fmt.Errorf("delays must satisfy 0 <= min (%v) <= max (%v)", cfg.MinDelay, cfg.MaxDelay)
	}
	return &Simulator{
		cfg:       cfg,
		publisher: publisher,
		logger:    logger,
		rnd:       rnd,
		sleep:     sleepContext,
		now:       time.Now,
	}, nil
}

// Process simulates the work for an accepted task:
//
//  1. wait a random delay in [MinDelay, MaxDelay], publish in_progress;
//  2. wait another random delay, publish done, or failed with
//     probability FailRate.
//
// It returns nil only once both updates have been published, which is
// when the caller may acknowledge the task. On error, including ctx being
// cancelled, the caller requeues the task; running it again publishes
// updates with the same IDs, which the control plane deduplicates.
//
// Random values are drawn in a fixed order (delay, delay, outcome), so a
// seeded source reproduces a run exactly.
func (s *Simulator) Process(ctx context.Context, task domain.Task) error {
	if err := s.sleep(ctx, s.delay()); err != nil {
		return fmt.Errorf("simulate task %s: %w", task.ID, err)
	}
	started := s.update(task, ProgressUpdateID(task.ID), domain.TaskStatusAccepted, domain.TaskStatusInProgress, "")
	if err := s.publisher.PublishUpdate(ctx, started); err != nil {
		return fmt.Errorf("report task %s %s: %w", task.ID, started.Status, err)
	}
	s.logger.Info("task in progress", "task_id", task.ID, "task_type", task.Type, "tenant_id", task.TenantID)

	if err := s.sleep(ctx, s.delay()); err != nil {
		return fmt.Errorf("simulate task %s: %w", task.ID, err)
	}
	status, reason := domain.TaskStatusDone, ""
	if s.fails() {
		status = domain.TaskStatusFailed
		reason = fmt.Sprintf("simulated %s failure (fail rate %v)", task.Type, s.cfg.FailRate)
	}
	outcome := s.update(task, OutcomeUpdateID(task.ID), domain.TaskStatusInProgress, status, reason)
	if err := s.publisher.PublishUpdate(ctx, outcome); err != nil {
		return fmt.Errorf("report task %s %s: %w", task.ID, outcome.Status, err)
	}
	s.logger.Info("task finished", "task_id", task.ID, "task_type", task.Type, "tenant_id", task.TenantID,
		"status", outcome.Status, "reason", reason)
	return nil
}

func (s *Simulator) update(task domain.Task, id string, from, to domain.TaskStatus, reason string) domain.TaskUpdate {
	return domain.TaskUpdate{
		ID:             id,
		TaskID:         task.ID,
		TenantID:       task.TenantID,
		TaskType:       task.Type,
		PreviousStatus: from,
		Status:         to,
		Reason:         reason,
		OccurredAt:     domain.NormalizeTime(s.now()),
	}
}

// delay draws a duration in [MinDelay, MaxDelay], both inclusive.
func (s *Simulator) delay() time.Duration {
	span := int64(s.cfg.MaxDelay - s.cfg.MinDelay)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.MinDelay + time.Duration(s.rnd.Int64N(span+1))
}

// fails reports whether the task fails. Float64 is in [0, 1), so a fail
// rate of 0 never fails and 1 always does.
func (s *Simulator) fails() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rnd.Float64() < s.cfg.FailRate
}

// sleepContext waits for d or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
