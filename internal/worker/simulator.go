// Package worker implements the provisioning simulator: it receives task
// events and reports progress (in_progress, then done or failed) after
// configurable delays.
package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// UpdatePublisher sends progress updates back to the control plane.
type UpdatePublisher interface {
	PublishUpdate(ctx context.Context, update domain.TaskUpdate) error
}

// Simulator fakes the provisioning work for a task.
type Simulator struct {
	cfg       config.Simulation
	publisher UpdatePublisher
	logger    *slog.Logger
}

// NewSimulator wires a Simulator.
func NewSimulator(cfg config.Simulation, publisher UpdatePublisher, logger *slog.Logger) *Simulator {
	return &Simulator{cfg: cfg, publisher: publisher, logger: logger}
}

// Process simulates the work for task, publishing in_progress and then a
// terminal outcome.
func (s *Simulator) Process(_ context.Context, _ domain.Task) error {
	return fmt.Errorf("simulate task: %w", domain.ErrNotImplemented)
}
