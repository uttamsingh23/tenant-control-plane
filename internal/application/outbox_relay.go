package application

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// OutboxRelay publishes committed task events from the outbox to the
// broker. Delivery is at-least-once: an event is marked published only
// after the broker confirms it.
type OutboxRelay struct {
	tx        Transactor
	publisher TaskPublisher
	interval  time.Duration
	logger    *slog.Logger
}

// NewOutboxRelay wires an OutboxRelay that polls every interval.
func NewOutboxRelay(tx Transactor, publisher TaskPublisher, interval time.Duration, logger *slog.Logger) *OutboxRelay {
	return &OutboxRelay{tx: tx, publisher: publisher, interval: interval, logger: logger}
}

// Run relays events until ctx is cancelled.
func (r *OutboxRelay) Run(_ context.Context) error {
	return fmt.Errorf("outbox relay: %w", domain.ErrNotImplemented)
}
