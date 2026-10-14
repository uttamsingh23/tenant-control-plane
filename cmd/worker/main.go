// Command worker runs the provisioning simulator: it consumes task events
// and publishes progress updates after configurable delays and failures.
//
//	worker --fail-rate=0.3 --min-delay-ms=200 --max-delay-ms=2000 --seed=42
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/config"
	"github.com/uttamsingh23/tenant-control-plane/internal/worker"
)

func main() {
	err := run()
	switch {
	case errors.Is(err, flag.ErrHelp):
		return
	case err != nil:
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorker(os.Args[1:], os.Getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	seed := cfg.Seed
	if seed == 0 {
		seed = rand.Uint64() //nolint:gosec // simulation timing, not security
	}
	rnd := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // simulation timing, not security

	topology := rabbitmq.DefaultTopology()
	publisher := rabbitmq.NewUpdatePublisher(cfg.RabbitMQURL, topology, logger)
	// Closed after the consumer has returned, so the in-flight task can
	// publish its last update during shutdown.
	defer func() { _ = publisher.Close() }()
	simulator, err := worker.NewSimulator(cfg.Simulation, publisher, rnd, logger)
	if err != nil {
		return err
	}
	consumer := rabbitmq.NewTaskConsumer(cfg.RabbitMQURL, topology, simulator,
		rabbitmq.TaskConsumerOptions{ShutdownTimeout: cfg.ShutdownTimeout}, logger)

	logger.Info("worker starting",
		"fail_rate", cfg.Simulation.FailRate,
		"min_delay", cfg.Simulation.MinDelay.String(),
		"max_delay", cfg.Simulation.MaxDelay.String(),
		"seed", seed,
		"shutdown_timeout", cfg.ShutdownTimeout.String())
	err = consumer.Run(ctx)
	logger.Info("worker stopped")
	// Cancellation via signal is a clean exit; anything else (including nil) is passed through.
	if !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
