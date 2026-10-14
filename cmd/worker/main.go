// Command worker runs the provisioning simulator: it consumes task events
// and publishes progress updates after configurable delays and failures.
//
//	worker --fail-rate=0.3 --min-delay-ms=200 --max-delay-ms=2000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
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

	simulator := worker.NewSimulator(cfg.Simulation, rabbitmq.NewUpdatePublisher(cfg.RabbitMQURL), logger)
	consumer := rabbitmq.NewTaskConsumer(cfg.RabbitMQURL, simulator, logger)

	logger.Info("worker starting",
		"fail_rate", cfg.Simulation.FailRate,
		"min_delay", cfg.Simulation.MinDelay.String(),
		"max_delay", cfg.Simulation.MaxDelay.String())
	// Cancellation via signal is a clean exit; anything else (including nil) is passed through.
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
