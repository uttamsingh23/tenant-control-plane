// Command api runs the control-plane service: the HTTP API, the outbox
// relay that publishes task events and the consumer of worker updates.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/httpapi"
	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/postgres"
	"github.com/uttamsingh23/tenant-control-plane/internal/adapters/rabbitmq"
	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		return err
	}
	store := postgres.NewStore(pool)

	tenants := application.NewTenantService(store, store, uuid.NewString, time.Now)
	tasks := application.NewTaskService(store, store, time.Now)
	relay := application.NewOutboxRelay(store, rabbitmq.NewTaskPublisher(cfg.RabbitMQURL), cfg.OutboxPollInterval, logger)
	consumer := rabbitmq.NewUpdateConsumer(cfg.RabbitMQURL, tasks, logger)

	var background sync.WaitGroup
	runBackground(ctx, &background, logger, "outbox relay", relay.Run)
	runBackground(ctx, &background, logger, "update consumer", consumer.Run)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTP.Addr, err)
	}
	srv := httpapi.NewServer(cfg.HTTP, httpapi.NewRouter(tenants, tasks, logger))
	logger.Info("http server listening", "addr", ln.Addr().String())

	serveErr := httpapi.Serve(ctx, srv, ln, cfg.HTTP.ShutdownTimeout)
	// Stop background components too if the server failed on its own.
	stop()
	background.Wait()
	logger.Info("shutdown complete")
	return serveErr
}

// runBackground runs fn in a goroutine tracked by wg. A failure is logged
// but does not stop the HTTP server.
func runBackground(ctx context.Context, wg *sync.WaitGroup, logger *slog.Logger, name string, fn func(context.Context) error) {
	wg.Go(func() {
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("background component stopped", "component", name, "err", err)
		}
	})
}
