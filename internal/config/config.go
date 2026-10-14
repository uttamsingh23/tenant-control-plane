// Package config loads process configuration from environment variables
// (and, for the worker, command-line flags). It performs no I/O beyond
// reading the values it is given, so it is trivially testable.
package config

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// Getenv looks up an environment variable; os.Getenv satisfies it.
type Getenv func(key string) string

// HTTP configures the API server.
type HTTP struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	// RequestTimeout bounds the context of each request, so database calls
	// of a slow request are cancelled. It must be below WriteTimeout, or
	// the connection is closed before the timeout response can be written.
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
}

// Outbox configures the relay that publishes outbox events to the broker.
type Outbox struct {
	PollInterval time.Duration
	BatchSize    int
	// Lease is how long a claimed event is reserved for one relay. It
	// must exceed twice PublishTimeout (one publish plus its mark).
	Lease          time.Duration
	PublishTimeout time.Duration
	// RetryMin and RetryMax bound the exponential retry backoff.
	RetryMin time.Duration
	RetryMax time.Duration
}

// API is the configuration of the control-plane process.
type API struct {
	HTTP        HTTP
	DatabaseURL string
	RabbitMQURL string
	Outbox      Outbox
	LogLevel    slog.Level
}

// Simulation controls the timing and outcome of simulated provisioning.
type Simulation struct {
	// FailRate is the probability in [0, 1] that a task ends in failed.
	FailRate float64
	MinDelay time.Duration
	MaxDelay time.Duration
}

// Worker is the configuration of the worker simulator process.
type Worker struct {
	RabbitMQURL string
	Simulation  Simulation
	LogLevel    slog.Level
}

// LoadAPI reads the control-plane configuration from the environment.
func LoadAPI(getenv Getenv) (API, error) {
	r := reader{getenv: getenv}
	cfg := API{
		HTTP: HTTP{
			Addr:              r.string("HTTP_ADDR", ":8080"),
			ReadHeaderTimeout: r.duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
			ReadTimeout:       r.duration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:      r.duration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:       r.duration("HTTP_IDLE_TIMEOUT", 60*time.Second),
			RequestTimeout:    r.duration("HTTP_REQUEST_TIMEOUT", 10*time.Second),
			ShutdownTimeout:   r.duration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second),
		},
		DatabaseURL: r.required("DATABASE_URL"),
		RabbitMQURL: r.required("RABBITMQ_URL"),
		Outbox: Outbox{
			PollInterval:   r.duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:      r.int("OUTBOX_BATCH_SIZE", 100),
			Lease:          r.duration("OUTBOX_LEASE", 30*time.Second),
			PublishTimeout: r.duration("OUTBOX_PUBLISH_TIMEOUT", 5*time.Second),
			RetryMin:       r.duration("OUTBOX_RETRY_MIN", 500*time.Millisecond),
			RetryMax:       r.duration("OUTBOX_RETRY_MAX", 30*time.Second),
		},
		LogLevel: r.logLevel("LOG_LEVEL", slog.LevelInfo),
	}
	if cfg.HTTP.RequestTimeout >= cfg.HTTP.WriteTimeout {
		r.errs = append(r.errs, fmt.Errorf("HTTP_REQUEST_TIMEOUT (%v) must be less than HTTP_WRITE_TIMEOUT (%v)",
			cfg.HTTP.RequestTimeout, cfg.HTTP.WriteTimeout))
	}
	if o := cfg.Outbox; o.BatchSize <= 0 {
		r.errs = append(r.errs, fmt.Errorf("OUTBOX_BATCH_SIZE must be positive, got %d", o.BatchSize))
	}
	if o := cfg.Outbox; o.Lease <= 2*o.PublishTimeout {
		r.errs = append(r.errs, fmt.Errorf("OUTBOX_LEASE (%v) must exceed twice OUTBOX_PUBLISH_TIMEOUT (%v)",
			o.Lease, o.PublishTimeout))
	}
	if o := cfg.Outbox; o.RetryMin > o.RetryMax {
		r.errs = append(r.errs, fmt.Errorf("OUTBOX_RETRY_MIN (%v) must not exceed OUTBOX_RETRY_MAX (%v)",
			o.RetryMin, o.RetryMax))
	}
	if err := r.err(); err != nil {
		return API{}, err
	}
	return cfg, nil
}

// LoadWorker reads the worker configuration. Environment variables provide
// defaults; command-line flags in args override them.
func LoadWorker(args []string, getenv Getenv) (Worker, error) {
	r := reader{getenv: getenv}
	cfg := Worker{
		RabbitMQURL: r.required("RABBITMQ_URL"),
		LogLevel:    r.logLevel("LOG_LEVEL", slog.LevelInfo),
	}
	failRate := r.float("WORKER_FAIL_RATE", 0)
	minDelayMS := r.int("WORKER_MIN_DELAY_MS", 500)
	maxDelayMS := r.int("WORKER_MAX_DELAY_MS", 2000)
	if err := r.err(); err != nil {
		return Worker{}, err
	}

	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.Float64Var(&failRate, "fail-rate", failRate, "probability in [0,1] that a task fails (env WORKER_FAIL_RATE)")
	fs.IntVar(&minDelayMS, "min-delay-ms", minDelayMS, "minimum delay per task step in ms (env WORKER_MIN_DELAY_MS)")
	fs.IntVar(&maxDelayMS, "max-delay-ms", maxDelayMS, "maximum delay per task step in ms (env WORKER_MAX_DELAY_MS)")
	if err := fs.Parse(args); err != nil {
		return Worker{}, err
	}

	var errs []error
	if failRate < 0 || failRate > 1 {
		errs = append(errs, fmt.Errorf("fail-rate must be within [0,1], got %v", failRate))
	}
	if minDelayMS < 0 || maxDelayMS < 0 {
		errs = append(errs, errors.New("delays must not be negative"))
	}
	if minDelayMS > maxDelayMS {
		errs = append(errs, fmt.Errorf("min-delay-ms (%d) must not exceed max-delay-ms (%d)", minDelayMS, maxDelayMS))
	}
	if err := errors.Join(errs...); err != nil {
		return Worker{}, err
	}

	cfg.Simulation = Simulation{
		FailRate: failRate,
		MinDelay: time.Duration(minDelayMS) * time.Millisecond,
		MaxDelay: time.Duration(maxDelayMS) * time.Millisecond,
	}
	return cfg, nil
}

// reader accumulates parse errors so all problems are reported at once.
type reader struct {
	getenv Getenv
	errs   []error
}

func (r *reader) err() error { return errors.Join(r.errs...) }

func (r *reader) string(key, def string) string {
	if v := r.getenv(key); v != "" {
		return v
	}
	return def
}

func (r *reader) required(key string) string {
	v := r.getenv(key)
	if v == "" {
		r.errs = append(r.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		r.errs = append(r.errs, fmt.Errorf("%s must be a positive duration, got %q", key, v))
		return def
	}
	return d
}

func (r *reader) int(key string, def int) int {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be an integer, got %q", key, v))
		return def
	}
	return n
}

func (r *reader) float(key string, def float64) float64 {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be a number, got %q", key, v))
		return def
	}
	return f
}

func (r *reader) logLevel(key string, def slog.Level) slog.Level {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s must be one of debug, info, warn, error; got %q", key, v))
		return def
	}
	return l
}
