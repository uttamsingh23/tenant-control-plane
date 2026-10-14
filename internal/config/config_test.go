package config

import (
	"errors"
	"flag"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) Getenv {
	return func(k string) string { return m[k] }
}

func TestLoadAPI_Defaults(t *testing.T) {
	cfg, err := LoadAPI(env(map[string]string{
		"DATABASE_URL": "postgres://u:p@localhost/db",
		"RABBITMQ_URL": "amqp://u:p@localhost:5672/",
	}))
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.HTTP.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 15s", cfg.HTTP.ShutdownTimeout)
	}
	if cfg.HTTP.RequestTimeout != 10*time.Second {
		t.Errorf("RequestTimeout = %v, want 10s", cfg.HTTP.RequestTimeout)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	wantOutbox := Outbox{
		PollInterval: 500 * time.Millisecond, BatchSize: 100, Lease: 30 * time.Second,
		PublishTimeout: 5 * time.Second, RetryMin: 500 * time.Millisecond, RetryMax: 30 * time.Second,
	}
	if cfg.Outbox != wantOutbox {
		t.Errorf("Outbox = %+v, want %+v", cfg.Outbox, wantOutbox)
	}
}

func TestLoadAPI_OutboxValidation(t *testing.T) {
	_, err := LoadAPI(env(map[string]string{
		"DATABASE_URL":           "postgres://u:p@localhost/db",
		"RABBITMQ_URL":           "amqp://u:p@localhost:5672/",
		"OUTBOX_BATCH_SIZE":      "0",
		"OUTBOX_LEASE":           "10s",
		"OUTBOX_PUBLISH_TIMEOUT": "5s",
		"OUTBOX_RETRY_MIN":       "1m",
		"OUTBOX_RETRY_MAX":       "1s",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"OUTBOX_BATCH_SIZE", "OUTBOX_LEASE", "OUTBOX_RETRY_MIN"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadAPI_RequestTimeoutBelowWriteTimeout(t *testing.T) {
	_, err := LoadAPI(env(map[string]string{
		"DATABASE_URL":         "postgres://u:p@localhost/db",
		"RABBITMQ_URL":         "amqp://u:p@localhost:5672/",
		"HTTP_REQUEST_TIMEOUT": "20s",
		"HTTP_WRITE_TIMEOUT":   "15s",
	}))
	if err == nil || !strings.Contains(err.Error(), "HTTP_REQUEST_TIMEOUT") {
		t.Fatalf("err = %v, want one mentioning HTTP_REQUEST_TIMEOUT", err)
	}
}

func TestLoadAPI_ReportsAllErrors(t *testing.T) {
	_, err := LoadAPI(env(map[string]string{
		"HTTP_SHUTDOWN_TIMEOUT": "soon",
		"LOG_LEVEL":             "loud",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "RABBITMQ_URL", "HTTP_SHUTDOWN_TIMEOUT", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadWorker_FlagsOverrideEnv(t *testing.T) {
	cfg, err := LoadWorker(
		[]string{"--fail-rate=1", "--max-delay-ms=50"},
		env(map[string]string{
			"RABBITMQ_URL":        "amqp://u:p@localhost:5672/",
			"WORKER_FAIL_RATE":    "0.2",
			"WORKER_MIN_DELAY_MS": "10",
		}),
	)
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}
	want := Simulation{FailRate: 1, MinDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}
	if cfg.Simulation != want {
		t.Errorf("Simulation = %+v, want %+v", cfg.Simulation, want)
	}
}

func TestLoadWorker_Defaults(t *testing.T) {
	cfg, err := LoadWorker(nil, env(map[string]string{"RABBITMQ_URL": "amqp://localhost/"}))
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}
	want := Simulation{FailRate: 0, MinDelay: 500 * time.Millisecond, MaxDelay: 2 * time.Second}
	if cfg.Simulation != want {
		t.Errorf("Simulation = %+v, want %+v", cfg.Simulation, want)
	}
	if cfg.Seed != 0 || cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("Seed, ShutdownTimeout = %d, %v; want 0, 10s", cfg.Seed, cfg.ShutdownTimeout)
	}
}

// TestLoadWorker_DelayConfiguration: delays are given in milliseconds from
// env or flags, and both bounds may be equal (a fixed delay) or zero.
func TestLoadWorker_DelayConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		args     []string
		min, max time.Duration
	}{
		{"env", map[string]string{"WORKER_MIN_DELAY_MS": "10", "WORKER_MAX_DELAY_MS": "20"}, nil, 10 * time.Millisecond, 20 * time.Millisecond},
		{"flags", nil, []string{"--min-delay-ms=30", "--max-delay-ms=40"}, 30 * time.Millisecond, 40 * time.Millisecond},
		{"flag overrides one env value", map[string]string{"WORKER_MIN_DELAY_MS": "10", "WORKER_MAX_DELAY_MS": "20"},
			[]string{"--max-delay-ms=50"}, 10 * time.Millisecond, 50 * time.Millisecond},
		{"fixed delay", nil, []string{"--min-delay-ms=25", "--max-delay-ms=25"}, 25 * time.Millisecond, 25 * time.Millisecond},
		{"no delay", nil, []string{"--min-delay-ms=0", "--max-delay-ms=0"}, 0, 0},
		{"upper limit", nil, []string{"--min-delay-ms=0", "--max-delay-ms=3600000"}, 0, MaxWorkerDelay},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{"RABBITMQ_URL": "amqp://localhost/"}
			for k, v := range tt.env {
				vars[k] = v
			}
			cfg, err := LoadWorker(tt.args, env(vars))
			if err != nil {
				t.Fatalf("LoadWorker: %v", err)
			}
			if cfg.Simulation.MinDelay != tt.min || cfg.Simulation.MaxDelay != tt.max {
				t.Errorf("delays = [%v, %v], want [%v, %v]", cfg.Simulation.MinDelay, cfg.Simulation.MaxDelay, tt.min, tt.max)
			}
		})
	}
}

func TestLoadWorker_SeedAndShutdownTimeout(t *testing.T) {
	vars := env(map[string]string{
		"RABBITMQ_URL":            "amqp://localhost/",
		"WORKER_SEED":             "7",
		"WORKER_SHUTDOWN_TIMEOUT": "3s",
	})
	cfg, err := LoadWorker(nil, vars)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Seed != 7 || cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("from env: Seed, ShutdownTimeout = %d, %v; want 7, 3s", cfg.Seed, cfg.ShutdownTimeout)
	}
	cfg, err = LoadWorker([]string{"--seed=42", "--shutdown-timeout=0s"}, vars)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Seed != 42 || cfg.ShutdownTimeout != 0 {
		t.Errorf("from flags: Seed, ShutdownTimeout = %d, %v; want 42, 0s", cfg.Seed, cfg.ShutdownTimeout)
	}
}

func TestLoadWorker_Validation(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		args []string
	}{
		{"fail rate above 1", nil, []string{"--fail-rate=1.5"}},
		{"fail rate below 0", nil, []string{"--fail-rate=-0.1"}},
		{"fail rate NaN", nil, []string{"--fail-rate=NaN"}},
		{"fail rate not a number", map[string]string{"WORKER_FAIL_RATE": "often"}, nil},
		{"negative min delay", nil, []string{"--min-delay-ms=-1"}},
		{"negative max delay", nil, []string{"--min-delay-ms=0", "--max-delay-ms=-1"}},
		{"min above max", nil, []string{"--min-delay-ms=100", "--max-delay-ms=10"}},
		{"delay above limit", nil, []string{"--max-delay-ms=3600001"}},
		{"delay not an integer", map[string]string{"WORKER_MAX_DELAY_MS": "1s"}, nil},
		{"negative seed", map[string]string{"WORKER_SEED": "-1"}, nil},
		{"negative shutdown timeout", nil, []string{"--shutdown-timeout=-1s"}},
		{"unknown flag", nil, []string{"--fail-rat=1"}},
		{"stray argument", nil, []string{"--fail-rate", "1", "extra"}},
		{"missing broker URL", map[string]string{"RABBITMQ_URL": ""}, nil},
		{"invalid broker URL", map[string]string{"RABBITMQ_URL": "http://localhost/"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{"RABBITMQ_URL": "amqp://localhost/"}
			for k, v := range tt.env {
				vars[k] = v
			}
			if _, err := LoadWorker(tt.args, env(vars)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestLoadWorker_ReportsAllErrors: every invalid value is reported at
// once, not just the first.
func TestLoadWorker_ReportsAllErrors(t *testing.T) {
	_, err := LoadWorker([]string{"--fail-rate=2", "--min-delay-ms=9", "--max-delay-ms=1", "--shutdown-timeout=-1s"},
		env(map[string]string{"RABBITMQ_URL": "localhost"}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"RABBITMQ_URL", "fail-rate", "min-delay-ms (9)", "shutdown-timeout"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadWorker_Help(t *testing.T) {
	_, err := LoadWorker([]string{"-h"}, env(map[string]string{"RABBITMQ_URL": "amqp://localhost/"}))
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
}
