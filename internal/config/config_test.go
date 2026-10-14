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

func TestLoadWorker_Validation(t *testing.T) {
	base := env(map[string]string{"RABBITMQ_URL": "amqp://localhost/"})
	tests := map[string][]string{
		"fail rate above 1": {"--fail-rate=1.5"},
		"negative delay":    {"--min-delay-ms=-1"},
		"min above max":     {"--min-delay-ms=100", "--max-delay-ms=10"},
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadWorker(args, base); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoadWorker_Help(t *testing.T) {
	_, err := LoadWorker([]string{"-h"}, env(map[string]string{"RABBITMQ_URL": "amqp://localhost/"}))
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
}
