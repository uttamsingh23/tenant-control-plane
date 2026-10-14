GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT         := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
GOVULNCHECK_VERSION   ?= v1.1.4
GOVULNCHECK           := go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
WORKER_ARGS           ?=
STRESS_COUNT          ?= 20
RACECHECK_ARGS        ?=

# Load .env (if present) and export it to every recipe. A conditional
# include stops make from auto-creating .env via the rule below.
ifneq (,$(wildcard .env))
include .env
export
endif

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z_.-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

.env: ## Create .env from .env.example if missing
	cp .env.example .env

.PHONY: fmt
fmt: ## Format all Go code
	gofmt -w .
	$(GOLANGCI_LINT) fmt

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not formatted
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then echo "unformatted files:"; echo "$$unformatted"; exit 1; fi

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (includes gosec security checks)
	$(GOLANGCI_LINT) run ./...

.PHONY: vuln
vuln: ## Scan dependencies and the standard library for known vulnerabilities
	$(GOVULNCHECK) ./...

.PHONY: test
test: ## Run the test suite with the race detector
	go test -race -count=1 ./...

.PHONY: stress
stress: ## Repeat every concurrency test STRESS_COUNT times under the race detector
	go test -race -count=$(STRESS_COUNT) -run 'Concurrent' ./...

.PHONY: race-check
race-check: ## Hammer the running stack with concurrent requests (worker at --fail-rate=0)
	go run ./cmd/racecheck $(RACECHECK_ARGS)

.PHONY: cover
cover: ## Run tests with coverage report
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: check
check: fmt-check vet lint vuln test ## Run every quality gate

.PHONY: build
build: ## Build the api and worker binaries into ./bin
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

.PHONY: up
up: .env ## Build and start everything (postgres, rabbitmq, api, worker), wait until healthy
	docker compose up -d --build --wait

.PHONY: deps
deps: .env ## Start only PostgreSQL and RabbitMQ (for run-api / run-worker)
	docker compose up -d --wait postgres rabbitmq

.PHONY: worker
worker: .env ## Recreate the worker container with new flags, e.g. make worker WORKER_ARGS="--fail-rate=1"
	WORKER_ARGS="$(WORKER_ARGS)" docker compose up -d --no-deps --force-recreate worker

.PHONY: ps
ps: ## Show container status and health
	docker compose ps

.PHONY: logs
logs: ## Follow api and worker logs
	docker compose logs -f api worker

.PHONY: smoke
smoke: ## Walk one tenant through create -> active -> PATCH -> DELETE -> destroyed
	scripts/lifecycle.sh

.PHONY: replay
replay: ## Publish duplicate, stale and poison updates to RabbitMQ and check nothing regressed
	scripts/replay.sh

.PHONY: down
down: ## Stop everything (keeps data)
	docker compose down

.PHONY: clean
clean: ## Stop everything and delete all data volumes
	docker compose down -v --remove-orphans
	rm -rf bin coverage.out

.PHONY: run-api
run-api: .env ## Run the control plane as a local process (after make deps)
	go run ./cmd/api

.PHONY: run-worker
run-worker: .env ## Run the worker as a local process (after make deps), e.g. WORKER_ARGS="--fail-rate=1"
	go run ./cmd/worker $(WORKER_ARGS)
