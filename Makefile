# gokraft developer Makefile. Run `make help` for the menu.
# Most targets are thin wrappers around go and docker compose so newcomers have
# one obvious command per task.

.DEFAULT_GOAL := help
BIN := bin
CMDS := collector logwriter metricwriter tracewriter queryapi loadgen

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all binaries into ./bin
	@mkdir -p $(BIN)
	@for cmd in $(CMDS); do \
		echo "building $$cmd"; \
		go build -o $(BIN)/$$cmd ./cmd/$$cmd || exit 1; \
	done

.PHONY: test
test: ## Run all unit tests
	go test ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	go test -race ./...

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: fmt
fmt: ## gofmt all code
	gofmt -w cmd internal

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: infra-up
infra-up: ## Start only the infra deps (Kafka, ClickHouse, TimescaleDB, Postgres)
	docker compose up -d kafka clickhouse timescale postgres

.PHONY: up
up: ## Build and start the full stack (infra + gokraft services)
	docker compose up -d --build

.PHONY: down
down: ## Stop the stack (keep volumes)
	docker compose down

.PHONY: clean
clean: ## Stop the stack and remove volumes (wipes stored telemetry)
	docker compose down -v
	rm -rf $(BIN)

.PHONY: loadgen
loadgen: ## Push synthetic traffic at the running collector
	docker compose --profile load up --build loadgen

.PHONY: logs
logs: ## Tail logs of the gokraft services
	docker compose logs -f collector logwriter metricwriter tracewriter queryapi

# --- run individual binaries locally (against infra-up) ---
.PHONY: run-collector run-logwriter run-metricwriter run-tracewriter run-queryapi run-loadgen
run-collector: ## Run the collector locally
	go run ./cmd/collector
run-logwriter: ## Run the log writer locally
	go run ./cmd/logwriter
run-metricwriter: ## Run the metric writer locally
	go run ./cmd/metricwriter
run-tracewriter: ## Run the trace writer locally
	go run ./cmd/tracewriter
run-queryapi: ## Run the query API + dashboard locally
	go run ./cmd/queryapi
run-loadgen: ## Run the load generator locally
	go run ./cmd/loadgen
