# Queuemaxxing - convenience targets.
#
# Nothing here is required: every target is a thin wrapper over plain `go`
# commands that work on any platform with a Go toolchain and no `make`.
# Run `make help` for the list.

GO         ?= go
PORT       ?= 8080
MODE       ?= FIFO
DATA       ?= ./data/queue.wal

# The demo targets deliberately use a throwaway port and WAL so they never
# disturb a server you already have running on $(PORT).
DEMO_PORT  ?= 8123
DEMO_WAL   ?= ./data/demo.wal
BIN        ?= ./bin

.DEFAULT_GOAL := help
.PHONY: help run console test race cover vet fmt build demo demo-lifo fresh clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

run: ## Start the server (web console on http://localhost:$(PORT))
	$(GO) run ./cmd/server -port $(PORT) -mode $(MODE) -data $(DATA)

console: ## Print the console URL
	@echo "http://localhost:$(PORT)"

test: ## Run the full test suite
	$(GO) test ./...

race: ## Run the full test suite under the race detector
	$(GO) test -race ./...

cover: ## Run tests with a coverage summary
	$(GO) test -cover ./...

vet: ## Run go vet
	$(GO) vet ./...

fmt: ## Check formatting (prints offending files; empty output means clean)
	@gofmt -l ./cmd ./pkg ./test

build: ## Build the server and worker into $(BIN)
	$(GO) build -o $(BIN)/queuemaxxing-server ./cmd/server
	$(GO) build -o $(BIN)/queuemaxxing-client ./cmd/client

# demo brings up a throwaway server on an empty WAL, runs the worker against
# it, and tears the server down again - one command, no leftover state, and
# no interference with a server already running on $(PORT).
demo: build ## Run the checkout worker end to end against a throwaway server
	@rm -f $(DEMO_WAL)
	@$(BIN)/queuemaxxing-server -port $(DEMO_PORT) -mode $(MODE) -data $(DEMO_WAL) >/dev/null 2>&1 & \
		SRV=$$!; \
		trap 'kill $$SRV 2>/dev/null || true' EXIT INT TERM; \
		until curl -fsS http://127.0.0.1:$(DEMO_PORT)/health >/dev/null 2>&1; do sleep 0.2; done; \
		$(BIN)/queuemaxxing-client -url http://127.0.0.1:$(DEMO_PORT)

demo-lifo: ## Same demo against a LIFO server (the tie-break flips)
	@$(MAKE) demo MODE=LIFO

fresh: ## Delete the WAL so the next start comes up on an empty queue
	rm -f $(DATA) $(DEMO_WAL)

clean: ## Remove build output and throwaway data
	rm -rf $(BIN) $(DEMO_WAL)
