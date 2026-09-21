# dispatch — common tasks. Pure-Go, no external dependencies, no code generation.
#
# Every target that matters is a plain `go` invocation, so you can always read
# the Makefile and run the command yourself instead of trusting the wrapper.

GO       ?= go
PKGS     ?= ./...
BENCHTIME ?= 100ms
RACE      ?= -race

# Versions used to build the container. Pinned so `make docker` is reproducible.
GO_VERSION   ?= 1.25
IMAGE        ?= dispatch:dev

.DEFAULT_GOAL := help

## help: list available targets
.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | awk -F': ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## build: compile every package
.PHONY: build
build:
	$(GO) build $(PKGS)

## test: run the full suite with the race detector
.PHONY: test
test:
	$(GO) test $(RACE) -count=1 $(PKGS)

## test-short: run the suite without the race detector (fast feedback)
.PHONY: test-short
test-short:
	$(GO) test -count=1 $(PKGS)

## test-v: run the full suite verbosely
.PHONY: test-v
test-v:
	$(GO) test $(RACE) -count=1 -v $(PKGS)

## cover: run tests and print per-function coverage
.PHONY: cover
cover:
	$(GO) test $(RACE) -count=1 -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -n 20

## cover-html: open an HTML coverage report in the browser
.PHONY: cover-html
cover-html: cover
	$(GO) tool cover -html=coverage.out

## bench: run every benchmark once with a bounded benchtime
.PHONY: bench
bench:
	$(GO) test -run '^$$' -bench . -benchtime $(BENCHTIME) $(PKGS)

## fmt: rewrite files into gofmt style
.PHONY: fmt
fmt:
	$(GO) fmt $(PKGS)

## fmt-check: fail if any file is not gofmt-clean (CI gate)
.PHONY: fmt-check
fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi
	@echo "gofmt: clean"

## vet: run go vet
.PHONY: vet
vet:
	$(GO) vet $(PKGS)

## staticcheck: run staticcheck if it is installed
.PHONY: staticcheck
staticcheck:
	@command -v staticcheck >/dev/null 2>&1 || { \
		echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"; \
		exit 1; \
	}
	staticcheck $(PKGS)

## golangci: run golangci-lint with the config in .golangci.yml
.PHONY: golangci
golangci:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not installed: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; \
	}
	golangci-lint run $(PKGS)

## lint: every static gate, in one command (the CI entrypoint)
.PHONY: lint
lint: fmt-check vet staticcheck golangci

## check: lint plus the race-detector suite — what CI runs
.PHONY: check
check: lint test

## leak: run only the goroutine-leak tests, verbosely
.PHONY: leak
leak:
	$(GO) test $(RACE) -count=1 -run Leak -v $(PKGS)

## run: start the demo service with its default settings
.PHONY: run
run:
	$(GO) run ./cmd/dispatchd

## run-demo: start the demo with retries and throttling exercised
.PHONY: run-demo
run-demo:
	$(GO) run ./cmd/dispatchd -workers 8 -rate 300 -burst 20 -subjects 3000 -log-level debug

## smoke: build the demo and probe /healthz and /stats once
.PHONY: smoke
smoke:
	$(GO) build -o /tmp/dispatchd ./cmd/dispatchd
	@/tmp/dispatchd -subjects 50 -addr 127.0.0.1:18080 & \
	pid=$$!; \
	trap 'kill $$pid 2>/dev/null' EXIT; \
	for _ in $$(seq 1 50); do \
		curl -fsS http://127.0.0.1:18080/healthz >/dev/null 2>&1 && break; \
		sleep 0.1; \
	done; \
	echo "healthz: $$(curl -fsS http://127.0.0.1:18080/healthz)"; \
	echo "stats:   $$(curl -fsS http://127.0.0.1:18080/stats)"; \
	kill $$pid 2>/dev/null || true

## docker: build the container image
.PHONY: docker
docker:
	docker build --build-arg GO_VERSION=$(GO_VERSION) -t $(IMAGE) .

## docker-run: run the container, publishing the demo port
.PHONY: docker-run
docker-run:
	docker run --rm -p 8080:8080 $(IMAGE)

## compose-up: start the compose stack in the background
.PHONY: compose-up
compose-up:
	docker compose up --build -d

## compose-down: stop the compose stack and remove volumes
.PHONY: compose-down
compose-down:
	docker compose down -v

## clean: remove build and coverage artefacts
.PHONY: clean
clean:
	rm -f coverage.out /tmp/dispatchd
	$(GO) clean -testcache
