.DEFAULT_GOAL := help

GO ?= go
GOLANGCI_LINT ?= golangci-lint

# Detect the Windows Go toolchain even when Git Bash does not set OS=Windows_NT.
ifeq ($(shell $(GO) env GOOS),windows)
GO_TMP := $(shell cygpath -m "$(HOME)/go-tmp")
GO_CACHE := $(shell cygpath -m "$(HOME)/go-cache")
GO_ENV := GOTMPDIR="$(GO_TMP)" GOCACHE="$(GO_CACHE)" TEMP="$(GO_TMP)" TMP="$(GO_TMP)"
else
GO_ENV :=
endif

.PHONY: help run fmt fmt-check vet lint test test-race build check ci hooks

help:
	@echo "make run        Start the API server"
	@echo "make fmt        Format Go files"
	@echo "make fmt-check  Check Go formatting"
	@echo "make vet        Run go vet"
	@echo "make lint       Run golangci-lint v2"
	@echo "make test       Run all tests"
	@echo "make test-race  Run tests with race detector"
	@echo "make build      Build all Go packages"
	@echo "make check      Run format, vet, lint, tests, and build"
	@echo "make ci         Run check and race tests"
	@echo "make hooks      Enable local Git hooks"

run:
	$(GO_ENV) $(GO) run ./cmd/server

fmt:
	$(GO_ENV) $(GO) fmt ./...

fmt-check:
	@files=$$(gofmt -l .); if [ -n "$$files" ]; then printf 'Run make fmt on:\n%s\n' "$$files"; exit 1; fi

vet:
	$(GO_ENV) $(GO) vet ./...

lint:
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 || { echo "Install golangci-lint v2: https://golangci-lint.run/docs/welcome/install/local/"; exit 1; }
	$(GO_ENV) $(GOLANGCI_LINT) run ./...

test:
	$(GO_ENV) $(GO) test ./... -count=1

test-race:
	$(GO_ENV) CGO_ENABLED=1 $(GO) test -race ./... -count=1

build:
	$(GO_ENV) $(GO) build ./...

check: fmt-check vet lint test build

ci: check test-race

hooks:
	@chmod +x .githooks/pre-commit .githooks/commit-msg
	git config core.hooksPath .githooks
	@echo "Git hooks enabled for this repository."
