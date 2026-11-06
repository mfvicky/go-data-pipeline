#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
printf '\n[1/6] Formatting\n'
if [[ -n "$(gofmt -l .)" ]]; then echo 'Go files need formatting' >&2; exit 1; fi
printf '\n[2/6] Vet\n'
go vet ./...
printf '\n[3/6] Unit and integration tests\n'
go test ./... -count=1
printf '\n[4/6] Race detector\n'
CGO_ENABLED=1 go test -race ./... -count=1
printf '\n[5/6] Build\n'
go build ./...
printf '\n[6/6] Lint\n'
if command -v golangci-lint >/dev/null; then
  golangci-lint run ./...
else
  echo 'SKIPPED: golangci-lint is not installed here; run make lint on Windows before commit.'
fi
printf '\nHardening source checks completed.\n'
