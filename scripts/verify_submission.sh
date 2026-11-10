#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: bash scripts/verify_submission.sh [--clean-clone]"
  echo "  (default)      Verify the current project directory"
  echo "  --clean-clone  Verify the current committed branch from an isolated local clone"
}

if [[ $# -gt 1 ]]; then usage; exit 2; fi
mode="${1:-}"
if [[ -n "$mode" && "$mode" != '--clean-clone' ]]; then usage; exit 2; fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cleanup_dir=''
cleanup() {
  if [[ -n "$cleanup_dir" ]]; then rm -rf "$cleanup_dir"; fi
}
trap cleanup EXIT

if [[ "$mode" == '--clean-clone' ]]; then
  command -v git >/dev/null || { echo 'git is required for --clean-clone' >&2; exit 2; }
  git -C "$project_root" rev-parse --is-inside-work-tree >/dev/null
  if [[ -n "$(git -C "$project_root" status --porcelain)" ]]; then
    echo 'Uncommitted changes detected. Commit the final files before --clean-clone.' >&2
    exit 2
  fi
  branch="$(git -C "$project_root" symbolic-ref -q --short HEAD || true)"
  if [[ -z "$branch" ]]; then
    echo 'Checkout a branch before using --clean-clone.' >&2
    exit 2
  fi
  cleanup_dir="$(mktemp -d)"
  echo "Cloning committed branch '$branch' into a temporary directory..."
  git clone --quiet --no-local --branch "$branch" "$project_root" "$cleanup_dir/project"
  project_root="$cleanup_dir/project"
fi

cd "$project_root"
for file in README.md go.mod Makefile docs/TECHNICAL_REPORT.md             docs/SUBMISSION_CHECKLIST.md docs/openapi.yaml             examples/local_job.json scripts/smoke.sh             internal/api/integration_http_test.go testdata/input/customers.csv; do
  if [[ ! -f "$file" ]]; then echo "Missing required file: $file" >&2; exit 1; fi
done

if command -v git >/dev/null && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if [[ -n "$(git ls-files -- .env)" ]]; then
    echo 'Security failure: .env is tracked in Git. Remove it from the index.' >&2
    exit 1
  fi
  git diff --check
  git diff --cached --check
fi

command -v go >/dev/null || { echo 'Go is required.' >&2; exit 2; }
echo "Go version: $(go version)"

if command -v make >/dev/null; then
  echo '[1/6] Formatting'
  make fmt-check
  echo '[2/6] Vet'
  make vet
  echo '[3/6] Tests'
  make test
  echo '[4/6] Race detector'
  make test-race
  echo '[5/6] Build'
  make build
  echo '[6/6] Lint'
  if command -v golangci-lint >/dev/null; then
    make lint
  else
    echo 'SKIP: golangci-lint not installed. Install it before submission.'
  fi
else
  if [[ "$(go env GOOS)" == windows ]]; then
    echo 'GNU Make is required here to supply writable Windows Go temp/cache paths.' >&2
    exit 2
  fi
  echo '[1/6] Formatting'
  unformatted="$(gofmt -l .)"
  if [[ -n "$unformatted" ]]; then printf 'Unformatted files:\n%s\n' "$unformatted"; exit 1; fi
  echo '[2/6] Vet'
  go vet ./...
  echo '[3/6] Tests'
  go test ./... -count=1
  echo '[4/6] Race detector'
  CGO_ENABLED=1 go test -race ./... -count=1
  echo '[5/6] Build'
  go build ./...
  echo '[6/6] Lint'
  if command -v golangci-lint >/dev/null; then
    golangci-lint run ./...
  else
    echo 'SKIP: golangci-lint not installed. Install it before submission.'
  fi
fi

echo 'Verification passed for the inspected source tree.'
if [[ "$mode" == '--clean-clone' ]]; then
  echo 'Verified committed local branch only; push and remote CI must be checked separately.'
fi
