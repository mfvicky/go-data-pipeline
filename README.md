# Go Data Processing Pipeline

Concurrent CSV/JSON/HTTP ingestion, record validation, transformation, aggregation and CSV/JSON export, and a REST API for storing pipeline jobs. Uses Go 1.23+ and `net/http` without a web framework.

**Current behavior:** `POST /api/v1/pipelines` creates a job and launches the pipeline asynchronously. Poll the progress endpoint until `COMPLETED`, `FAILED`, or `CANCELLED`. Job results and errors are stored in memory only.

## Requirements

- Go 1.23 or newer (Go 1.27 is supported).
- Optional: Docker with Compose.
- GCC/CGO only for `go test -race ./...` on Windows.

## Run locally

From the project root:

```bash
go mod tidy
go run ./cmd/server
```

The default server listens only on `127.0.0.1:8080`. For authenticated local testing:

```bash
export API_KEY='replace-with-a-long-random-secret'
go run ./cmd/server
```

Using the same Git Bash terminal to set environment variables and start the server is important. Environment changes in one shell do not apply to other shells.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `APP_ENV` | `development` | `development` or `production` |
| `HOST` | `127.0.0.1` | Listener address |
| `PORT` | `8080` | Listener port |
| `API_KEY` | empty | `X-API-Key` value; required in production or with non-loopback `HOST` |
| `INPUT_DIR` | `testdata/input` | Directory from which API job specs can reference local files |
| `ALLOW_PRIVATE_SOURCES` | `true` in development, `false` in production | Whether job specs may reference private network URLs |
| `RATE_LIMIT_PER_MINUTE` | `120` | Request limit per remote IP for API endpoints |
| `READ_HEADER_TIMEOUT` | `5s` | Header timeout |
| `READ_TIMEOUT` | `15s` | Request timeout |
| `WRITE_TIMEOUT` | `30s` | Response timeout |
| `IDLE_TIMEOUT` | `60s` | Keep-alive timeout |
| `SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown deadline |

See `.env.example`. The Go application reads *environment variables*, not `.env` files automatically. Load settings using your shell, container environment or deployment manager. Do not commit API keys.

## Aggregation and export

`pipeline.Run(ctx, jobID, spec)` now executes `aggregations` and `exports` from `JobSpec` after validation and transformation. Aggregation supports `count`, `sum`, and `avg`; `group_by` accepts multiple field names. Numeric fields can be JSON numbers or numeric CSV strings. Bad numeric/group values are added to `RunResult.Errors` for the aggregation stage and excluded from that aggregation. Ungrouped empty aggregates return value and count of zero; grouped empty aggregates produce no rows.

`RunResult.Aggregations` holds results (`name`, `operation`, optional `field` and `group`, `value`, `count`). A JSON export contains `job_id`, `records`, `aggregations`, and `errors`. A CSV export writes the transformed records, with metadata columns `record_id`, `source`, and `timestamp` followed by sorted data columns. Use `exports` with explicit paths; the legacy `export_targets` field does not create files.

`examples/create_job.json` shows the configuration. Verify execution and generated files through `go test ./internal/pipeline -run TestRunAggregationAndExports -v`. **The HTTP POST endpoint now runs these stages asynchronously.** The HTTP API confines export paths to `output/`. Use trusted source URLs until connect-time HTTP DNS-rebinding and redirect checks are implemented.

## HTTP API

| Method | Path | Status |
| --- | --- | --- |
| GET | `/health` | Available, public |
| GET | `/openapi.yaml` | Available, public |
| GET | `/swagger/` | Available, public |
| POST | `/api/v1/pipelines` | Creates and starts an asynchronous job |
| GET | `/api/v1/pipelines/{id}/progress` | Status, counters, throughput and stage latencies |
| GET | `/api/v1/pipelines/{id}/results` | Records, aggregations, errors, failed records (terminal jobs only) |
| GET | `/api/v1/pipelines/{id}/errors` | Live failures and failed records |
| POST | `/api/v1/pipelines/{id}/cancel` | Requests cancellation (202); poll progress |
| GET | `/api/v1/pipelines` | Available; lists saved jobs |
| GET | `/api/v1/pipelines/{id}` | Available; retrieves job details |
| DELETE | `/api/v1/pipelines/{id}` | Available; deletes non-running jobs |

All `/api/v1/pipelines` routes require `X-API-Key` **when the server has `API_KEY` set**. Production requires it. Health and documentation routes are public.

### Example requests

```bash
curl -i http://localhost:8080/health
curl -i http://localhost:8080/openapi.yaml

curl -i -X POST http://localhost:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: ${API_KEY}" \
  -d '{"sources":[{"name":"customers_csv","type":"csv","endpoint":"testdata/input/customers.csv"}]}'

curl -i -H "X-API-Key: ${API_KEY}" http://localhost:8080/api/v1/pipelines
curl -i -H "X-API-Key: ${API_KEY}" http://localhost:8080/api/v1/pipelines/YOUR_JOB_ID
curl -i -X DELETE -H "X-API-Key: ${API_KEY}" http://localhost:8080/api/v1/pipelines/YOUR_JOB_ID
```

Expected create status: **201**, initially `PENDING`. The job then transitions through `RUNNING` to `COMPLETED`/`FAILED`/`CANCELLED`. Results return **409** until the job finishes. List/details: **200**, delete: **204** for finished jobs, missing job: **404**, invalid spec: **400**, wrong API key: **401**, throttled: **429**.

## Swagger / OpenAPI

Open [http://localhost:8080/swagger/](http://localhost:8080/swagger/) and use **Authorize** to supply an API key if configured. The OpenAPI 3 specification is at `docs/openapi.yaml` and served at `/openapi.yaml`. Swagger UI uses version-pinned assets from jsDelivr, so its interactive page needs browser access to that CDN; the YAML endpoint works offline.

## CSV / JSON / API ingestion CLI

```bash
go run ./cmd/ingest-demo -type csv -endpoint testdata/input/customers.csv
go run ./cmd/ingest-demo -type json -endpoint testdata/input/customers.json
go run ./cmd/ingest-demo -type json -endpoint testdata/input/nested.json -record-path data.records
go run ./cmd/ingest-demo -type api -endpoint https://jsonplaceholder.typicode.com/posts -timeout 60s
```

The sample CSV contains 3 records, the customer JSON contains 2 records, and nested JSON contains 2 records. Remote HTTP may fail if the service or network is unavailable.

## Docker

From the project root, create a local `.env` file containing a strong random `API_KEY`. In Git Bash (with OpenSSL):

```bash
printf 'API_KEY=%s\n' "$(openssl rand -hex 32)" > .env
docker compose up --build -d
docker compose ps
curl -i http://localhost:8080/health
```

Read your API key from the `.env` file for authenticated requests. Stop the service with `docker compose down`. Compose maps container port 8080 to your machine's loopback address only. The container is non-root, read-only and has its input dataset packaged under `/app/testdata/input`.

The image uses a multi-stage Go build and a minimal distroless runtime. A working Docker engine is required; CI currently checks Go code, not Docker image builds.

## Progress, results, errors and cancellation

`POST /api/v1/pipelines` runs the job in the background. Status starts as `PENDING` and may immediately transition to `RUNNING`. `GET /api/v1/pipelines/{id}/progress` returns `records_read`, `records_processed`, `records_failed`, `records_pending`, `error_count`, `rate_per_second`, `percent_complete`, and `stage_latencies` (count, total milliseconds and average milliseconds per stage). Ingestion latency is measured per **source**, validation/transformation per **record**, aggregation per **batch**, and export per **file**. `records_processed` counts transformed records that passed validation/transformation; numeric aggregation errors are counted separately without rejecting those records.

`GET /api/v1/pipelines/{id}/results` returns data only after a terminal state; before then it responds with `409`. The `errors` endpoint works while running and includes the original record when available. The `cancel` endpoint responds `202` when cancellation is requested; poll progress until it becomes `CANCELLED`. Finished jobs return `409` to cancellation requests. Partial results remain queryable for failed/cancelled jobs. Unexpected source failures are collected while other sources continue. A nonfatal record rejection does not cause the whole job to fail.

### Smoke verification

With the server running (`make run`) and optional `API_KEY` configured, run in a **second** Git Bash terminal:

```bash
BASE_URL=http://127.0.0.1:8080 bash scripts/smoke.sh
```

The smoke script submits CSV/JSON input, polls progress, reads results and errors, and checks both completed output and failed-record collection. `API_KEY` can be supplied in the testing terminal. See [Observability Verification](docs/OBSERVABILITY_VERIFICATION.md) for manual cURL commands and expected codes.

## Tests and development checks

```bash
go fmt ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go build ./...
```

`go test -race` needs CGO and a C compiler. On Windows Git Bash with GCC from MSYS2 UCRT64:

```bash
export PATH="/c/msys64/ucrt64/bin:$PATH"
export CGO_ENABLED=1
export CC=gcc
go test -race ./...
```

CI runs formatting checks, vet, unit/integration tests, race detection and builds on Linux.

## Developer Tooling


This project uses a Makefile, golangci-lint, Git hooks, and Postman for development and testing.


### Makefile Commands


| Command | Description |
|---|---|
| `make help` | List available commands |
| `make run` | Start the API server |
| `make fmt` | Format Go code |
| `make fmt-check` | Check formatting without changing files |
| `make vet` | Run Go static analysis |
| `make lint` | Run golangci-lint |
| `make test` | Run all tests |
| `make test-race` | Check for data races |
| `make build` | Build the project |
| `make check` | Run formatting, vet, lint, tests, and build |
| `make ci` | Run all checks including race tests |
| `make hooks` | Enable local Git hooks |


### Git Hooks

Enable hooks once per repository:

```bash
make hooks
```
The pre-commit hook checks formatting, vet, lint, and tests.

Commit messages follow Conventional Commits:

```bash
git commit -m "feat(pipeline): add worker pools"
git commit -m "fix(api): handle invalid requests"
git commit -m "test(validation): add validation tests"
```

### Postman

Import `examples/postman_collection.json` into Postman.

Collection variables:
- `baseUrl`: `http://localhost:8080`
- `apiKey`: API_KEY from your environment
- `jobId`: Automatically populated when creating a job


For detailed setup and verification instructions, see:
- [Setup Guide](docs/SETUP.md)
- [Verification Guide](docs/VERIFICATION.md)


## Architecture

```text
HTTP client -> middleware -> handlers -> in-memory job repository

Source readers -> recordsCh -> validation workers -> transformation workers
                                |                     |
                                +----- errorsCh ------+
```

See [architecture details](docs/architecture.md), [design trade-offs](docs/design-decisions.md), and the original [architecture illustration](docs/go-pipeline-architecture.png).

## Security boundaries and limitations

- Do not expose the API on the internet without TLS termination, access controls, and appropriate network restrictions. API-key authentication alone is not a complete identity system.
- The source policy checks job specifications at submission time. **Do not connect untrusted jobs to outbound HTTP execution until redirect and DNS-rebinding-safe checks are enforced at connection time.**
- File source paths are constrained at submission time to `INPUT_DIR`; future execution should recheck opened files to avoid filesystem races.
- Authentication is required on non-loopback listeners; development defaults work on local loopback only.
- Rate limiting is in-memory and per process, based on the direct connection IP. Use a gateway for distributed limits or trusted proxy handling.
- Jobs, progress, errors and results are kept in memory and will be lost after restart; no database or durable job queue is implemented.
- HTTP jobs execute user-configured source URLs. API export paths are restricted to `output/`, but stronger OS-level sandboxing and outbound HTTP redirect/DNS rebinding protections are still pending.
