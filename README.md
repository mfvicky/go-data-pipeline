# Golang Data Processing Pipeline

> **Day 1 implementation — job management and source ingestion.** This is not yet an executable end-to-end pipeline. Creating a job stores a `PENDING` configuration; it does **not** launch workers, validate/transform rows, aggregate, export, or update progress. Those features belong to Days 2–3 of the assignment.

## Requirements

- Go **1.23+** (standard library only; no external Go dependencies)
- For the HTTP ingestion example, network access to the requested API

The original scaffold requested Go 1.27.2; the module is set to Go 1.23 so it builds with the available toolchain. Code uses Go 1.22+ `net/http` method/path routing.

## Run the API

From the project root:

```bash
go run ./cmd/server
# or: go build -o bin/server ./cmd/server && ./bin/server
# optional: PORT=8090 go run ./cmd/server
```

The server listens on `:8080` unless `PORT` is set. Try:

```bash
curl http://localhost:8080/health

# Create a job. Response: HTTP 201 with a 32-character job_id and PENDING state.
curl -i -X POST http://localhost:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  --data-binary @examples/create_job.json

# Substitute the returned ID into the following calls:
curl http://localhost:8080/api/v1/pipelines
curl http://localhost:8080/api/v1/pipelines/YOUR_JOB_ID
curl -i -X DELETE http://localhost:8080/api/v1/pipelines/YOUR_JOB_ID
```

Job creation accepts a JSON object with `sources`, optional `validation_rules` / `transform_rules`, `aggregations`, `worker_pool_sizes`, `timeout_seconds`, and `exports`. The original `export_targets` string array is also supported. Configuration is validated and persisted in memory, but **no source is read by POST yet**.

### Day 1 API

| Method | Route | Behavior |
|---|---|---|
| GET | `/health` | HTTP 200, health text |
| POST | `/api/v1/pipelines` | HTTP 201, create a pending job and return ID |
| GET | `/api/v1/pipelines` | HTTP 200, list job-status snapshots |
| GET | `/api/v1/pipelines/{id}` | HTTP 200, return spec + status; 404 if absent |
| DELETE | `/api/v1/pipelines/{id}` | HTTP 204, remove job metadata; 404 if absent; 409 if running |

Not yet present: `GET /{id}/progress`, `GET /{id}/results`, `GET /{id}/errors`, `PATCH /{id}/cancel`. They will be added when the pipeline runner and metrics exist. Jobs are non-running in Day 1.

**Example create response** (ID changes each time):

```json
{
  "job_id": "f22fb29c581b509a9cb7b2f85bf3d254",
  "state": "PENDING",
  "message": "Job saved; pipeline execution is not implemented in Day 1"
}
```

**Example list response** (shows zero work done on Day 1):

```json
{
  "jobs": [
    {
      "job_id": "f22fb29c581b509a9cb7b2f85bf3d254",
      "state": "PENDING",
      "records_processed": 0,
      "records_pending": 0,
      "error_count": 0,
      "percent_complete": 0,
      "created_at": "2026-10-27T05:00:00Z"
    }
  ],
  "count": 1
}
```

Client errors return a JSON object like `{"error":"sources must contain at least one source"}`. Job payloads are limited to 1 MiB; unsupported media types return 415, too-large payloads return 413.

## Try the ingestion readers directly (Day 1)

The readers can already parse all required source types without job execution:

```bash
# CSV with header row; values are strings
go run ./cmd/ingest-demo -type csv -endpoint testdata/input/customers.csv

# JSON array of objects; numbers use json.Number to preserve precision
go run ./cmd/ingest-demo -type json -endpoint testdata/input/customers.json

# Nested JSON response; record_path selects the array
go run ./cmd/ingest-demo -type json -endpoint testdata/input/nested.json -record-path data.records

# External HTTP JSON API; requires an internet connection
go run ./cmd/ingest-demo -type api -endpoint https://jsonplaceholder.typicode.com/posts
```

`internal/ingestion.Read(ctx, source)` returns `[]domain.Record`; `SendToChannel(ctx, source, out)` bridges its output to a typed Go channel for Day 2. Both honor context cancellation. The HTTP reader uses GET, a client timeout of 15 seconds, and optional headers from source configuration (for direct Go callers). File and HTTP inputs are limited to 20 MiB per source; the Day 1 reader loads one source into memory. JSON supports a root object, root array of objects, or a nested object/array selected by a dot-separated `record_path` (e.g. `data.records`). CSV requires unique nonempty headers and equal-length rows.

## Architecture

```text
                         HTTP client
                              |
                     net/http router
                              |
                  REST API job handlers
                              |
                mutex-protected JobRepository
                   (spec + status snapshots)

             ----------------------------------
             Day 1 source readers (independent)
             ----------------------------------
               CSV    JSON    HTTP JSON API
                |       |           |
                +-------+-----------+
                        |
                  []domain.Record
                        |
                 SendToChannel()
                        |
                 chan domain.Record
                        |
           Day 2 planned: validation workers
           Day 2 planned: transform workers
           Day 3 planned: aggregate / export
```

The repository returns deep copies of JSON configuration maps and status records, preventing callers from modifying shared state without a lock. IDs use `crypto/rand`. API reads and deletions are thread-safe. The job repository is intentionally **in-memory**: it does not survive a server restart. Records are read only by direct ingestion calls on Day 1.

## Directory layout

```text
cmd/server/main.go               HTTP service
cmd/ingest-demo/main.go          Direct ingestion demo CLI
internal/api/                    HTTP routing + CRUD handlers
internal/domain/                 Job/record models + spec validation
internal/ingestion/              CSV, JSON, HTTP API readers
internal/pipeline/stage.go       Existing typed channel scaffold
internal/repository/             Safe in-memory job repository
internal/*/*_test.go            Unit, HTTP, concurrency tests
examples/create_job.json        Job specification
testdata/input/                CSV/JSON fixtures (under testdata/input/)
```

## Test and build

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Tests use `httptest.Server` instead of relying on public APIs, so the suite can run offline. They cover accepted/rejected job specs, API lifecycle and HTTP errors, repository copy isolation and concurrent access, CSV/JSON parsing, nested JSON, HTTP API requests, malformed input, and cancellation.

## Day 2–4 backlog

- **Day 2:** orchestrate per-source goroutines, validation/transform worker pools, safe channel ownership, context cancellation and timeouts.
- **Day 3:** aggregation, CSV/JSON exports, centralized errors, progress/metrics, `/progress`, `/results`, `/errors`, and `/cancel`.
- **Day 4:** full integration/cancellation/race coverage, final architecture diagram, expected output artifacts, and one-page design report.

For local development, use trusted paths and HTTP URLs. Before internet-facing deployment, add authentication, source URL/path allowlisting, persistent storage, and stronger operational limits.
