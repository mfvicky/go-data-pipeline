# Verification Guide — Current Go Data Pipeline

Use this guide **after** [SETUP.md](SETUP.md). It matches the production-structure project derived from `cmd2.zip` and tests **only functionality currently implemented**.

**Current behavior:** The REST API asynchronously runs pipeline jobs. Progress, results, errors and cancellation routes are implemented. See [Observability Verification](OBSERVABILITY_VERIFICATION.md) for current smoke-test commands.

## 1. Automated project checks

From the repository root in Git Bash:

```bash
cd /go-data-pipeline
go mod tidy
go build ./...
go vet ./...
go test ./... -v -count=1
```

Expected: the implemented packages' tests report `PASS` / `ok`. Packages with no test files are not errors. Run race detection separately if MSYS2 GCC is configured:

```bash
export PATH="/c/msys64/ucrt64/bin:$PATH"
export CGO_ENABLED=1
export CC=gcc
go test -race ./... -count=1
```

Expected: no `WARNING: DATA RACE` messages and no test failures. A successful test run checks only the cases covered by the tests.

## Developer Tooling Verification

### Verify Makefile

```bash
make help
make test
make vet
make lint
make build
make check
```

Expected: All commands complete successfully.

### Verify Git Hooks

```bash
git config --get core.hooksPath
```

Expected:

```text
.githooks
```

### Verify Commit Message Format

Valid examples:

```text
feat: add pipeline worker
fix(api): correct response handling
test: add validation tests
```

Invalid example:

```text
updated code
```

### Verify Race Detection

```bash
make test-race
```

Requires CGO and a compatible GCC compiler.


### Verify Postman Collection


1. Open Postman.
2. Import `examples/postman_collection.json`.
3. Set `baseUrl` and `apiKey`.
4. Start the Go API server.
5. Run the collection requests in order.
6. Verify that all Postman assertions pass.

## 2. Start the server and load API credentials

Start the **local** server in Terminal 1, using the settings from `.env`:

```bash
set -a
source .env
set +a
go run ./cmd/server
```

Or start Docker instead with `docker compose up -d` (do not run both on the same port).

In Terminal 2:

```bash
cd /go-data-pipeline
set -a
source .env
set +a
```

The commands below assume port **8080** and that an `API_KEY` has been set. For the Docker version, `.env` provides the same key used by Compose.

## 3. Verify health, authentication, and Swagger

```bash
curl -i http://localhost:8080/health
curl -i http://localhost:8080/api/v1/pipelines
curl -i -H "X-API-Key: $API_KEY" http://localhost:8080/api/v1/pipelines
curl -i http://localhost:8080/openapi.yaml
```

| Check | Expected |
| --- | --- |
| `GET /health` | **200**, `Pipeline API is running successfully!` |
| `GET /api/v1/pipelines` without key | **401**, when server has `API_KEY` configured |
| Same request with valid `X-API-Key` | **200**, JSON with `jobs` and `count` |
| `GET /openapi.yaml` | **200**, OpenAPI YAML |
| Open <http://localhost:8080/swagger/> | Interactive documentation if CDN assets are reachable |

On authenticated API responses, look for an `X-Request-ID` header; the server also emits structured request logs. The rate-limiter behavior is tested with `TestRouterRateLimit` rather than by flooding a running server.

## 4. Create a job and inspect its saved state

```bash
curl -i -X POST http://localhost:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: $API_KEY" \
  -d '{"sources":[{"name":"customers_csv","type":"csv","endpoint":"testdata/input/customers.csv"}]}'
```

Expected: **201 Created** and a JSON body containing a newly generated `job_id`, `state: "PENDING"`, and a message indicating asynchronous execution was accepted. Copy the real ID from your response:

```bash
JOB_ID='PASTE_YOUR_JOB_ID_HERE'
curl -i -H "X-API-Key: $API_KEY" http://localhost:8080/api/v1/pipelines
curl -i -H "X-API-Key: $API_KEY" "http://localhost:8080/api/v1/pipelines/$JOB_ID"
```

Expected: **200** for list and details, with the job present. `records_processed` increases as the job runs; poll `/api/v1/pipelines/$JOB_ID/progress` for `COMPLETED`.

### Invalid request test

```bash
curl -i -X POST http://localhost:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: $API_KEY" \
  -d '{}'
```

Expected: **400 Bad Request**, error `sources must contain at least one source`.

### Source path restriction test

```bash
curl -i -X POST http://localhost:8080/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: $API_KEY" \
  -d '{"sources":[{"type":"csv","endpoint":"../outside.csv"}]}'
```

Expected: **400** because the source is not an allowed regular file inside `INPUT_DIR`. The exact error may differ depending on whether the file exists.

### Delete and verify removal

Wait until `/progress` reports `COMPLETED`, `FAILED`, or `CANCELLED` before deleting the job. Pending/running jobs cannot be deleted.

```bash
curl -i -X DELETE -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/pipelines/$JOB_ID"
curl -i -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/pipelines/$JOB_ID"
```

Expected: **204 No Content**, then **404 Not Found**.

## 5. Verify CSV/JSON/API readers (Go CLI)

The following commands run in Git Bash from the **project source directory**, not inside Postman. They work even when the API server is stopped.

### CSV input

```bash
go run ./cmd/ingest-demo -type csv -endpoint testdata/input/customers.csv
```

Expected: **`read 3 records`**.

### JSON input

```bash
go run ./cmd/ingest-demo -type json -endpoint testdata/input/customers.json
```

Expected: **`read 2 records`**.

### Nested JSON input

```bash
go run ./cmd/ingest-demo -type json -endpoint testdata/input/nested.json -record-path data.records
```

Expected: **`read 2 records`**.

### External HTTP API

```bash
go run ./cmd/ingest-demo -type api \
  -endpoint 'https://jsonplaceholder.typicode.com/posts' \
  -timeout 60s > post.txt
```

Expected: **`read 100 records`** on stderr and JSON records written to `post.txt`, **if** the remote service is reachable. If it times out, check access first:

```bash
curl -I --max-time 30 'https://jsonplaceholder.typicode.com/posts'
```

An external-network failure is not proof that local CSV/JSON ingestion is broken. `post.txt` is a generated file and is ignored by `.gitignore`.

## 6. Verify validation and transformation independently

```bash
go test ./internal/validation -v -count=1
go test ./internal/transformation -v -count=1
```

Expected tests include:

- Validation: `TestCheck`, `TestFilter`, `TestExistingCSV`, `TestInvalidConfiguration`.
- Transformation: `TestApply`, `TestInvalidConversion`, `TestInvalidConfiguration`, `TestExistingCSV`.

These verify required fields, types, numeric rules, invalid-record filtering, string normalization, and type conversions.

## 7. Verify worker pools and the connected pipeline

```bash
go test ./internal/pipeline -v -count=1
```

Run focused tests:

```bash
go test ./internal/pipeline -run '^TestRunCSVAndJSON$' -v -count=1
go test ./internal/pipeline -run '^TestRunFiltersInvalidRecords$' -v -count=1
go test ./internal/pipeline -run '^TestTransformationWorkerError$' -v -count=1
go test ./internal/pipeline -run '^TestWorkersProcessConcurrently$' -v -count=1
```

Expected from the sample integration test: **5 valid processed records** (3 CSV + 2 JSON), **0 errors**, and cities `bengaluru:1`, `delhi:3`, `mumbai:1`. The invalid-record test expects **2 processed** and **2 errors**. Record order is not guaranteed under concurrent processing.

### Cancellation and timeout

```bash
go test ./internal/pipeline \
  -run '^TestRunCancellationStopsHTTPReader$' -v -count=1

go test ./internal/pipeline \
  -run '^TestRunTimeoutStopsHTTPReader$' -v -count=1
```

Expected: both tests **PASS**. They use a test HTTP server and do not depend on a public API being reachable.

## 8. Postman verification

1. Create a collection `Go Data Pipeline` and a collection variable `base_url` = `http://localhost:8080`.
2. Add `api_key` containing the same value as `.env` `API_KEY` (handle it as a secret).
3. Create `GET {{base_url}}/health`: expect **200**.
4. Create `POST {{base_url}}/api/v1/pipelines`: Header `X-API-Key: {{api_key}}`, Body → raw → JSON:

   ```json
   {
     "sources": [
       {
         "name": "customers_csv",
         "type": "csv",
         "endpoint": "testdata/input/customers.csv"
       }
     ]
   }
   ```

   Expect **201** with `PENDING`. In **Scripts → Post-response**, save the ID:

   ```javascript
   if (pm.response.code === 201) {
     pm.collectionVariables.set('job_id', pm.response.json().job_id);
   }
   ```

5. Create `GET {{base_url}}/api/v1/pipelines`: expect **200**, provide `X-API-Key`.
6. Create `GET {{base_url}}/api/v1/pipelines/{{job_id}}`: expect **200**, provide `X-API-Key`.
7. Poll `GET {{base_url}}/api/v1/pipelines/{{job_id}}/progress` until the job is finished. Then create `DELETE {{base_url}}/api/v1/pipelines/{{job_id}}`: expect **204**, provide `X-API-Key`.
8. Repeat the GET-by-ID: expect **404**.
9. Repeat the list request **without** `X-API-Key`: expect **401** when authentication is configured.

For Swagger, visit <http://localhost:8080/swagger/> and use the **Authorize** control for the protected operations.

## 9. Verification checklist

| Area | How to verify | Success criteria |
| --- | --- | --- |
| Build | `go build ./...` | No errors |
| Vet | `go vet ./...` | No diagnostics |
| Tests | `go test ./... -count=1` | All pass |
| Race detector | `go test -race ./...` | No detected races |
| Health | `GET /health` | 200 |
| Authentication | `GET /api/v1/pipelines`, with/without key | 200 / 401 |
| Create job | `POST /api/v1/pipelines` | 201, `PENDING` |
| List/details/delete | GET/DELETE requests | 200 / 204 / 404 |
| Swagger | `/swagger/`, `/openapi.yaml` | UI accessible / 200 |
| CSV/JSON ingestion | CLI | 3 / 2 records |
| Nested JSON ingestion | CLI | 2 records |
| HTTP ingestion | CLI | 100 records if service reachable |
| Validation/transformation | Package tests | Pass |
| Worker pools | Pipeline tests | Pass |
| Cancellation/timeout | Pipeline tests | Pass |
| Docker | `docker compose ps` + health | Service running and healthy; local verification required |

## 10. Known limitations — not failures

- `POST /api/v1/pipelines` starts asynchronous processing; poll `/progress` to see terminal state.
- `/api/v1/pipelines/{id}/progress`, `/results`, `/errors`, and `/cancel` are implemented; see the current verification guide below.
- Aggregation and file export are implemented in `pipeline.Run()` and API jobs.
- Stored jobs are in memory and disappear on restart.
- API source checks do not yet provide full connect-time protections against DNS rebinding/redirect-based SSRF. Do not expose untrusted job execution publicly without further hardening.
- Docker image build/runtime was **not verified in the development environment**; you need to run the Docker checks on your machine.

For configuration and Docker troubleshooting, see [SETUP.md](SETUP.md).

> For a complete current API verification flow, use [OBSERVABILITY_VERIFICATION.md](OBSERVABILITY_VERIFICATION.md).
