# API Execution, Observability and Cancellation Verification

Run these commands in **Git Bash**, from the repository root. This example uses trusted local sample files.

## 1. Start the server

In terminal A, after stopping any container that uses port 8080:

```bash
make run
```

If `API_KEY` is configured, use the same value in terminal B:

```bash
export API_KEY='the-same-key-as-the-server'
```

## 2. Automatic end-to-end smoke test

In terminal B:

```bash
BASE_URL=http://127.0.0.1:8080 bash scripts/smoke.sh
```

Expect `Smoke test passed`. The script checks health, creates a CSV/JSON job, waits for `COMPLETED`, and fetches its aggregation results, errors, and progress.

## 3. Manual endpoint checks

```bash
BASE_URL=http://127.0.0.1:8080
AUTH=()
if [[ -n "${API_KEY:-}" ]]; then AUTH=(-H "X-API-Key: $API_KEY"); fi

curl -i "$BASE_URL/health"

curl -i -X POST "$BASE_URL/api/v1/pipelines" \
  "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"sources":[{"name":"csv","type":"csv","endpoint":"testdata/input/customers.csv"}],"validation_rules":{"required_fields":["id"]}}'
```

Copy the returned `job_id` into `JOB_ID`, then run:

```bash
JOB_ID='replace-with-actual-job-id'
curl -i "${AUTH[@]}" "$BASE_URL/api/v1/pipelines/$JOB_ID/progress"
curl -i "${AUTH[@]}" "$BASE_URL/api/v1/pipelines/$JOB_ID/results"
curl -i "${AUTH[@]}" "$BASE_URL/api/v1/pipelines/$JOB_ID/errors"
curl -i -X POST "${AUTH[@]}" "$BASE_URL/api/v1/pipelines/$JOB_ID/cancel"
```

Expected results:

| Action | Expected response |
| --- | --- |
| Create | `201`, state initially `PENDING` |
| Progress | `200`, current state and counters |
| Results while running | `409`, not ready |
| Results after `COMPLETED`/`FAILED`/`CANCELLED` | `200`, records, aggregations, errors, rejected records |
| Errors | `200`, may be empty for valid input |
| Cancel while running | `202`; poll progress until `CANCELLED` |
| Cancel after completion | `409` |
| Unknown job | `404` |
| Delete running job | `409`; delete after terminal state returns `204` |

Most local CSV jobs finish quickly. To reliably test cancellation, timeout, malformed records, and multiple concurrent jobs, run the automated Go tests below.

## 4. Automated tests

```bash
make test
make test-race
make lint
make check

go test ./internal/api -run 'TestAPI' -v
go test ./internal/pipeline -run 'TestRunObserver' -v
```

On Windows Git Bash, `make test-race` requires MSYS2 UCRT64 GCC and `CGO_ENABLED=1`. Set `PATH` for UCRT64 if necessary.

## Known boundaries

Jobs and results are in memory and are lost when the server restarts. `records_processed` counts successful transformed records, and `records_failed` counts validation/transformation rejections. Aggregation errors do not automatically reject records. Stage latency represents source reads for ingestion, per-record validation/transformation, batch aggregation, and per-file export. During ingestion the total number of records is unknown; progress percentage becomes meaningful after ingestion finishes.

**Security:** These endpoints execute submitted source URLs and write configured export paths. API-created job exports must stay within `output/`. Keep the service accessible only to trusted clients until connect-time SSRF/redirect hardening and stronger filesystem isolation are implemented.
