# Verification Guide — Current GORM + RabbitMQ Pipeline

Use this after [SETUP.md](SETUP.md), from the current project root.

## Local Go checks (Git Bash)

```bash
make deps
make fmt-check
make vet
make lint
make test
make test-race
make build
make ci
```

All commands must exit successfully. A successful `go test -race ./...` shows no race warning. These checks do **not** prove live RabbitMQ or PostgreSQL operation: the default unit suite can skip tests requiring `TEST_POSTGRES_DSN`.

## Docker and monitoring (PowerShell)

```powershell
docker compose up --build -d
docker compose ps
docker compose logs --tail=60 api rabbitmq postgres
docker compose exec -T rabbitmq rabbitmq-diagnostics -q ping
curl.exe http://localhost:8080/health
curl.exe http://localhost:8080/ready
curl.exe "http://localhost:9090/api/v1/query?query=up"
```

Expected: API, PostgreSQL and RabbitMQ healthy; the Prometheus `job="go-data-pipeline"` series has `up=1`. `/ready` returns `200` when database connectivity is available.

## REST API contract

Import `examples/postman_collection.json` into Postman, set `baseUrl=http://localhost:8080` and `apiKey` to your current credential. Run the ordinary collection (not the separately marked destructive/manual requests). The negative test `03.06` now uses **PATCH** to confirm a completed job returns HTTP 409. Other operations include creating a job (201), reading status/progress/results/errors (200) and querying metrics (200). Test counts may vary as you customize the collection.

For a pending/running job the required cancellation method is:

```powershell
curl.exe -i -X PATCH -H "X-API-Key: <your-key>" http://localhost:8080/api/v1/pipelines/<job-id>/cancel
```

Expected `202` while cancellable, `404` for an unknown job and `409` for a terminal job. Poll `/progress` for `CANCELLED` after an accepted cancellation. `POST /cancel` remains temporarily supported for older clients.

## Three-source Global Daily Report, GORM and export volume

```powershell
$apiKey = Read-Host "Enter your API key"
.\examples\verify_global_daily.ps1 -ApiKey $apiKey
```

This posts synthetic COVID CSV, CoinGecko-style JSON and Open-Meteo-style JSON, checks the UTC daily join and queries `pipeline_daily_reports` in PostgreSQL. The sample includes an invalid COVID row, which should be present in `/errors` while the overall job completes.

To validate persistence **and** artifact cleanup, run:

```powershell
.\examples\verify_global_daily.ps1 -ApiKey $apiKey -RestartApi -DeleteAfterCheck
```

This recreates nothing destructive: it restarts only the API, verifies the job and files are still accessible, deletes that finished job and confirms its file directory/database report rows are gone. Never remove the PostgreSQL/RabbitMQ/output Docker volumes during this procedure.

## Submission checks

```bash
git status --short
git diff --check
```

Check `.env`, `prometheus-secrets/api_key`, generated `output/` and any passwords are **not** in the archive or tracked by Git. Rotate credentials previously shared. Validate the hosted GitHub Actions CI workflow separately; a local green `make ci` is not proof of hosted CI execution.

See [Architecture](architecture.md), [Technical Report](TECHNICAL_REPORT.md) and [Design Decisions](design-decisions.md) for implementation details and honest limitations.
