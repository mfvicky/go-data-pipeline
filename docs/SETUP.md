# Setup Guide — Go Data Pipeline with GORM + RabbitMQ

## 1. Requirements

Go 1.27 as specified by `go.mod`, Docker Desktop with Compose, Git Bash/PowerShell on Windows, GNU Make, and `golangci-lint` v2. CGO/GCC is required for `go test -race` on Windows, but the GORM/pgx PostgreSQL driver itself does **not** need native libpq. You can use Linux/macOS equivalents for local commands.

## 2. Configure secrets safely

Copy `.env.example` to `.env` and set **distinct** strong secrets for `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD` and `API_KEY`. Keep `.env` out of Git, ZIPs and chat messages. Place exactly the API key value in `prometheus-secrets/api_key` (create the directory if needed); this file is excluded from shared archives. If you previously exposed a live API key, rotate it in both locations. A Docker database/broker volume initialized with an older password does not update its internal user when `.env` changes; change the user password properly rather than deleting volumes.

## 3. Verify locally (VS Code Git Bash)

```bash
export PATH="/c/msys64/ucrt64/bin:$PATH"  # Windows MSYS2 GCC (for race tests)
export CGO_ENABLED=1
export CC=gcc
make deps
make ci
```

If Go reports that no module cache exists, ensure `GOPATH` and `GOMODCACHE` are configured; the included Makefile passes these paths to its Go recipes. Run all commands in the same terminal.

## 4. Run the four Docker services (PowerShell)

```powershell
docker compose up --build -d
docker compose ps
docker compose logs --tail=60 rabbitmq api
curl.exe http://localhost:8080/health
curl.exe http://localhost:8080/ready
curl.exe "http://localhost:9090/api/v1/query?query=up"
```

API at `http://localhost:8080`, RabbitMQ management at `http://localhost:15672` (credentials from `.env`), Prometheus at `http://localhost:9090`. RabbitMQ can take time to initialize; the Compose health-check grace period is 90 seconds. Do **not** run `docker compose down -v` as part of a normal update: that command deletes all named volumes, including PostgreSQL data and exported reports.

## 5. Verify the exact assignment example

```powershell
$apiKey = Read-Host "Enter your API key"
.\examples\verify_global_daily.ps1 -ApiKey $apiKey
```

Expected: three joined UTC dates, one deliberately rejected COVID row, JSON/CSV exports and three PostgreSQL report rows. For optional restart and delete checks, run the same script with `-RestartApi -DeleteAfterCheck`. The bundled fixture data is synthetic. See [Global Daily Report](GLOBAL_DAILY_REPORT.md) and [Verification](VERIFICATION.md).

## 6. API and optional TLS

Every protected REST endpoint takes `X-API-Key`. `PATCH /api/v1/pipelines/{id}/cancel` is the assignment method; `POST` remains a legacy alias. `DELETE` accepts only a terminal job and cleans its job-specific files, database snapshot and daily-report rows. Swagger UI: `http://localhost:8080/swagger/`, OpenAPI: `http://localhost:8080/openapi.yaml`.

For a real domain, configure `PIPELINE_DOMAIN` and a valid DNS record, then run `docker compose --profile tls up -d` with external ports 80/443 available. This is optional and requires network/security administration.
