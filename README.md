# Go Data Processing Pipeline — GORM + RabbitMQ + Global Daily Report

Concurrent Go REST API for CSV, JSON and HTTP ingestion, schema/business validation, transformation, grouped aggregation, JSON/CSV export, status, cancellation, errors and results. PostgreSQL stores job snapshots using **GORM + pgx**; **RabbitMQ** delivers durable job-ID messages to bounded worker consumers. **Prometheus** scrapes `/metrics`. The existing API paths remain the same as in `docs/openapi.yaml`; transient RabbitMQ publication failures additionally return 503 with the persisted job ID.

## New: Global Daily Report (three-source concurrency example)

The pipeline now supports **source-specific adapters and required-field checks**
for COVID-style CSV, CoinGecko prices and Open-Meteo daily/hourly weather. Set
`"report":{"type":"global_daily"}` to merge records by date. The job result
includes `daily_report`; JSON and CSV can export that report, and a dedicated
`postgres` export stores rows in the GORM-managed `pipeline_daily_reports`
table. A bounded channel feeds an incremental single-goroutine aggregation stage while worker pools run. A dedicated export goroutine writes file/DB targets after aggregation completes.

Try `examples/global_daily_sample_job.json` and run
`examples/verify_global_daily.ps1` after starting Docker. The bundled samples
are **synthetic deterministic fixtures**; one intentionally invalid COVID row
shows source-level error collection without stopping the job. Online CoinGecko
and Open-Meteo sources are illustrated in `examples/global_daily_live_job.json`
(provider limits and connectivity apply). Full instructions and join semantics:
[docs/GLOBAL_DAILY_REPORT.md](docs/GLOBAL_DAILY_REPORT.md).

## Stack

- Go 1.27 for the Docker build (Go 1.27 is recommended). GORM/pgx does not require libpq or CGO. Race detection on Windows still needs a C compiler.
- PostgreSQL 17 (`postgres_data` persistent Docker volume); ORM migrations in `internal/repository/migrations/`. Schema version 3 includes `pipeline_daily_reports` and keeps the **existing** `jobs(id,data JSONB)` and `schema_migrations` tables.
- RabbitMQ 4.1 (`rabbitmq_data` volume), publisher confirms, persistent messages, manual ACK after status/result persistence, prefetch=1, quorum queue with a 10-attempt redelivery cap, dead-letter queue for malformed/exhausted messages, consumer reconnect.
- 1 API replica, N concurrent job consumers, JSON logging with job ID and request ID, API key, rate limiting, security headers, CORS, restricted file inputs, source allowlist, SSRF defense.

## Quick start in Windows PowerShell

1. Create `.env` from `.env.example` and set **different strong secrets** for `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD`, `API_KEY`. Make RabbitMQ credentials URL-safe (letters/numbers/`-`/`_`) or URI-encode them in an AMQP URL. When changing a credential on an initialized volume, update the existing database/broker credential as well; an environment change alone does not rotate existing users.
2. Create `prometheus-secrets/api_key` containing the `API_KEY` value **without quotes**; this file is intentionally absent from the submitted ZIP and ignored by Git.
3. Run:

```powershell
go mod tidy
make ci
docker compose up --build -d
docker compose ps
curl.exe http://localhost:8080/health
curl.exe http://localhost:8080/ready
curl.exe "http://localhost:9090/api/v1/query?query=up"
```

If you have already set up Windows Git Bash, run `make ci` there; Docker Compose can run from PowerShell. **Do not use `docker compose down -v`** unless you intentionally want to delete PostgreSQL/RabbitMQ data.

The project includes the existing `go.sum`. Run `go mod tidy` after applying updates so dependency checksums remain in sync; commit any resulting changes.

## Endpoints

| Method | URL | Description |
|---|---|---|
| GET | `/health`, `/ready` | Health and DB readiness |
| POST | `/api/v1/pipelines` | Create and enqueue an async job |
| GET | `/api/v1/pipelines` | List statuses with pagination |
| GET | `/api/v1/pipelines/{id}` | Read spec + status |
| GET | `/api/v1/pipelines/{id}/progress` | Progress |
| GET | `/api/v1/pipelines/{id}/results` | Records and aggregations |
| GET | `/api/v1/pipelines/{id}/errors` | Errors |
| PATCH | `/api/v1/pipelines/{id}/cancel` | Cancel a pending/running job (required assignment contract) |
| POST | `/api/v1/pipelines/{id}/cancel` | Deprecated compatibility alias for older Postman collections |
| DELETE | `/api/v1/pipelines/{id}` | Delete terminal job, its GORM data/report rows, and job-owned file exports |
| GET | `/metrics` | Prometheus text endpoint (API key) |
| GET | `/docs/openapi.yaml`, `/swagger` | API definition / docs |

Protected endpoints use `X-API-Key: <API_KEY>`. Example payloads are in `examples/`; Postman collection in `examples/postman_collection.json`.

## Environment

| Variable | Meaning |
|---|---|
| `STORAGE_DRIVER` | `memory` for standalone tests, `postgres` for Docker |
| `QUEUE_DRIVER` | `memory` for standalone tests, `rabbitmq` for Docker |
| `DATABASE_URL` or `PG*` | PostgreSQL DSN or PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE |
| `RABBITMQ_URL` | AMQP URL (default Compose provides it) |
| `RABBITMQ_QUEUE` | Durable queue name, default `pipeline.jobs` |
| `MAX_CONCURRENT_JOBS` | Number of worker consumers (default 5) |
| `JOB_QUEUE_CAPACITY` | In-memory queue capacity (memory mode only) |
| `JOB_RETENTION` | Terminal job expiry, default 24h |
| `INPUT_DIR`, `SOURCE_HOST_ALLOWLIST`, `ALLOW_PRIVATE_SOURCES` | Source security settings |
| `API_KEY`, `CORS_ORIGINS`, `RATE_LIMIT_PER_MINUTE` | API controls |
| `LOG_LEVEL`, `LOG_FORMAT` | Structured logging controls |
| `PIPELINE_DOMAIN` | Optional Caddy HTTPS profile hostname |

## Runtime verification

```powershell
docker compose exec -T postgres psql -U pipeline -d pipeline -c "SELECT version FROM schema_migrations ORDER BY version"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages consumers
docker compose logs --tail=50 api
```

Open RabbitMQ management: http://localhost:15672 (configured RabbitMQ user/password), Prometheus: http://localhost:9090. To enable Caddy HTTPS after you configure real DNS, replace `PIPELINE_DOMAIN` and use `docker compose --profile tls up -d`; the default localhost API binding is intended for development. A public HTTPS setup needs ports 80/443 accessible and a valid domain.

## Architecture and guarantees

`POST -> services.JobService -> GORM (spec/status PENDING) -> RabbitMQ (persistent confirmed job ID) -> bounded consumer -> jobs.Manager -> pipeline.Run -> GORM (result/final state) -> ACK`. The pipeline uses concurrent ingestion and validation/transformation worker pools with backpressure. Errors are collected and cancellation propagates through contexts. Aggregation is incremental, but job output/result persistence still buffers records in memory; very large jobs are not fully streaming.

On API restart, persisted PENDING/RUNNING jobs are reset to PENDING and republished by ID. A one-minute recovery dispatcher also retries pending IDs after a temporary broker outage. If a publish fails, the API returns HTTP 503 with the saved `job_id` and the record stays PENDING for retry. Duplicate IDs are ignored after reaching a terminal state. **Limitations:** This example is deliberately single-API-replica: repository state is cached locally, there is no distributed lease/fencing, transactional outbox, production-grade external object storage for exported files, or multi-tenant authorization. Local Docker file exports use the persistent `pipeline_exports` volume, and are deleted with their job. Do not horizontally scale without adding those controls. Queue broker redelivery by itself does not provide exactly-once side effects.

For a detailed architecture narrative and limitations see `docs/architecture.md`, `docs/TECHNICAL_REPORT.md`, and `docs/VERIFICATION.md`.

## Security hygiene

Do not commit `.env`, `prometheus-secrets/api_key`, output files, or credentials. Rotate any API key previously copied into logs, ZIP archives or chat. Use an outbound firewall / egress policy in addition to application SSRF checks for internet-facing deployments. The optional `SOURCE_HOST_ALLOWLIST` accepts exact hostnames and `*.example.com` patterns.

## Assignment-compliance upgrades (current version)

- **Correct cancellation method:** `PATCH /api/v1/pipelines/{id}/cancel` returns 202, 404 or 409. A `POST` compatibility alias remains for older clients.
- **Artifact lifecycle:** exports live in a dedicated `output/<job-id>/` directory. DELETE of a completed/failed/cancelled job deletes only that directory, then deletes its GORM job record. Foreign-key cascade deletes `pipeline_daily_reports` rows. The Docker `pipeline_exports` volume survives API recreation; deleting the volume is intentionally destructive.
- **Per-source field rules:** `sources[].field_map`, `required_fields`, `transform_rules` (same syntax as global transformations), and `enrich_fields` are applied before the shared validation pool. Source-specific rejected rows appear under `/errors`.
- **Fan-in:** a bounded `aggregatedInput` channel is consumed by a single incremental aggregation goroutine concurrently with ingestion, validation and transformation; one export goroutine serializes selected output targets. An error collector runs concurrently. Input and metric snapshots can be observed during execution.
- **Repeatable multi-source demo:** `examples/global_daily_sample_job.json` uses bundled synthetic COVID CSV, CoinGecko-style JSON and Open-Meteo-style JSON. `examples/verify_global_daily.ps1` checks reports and GORM rows in PostgreSQL.

**Run checks:** `make deps`, `make ci`, `docker compose up --build -d`, then `./examples/verify_global_daily.ps1` in PowerShell with your local API key. See [current setup](docs/SETUP.md), [verification](docs/VERIFICATION.md), [architecture](docs/architecture.md), [technical report](docs/TECHNICAL_REPORT.md), and [global report instructions](docs/GLOBAL_DAILY_REPORT.md).
