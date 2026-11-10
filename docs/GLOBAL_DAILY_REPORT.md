# Global Daily Report — working three-source example

This is an **opt-in** feature; existing jobs and REST paths are unchanged. It
normalizes COVID-style CSV data, CoinGecko JSON and Open-Meteo daily/hourly JSON,
then **joins by date**. JSON/CSV file exports and a separate PostgreSQL report
export are available. RabbitMQ continues to dispatch job IDs; GORM stores jobs
and migrates the new `pipeline_daily_reports` table.

## Quickstart (Windows PowerShell)

1. Back up your project. Extract the **changes-only ZIP** over its root, including
   the three `testdata/input/global_*` fixtures and the new SQL migration.
2. Preserve `.env` and the PostgreSQL/RabbitMQ Docker volumes. Run `go mod tidy`
   and `make ci` in Git Bash. Then in PowerShell:

   ```powershell
   docker compose up --build -d
   docker compose ps
   ```
3. With `API_KEY` from your own `.env` available only locally:

   ```powershell
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
   .\examples\verify_global_daily.ps1 -ApiKey '<your current API key>'
   ```

   The script posts `examples/global_daily_sample_job.json`, polls to
   COMPLETED, checks 3 joined dates, and prints the daily report. The input
   samples contain **synthetic numbers, not real COVID/market/weather values**.
   One COVID row deliberately contains `not-a-number`, so the example should
   have **one rejected row** in `/errors` but still complete successfully.

4. Check PostgreSQL report export, replacing `<job-id>` with the script output:

   ```powershell
   docker compose exec -T postgres psql -U pipeline -d pipeline -c "SELECT report_date, data FROM pipeline_daily_reports WHERE job_id = '<job-id>' ORDER BY report_date"
   ```

   The Docker API input root is `/app/testdata/input`; the Dockerfile includes
   these fixtures. Local filesystem paths outside `INPUT_DIR` are rejected.

## Job configuration

See `examples/global_daily_sample_job.json` for a complete offline example and
`examples/global_daily_live_job.json` for an online HTTP alternative.

- `sources[].adapter` values: `covid_csv`, `coingecko_market_chart`,
  `coingecko_simple`, `open_meteo_daily`, `open_meteo_hourly`. Omitting `adapter`
  retains legacy ingestion. `type` remains `csv`, `json`, or `api`.
- `sources[].field_map` maps canonical field names to source columns; for COVID
  use `date`, `cases`, `deaths`. `required_fields` is per-source; bad CSV rows are
  recorded as ingestion errors while the remainder continues.
- `sources[].transform_rules` supports the usual `trim_strings`, `normalize`,
  and `type_conversions` options for a single source. `enrich_fields` adds static
  metadata like `{"region":"global"}` without replacing input fields.
- `sources[].asset` identifies the coin for CoinGecko (for example `bitcoin`).
  The market-chart adapter expects `prices: [[unix_milliseconds, usd_price], ...]`.
  The simple-price adapter expects `last_updated_at` (set
  `include_last_updated_at=true` in the provider request).
- Open-Meteo daily uses `daily.time` and `daily.temperature_2m_mean`, or
  the arithmetic mean of `daily.temperature_2m_max/min` if mean is unavailable.
  Open-Meteo hourly uses `hourly.time` and `hourly.temperature_2m`, averaging
  non-null values for each date. Use `timezone=UTC` in provider URLs to align
  cross-source dates.
- Set `"report":{"type":"global_daily"}` to activate the join. The output is
  available under `daily_report` in `GET /api/v1/pipelines/{id}/results`.
- `aggregations[].source_kind` can filter an aggregate to `covid`, `crypto`, or
  `weather`, preventing missing-field errors on other source types.
- `exports:[{"type":"postgres"}]` stores report-date rows in the fixed
  `pipeline_daily_reports` table. No user-supplied SQL or table path is accepted;
  the server uses its existing `DATABASE_URL` or `PG*` credentials.

## Join semantics and guarantees

The report is sorted ascending by `date` (`YYYY-MM-DD`). Each date contains:

| Column | Logic |
|---|---|
| `cases`, `deaths` | Sum COVID numeric rows across countries (can be negative for corrections) |
| `crypto_prices` | USD price for each asset, taking latest UTC timestamp on the day |
| `temperature_c` | Average of weather rows for that date |
| `sources` | Sorted distinct source names contributing to the date |

Absent measurements are omitted from JSON, not filled with made-up zeroes;
CSV cells are blank. If a source has different date coverage, the report includes
the union of available dates, so a row may not contain all three types. This is
**not a relational join on country or geography**: temperature is the chosen
weather location and COVID numbers are summed globally. Source/API errors are
collected and partial reports can still complete. Validate source coverage if
completeness is critical.

Three source workers can fetch independently; five validation workers and five
transformation workers process records via typed channels. A bounded fan-in
channel feeds **one incremental aggregation goroutine concurrently** with the
worker pools. After aggregation, one export goroutine writes JSON, CSV and/or
PostgreSQL. Metrics snapshots and a separate error collector are active while
workers run. **The API and GORM result snapshots still retain the entire list
of transformed records**, so the overall job memory footprint grows with the
input size; this is not an unbounded streaming data warehouse.

## Database semantics

`internal/repository/migrations/003_daily_reports.sql` adds a dedicated table
with `(job_id, report_date)` primary key and `data JSONB`. Export uses a
transaction to replace only the current job's rows, and a foreign key cascades
when its job is deleted. Job status/results are still stored in the existing
GORM-backed `jobs` table. A separate `TEST_POSTGRES_DSN` integration test checks
real database insertion/re-export (skipped without a database).

## Live-provider caveats

Remote examples need internet connectivity and can fail because of provider
rate limits, API-key requirements, temporary outages, or changed response
shapes. The sample live request uses CoinGecko's market-chart format and an
Open-Meteo daily forecast. Optionally put
`SOURCE_HOST_ALLOWLIST=api.coingecko.com,api.open-meteo.com` in `.env` to
restrict permitted source hosts. Do not disable SSRF protection just to make
localhost mock URLs accessible in production. The offline fixtures are
recommended for repeatable assignment verification.

## Tests

```bash
go test ./internal/ingestion ./internal/report ./internal/pipeline -count=1
go test -race ./... -count=1
make ci
```

For real ORM export testing, set `TEST_POSTGRES_DSN` to a **dedicated test
PostgreSQL database** and run `go test ./internal/export -run
TestPostgresReportExportIntegration -v` (test creates then deletes its job).
Re-run your existing Postman 53-test suite to check API compatibility.

## Job deletion and restart behavior

The API rewrites file exports into `/app/output/<job-id>/<filename>`, which is
backed by the Docker `pipeline_exports` named volume. An API container restart
or recreation does not remove these exported files. Once the job has reached
a terminal state, `DELETE /api/v1/pipelines/<job-id>` removes its own file
folder, the stored result/errors, and GORM report rows via PostgreSQL CASCADE.
Do not run `docker compose down -v` during ordinary updates; it removes
PostgreSQL, RabbitMQ and export volumes. The correct cancellation endpoint is
`PATCH /api/v1/pipelines/<job-id>/cancel` (`POST` remains a compatibility alias).
