# Architecture — Go Data Pipeline (current)

```text
POST /api/v1/pipelines   (API key, rate limiting, validation)
          |
          v
   internal/services/JobService ---------> GORM / PostgreSQL
          |                              jobs JSONB + report-date rows
          v                                         ^
   RabbitMQ durable job-ID queue                    |
          | (manual ACK after persistence)          |
          v                                         |
   bounded Jobs Manager ----------------------------+
          |
          v
  Source scheduler (fan-out to up to 3 configurable readers)
   / CSV / JSON / HTTP / provider-specific adapters
          |
       recordsCh                 errorCh ----------> collector goroutine
          |                                                |
    validation worker pool                      persistent job error log
          | validatedCh                                   |
    transformation worker pool                            |
          | transformedCh <------------ progress/status snapshots
          v
   bounded fan-in aggregate-input channel
          |                  \
      incremental single      retain processed records for results API
      aggregation goroutine                 |
          |                                 |
          +--- aggregation and daily join --+
                            |
                   export goroutine
             JSON / CSV / PostgreSQL (GORM)
                            |
             persist result and final state
                            |
               RabbitMQ delivery ACK
```

## Data flow and concurrency

- Each source is assigned to the configurable ingestion pool; each reader streams CSV/JSON rows or decodes bounded provider payloads. Source-specific `field_map`, `required_fields`, `transform_rules` and `enrich_fields` normalize data before the shared validation pool.
- Worker pools are connected by typed channels. Producers close **their own** output channel after their `WaitGroup` completes; readers handle cancellation via `context.Context` and `select`. A separate goroutine drains `errorCh` and retains failed record data.
- `aggregatedInput` is bounded and consumed while records are still arriving. One goroutine incrementally accumulates regular aggregations and, when requested, the UTC date-keyed Global Daily Report. It stores one bucket per configured group/date; full processed records are separately retained for `/results`.
- An export goroutine serializes requested outputs and uses temporary-file-and-rename for files. PostgreSQL reports are inserted under a server-selected table with GORM and a numbered migration. Progress and stage timings are emitted while the job runs.
- The API is asynchronous. RabbitMQ queues **job IDs**, not whole input files. The worker looks up the persisted spec and ACKs only after final result/status persistence. Database job state is the authoritative source for job details and completion. Recovery retries pending jobs after restart; a transient uncertain publish returns 503 with a persisted job ID.

## Storage and artifact lifetime

- `jobs(id TEXT, data JSONB)` stores job specs, status, errors and results. `pipeline_daily_reports(job_id, report_date, data JSONB)` stores optional joined reports; `ON DELETE CASCADE` removes these rows when a job is deleted.
- File targets are relocated into `/app/output/<job-id>/` by the application service. Docker mounts the `pipeline_exports` named volume here, preserving exports across API recreation. Deletion checks the job is terminal, removes only the safe per-job export directory, then deletes the DB snapshot. Old artifacts created before per-job scoping cannot be safely cleaned automatically.
- Retention cleanup uses the same deletion path. Database and filesystem deletion are not one atomic transaction; failure of the first step retains the DB record for later retry.

## Security and operational limitations

- API key, strict JSON, request limits, request IDs, structured logs, defensive headers, CORS and rate limiting are implemented. Sources are restricted to `INPUT_DIR` or validated HTTP(S) destinations. DNS, redirect and dial-time checks reduce SSRF; `SOURCE_HOST_ALLOWLIST` narrows permitted domains.
- A development Compose stack includes PostgreSQL, RabbitMQ, Prometheus and the API. Caddy/TLS is optional with a real domain and network configuration; it is not proof of a production deployment. The RabbitMQ and PostgreSQL passwords should be distinct from the API key.
- This is a **single-API-replica** implementation with at-least-once messaging, not distributed exactly-once execution. It does not implement multi-tenant RBAC, a transactional outbox, distributed leases, or complete streaming response/result persistence. The `Records` slice still grows with input size, even though ingestion and aggregation stream.
