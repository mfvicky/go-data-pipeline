# Architecture

```text
Client / Swagger UI
        |
        v
net/http server (cmd/server)
        |
request ID -> request logging -> panic recovery -> security headers
        |
        +--> /health, /openapi.yaml, /swagger/ (public)
        |
        +--> rate limit -> API-key auth -> /api/v1/pipelines
                                     |
                                     v
                              API handlers
                                     |
                                     v
                         In-memory job repository
                            (PENDING → RUNNING → terminal)

API job manager starts pipeline.RunWithObserver asynchronously:
CSV/JSON/HTTP source goroutines
        |
     records channel
        |
 validation worker pool -> errors channel
        |
 transformation worker pool -> errors channel
        |
    processed records
        |
 aggregation -> JSON/CSV export
        |
 centralized results/errors -> in-memory repository
        |
 progress / errors / results / cancel HTTP endpoints
```

## Package responsibilities

- `internal/api`: API handlers and HTTP routes.
- `internal/config`: environment parsing and safe server defaults.
- `internal/middleware`: request IDs, logging, recovery, API-key auth, throttling, headers.
- `internal/security`: source URL and local-file checks at job submission.
- `internal/domain`: typed records, job specifications and job state.
- `internal/ingestion`: CSV/JSON/local and external HTTP input readers.
- `internal/validation`: record rules and invalid-record filtering.
- `internal/transformation`: normalization and type conversions.
- `internal/pipeline`: channels, worker pools, cancellation, aggregation/export runner and live metrics.
- `internal/jobs`: asynchronous job manager, status changes and cancellation.
- `internal/aggregation`, `internal/export`: aggregation and CSV/JSON file output.
- `internal/repository`: in-memory state with mutex protection.
- `docs`: embedded OpenAPI specification and Swagger UI entrypoint.

## Notes

- The REST API saves specifications and starts asynchronous pipeline jobs.
- Aggregation, exporting and live progress metrics are implemented; results remain in memory.
- The repository is in-memory; restarting the server clears jobs.
- Source validation on job creation does not replace secure DNS resolution and network filtering at *connection time*. Add connection-time SSRF protection before exposing job execution to untrusted users.
