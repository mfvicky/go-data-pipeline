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
                            (status PENDING)

Pipeline runner (currently called by Go code/tests, not REST API):
CSV/JSON/HTTP source goroutines
        |
     records channel
        |
 validation worker pool -> errors channel
        |
 transformation worker pool -> errors channel
        |
    processed records
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
- `internal/pipeline`: channels, worker pools, cancellation and runner.
- `internal/repository`: in-memory state with mutex protection.
- `docs`: embedded OpenAPI specification and Swagger UI entrypoint.

## Notes

- The REST API stores specifications; automatic job execution is not connected yet.
- Aggregation, exporting and advanced metrics are not implemented by this code baseline.
- The repository is in-memory; restarting the server clears jobs.
- Source validation on job creation does not replace secure DNS resolution and network filtering at *connection time*. Add connection-time SSRF protection before exposing job execution to untrusted users.
