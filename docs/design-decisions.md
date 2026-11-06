# Design choices and trade-offs

The project uses Go's standard `net/http` package and packages grouped by responsibility. This keeps learning and debugging straightforward without introducing a framework or unnecessary service/interface layers.

CSV, JSON and HTTP API readers convert input into `domain.Record`. A runner fans out source reads and uses configurable validation/transformation goroutine pools connected by typed channels. `context.Context` controls cancellation; producers own channel closure. Results and stage errors are returned to the runner's caller. The HTTP API now launches runner jobs asynchronously and stores status, results, and errors in the in-memory repository.

The job repository uses a mutex-protected in-memory map. This is convenient for development but jobs disappear at restart and are not shared across replicas. A persistent database and transactional job lifecycle will be needed for durable production workloads.

Configuration comes from environment variables. Production and non-loopback listeners require an API key. Middleware supplies request IDs, structured logs, panic recovery, rate limiting and basic response hardening. Job-submission validation restricts local source paths and rejects private/reserved source IPs when configured. These are defense-in-depth controls, not complete SSRF protection: before enabling API-triggered execution, outbound HTTP connections and redirects must be restricted at dial time to prevent DNS rebinding and redirect bypasses.

The Docker build uses a Go builder stage and a non-root distroless runtime. Swagger documents existing HTTP endpoints only. Integration, load and external-service tests should be expanded as the API starts running jobs.
