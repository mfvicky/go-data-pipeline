# Design decisions and trade-offs — current version

## Why Go channels and worker pools?

Ingestion, validation and transformation have distinct responsibilities and exchange `domain.Record` values using typed channels. The source pool and the validation/transformation pools have configurable sizes; a single fan-in accumulator consumes transformed records as they arrive. Channels provide bounded backpressure, while `sync.WaitGroup` ensures only producers close their respective output channels. Context cancellation and timeouts stop producers and workers cooperatively.

## Why RabbitMQ with PostgreSQL/GORM?

PostgreSQL is the system of record for job specifications, statuses, errors, individual results and date-joined reports. GORM uses pgx; no native libpq runtime is needed. Numbered SQL migrations version schema updates. RabbitMQ transports confirmed, persistent job-ID messages and manual consumer acknowledgements, so job execution is decoupled from HTTP request latency. On restart, pending jobs are re-dispatched; duplicate deliveries are checked against persisted terminal states. This design avoids requiring a full multi-node message-processing platform for the assignment.

**Trade-off:** the database write and broker publication are not one transaction. A database-backed pending-job replay path mitigates failures but is not equivalent to a transactional outbox. The repository caches state in process; do not run multiple API replicas without distributed locking and recovery design. Queue delivery is at least once rather than exactly once.

## Why both file and database export?

File exports provide portable CSV/JSON artifacts; a dedicated report table supports querying by job and date. Files use temporary-write-then-rename within each job's output folder and a persistent Docker volume. GORM report writes occur inside a transaction and cascade on job deletion. Database and filesystem changes cannot be committed atomically together; cleanup errs on the side of preserving the job record when file removal fails.

## Why opt-in provider adapters?

The existing generic CSV/JSON/API pipeline remains useful for arbitrary data. Explicit adapters normalize the COVID CSV, CoinGecko historical prices and Open-Meteo weather formats into a consistent UTC daily schema. Per-source mapping, required fields, type conversions and static enrichment handle heterogeneity before the shared validator. Missing cross-source dates appear as absent fields; the report does not fabricate measurements.

## Limitations intentionally retained

- The API persists full processed records, so result RAM/JSONB size grows with the job. Source streams, aggregation buckets and file writes do not make the entire system unlimited-memory.
- The bundled data is synthetic; live providers may change schemas, impose rate limits or require authentication.
- A source limit (currently 20 MiB per source) prevents unbounded reads; very large real-world datasets require partitioning or a configurable streaming storage tier.
- Network egress firewall enforcement, TLS certificate issuance, multi-tenant authorization, transactional outbox and shared object storage are deployment-scale improvements, not included in this assignment implementation.
