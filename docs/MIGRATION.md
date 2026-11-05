# Applying this update to the existing project

This project was built from `cmd2.zip`. The existing ingestion, validation, transformation, repository and pipeline business logic remains in place.

## Apply changes

1. Commit or back up your current local changes.
2. Extract the full project ZIP to a separate folder to compare files, **or** extract the changes-only ZIP over your existing project root.
3. Review `.env.example` and set `API_KEY` before using `HOST=0.0.0.0` or `APP_ENV=production`.
4. Run the commands below from the project root.

```bash
go mod tidy
go fmt ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go build ./...
go run ./cmd/server
```

The local development defaults bind to `127.0.0.1:8080`. If `API_KEY` is set, add `-H "X-API-Key: $API_KEY"` to requests to `/api/v1/pipelines`.

## Change summary

- New: `internal/config`, `internal/middleware`, `internal/security`.
- New: API constants, ingestion/pipeline constants, OpenAPI/Swagger, Docker, CI and documentation.
- Updated: `cmd/server/main.go`, `internal/api/router.go`, `internal/api/handlers.go`, `internal/ingestion/http.go`, `internal/pipeline/runner.go`, `internal/pipeline/worker.go`, `.gitignore`, `README.md`.
- Preserved: Job creation remains `PENDING`, pipeline execution remains independent, and no aggregation or export implementation was added.
- Cleaned: Generated `output/`, `post.txt`, and `cmd.txt` from the source ZIP are intentionally excluded from deliverables.

## Security notes

API key and rate limiting apply to the server created in `cmd/server/main.go`. Direct calls to `api.NewRouter()` in existing unit tests use the backward-compatible no-key setup. Use `api.NewRouterWithOptions()` when instantiating the router in applications.

Source validation checks file roots and DNS addresses at submission time. Before triggering pipeline execution directly from untrusted HTTP requests, add dial-time network restrictions, redirect checking, and safe file-open handling.
