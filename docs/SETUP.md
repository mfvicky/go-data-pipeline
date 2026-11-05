# Setup Guide — Go Data Processing Pipeline

This guide matches the production-structure project built from `cmd2.zip` (with `internal/config`, `internal/middleware`, `internal/security`, `Dockerfile`, and `compose.yaml`). Use **Windows 10, VS Code, and Git Bash** for the commands below. Run commands from the repository root unless a step says otherwise.

## 1. Requirements

- Go 1.23 or newer. Check with `go version`.
- Git Bash and cURL for local testing.
- Docker Desktop with Compose **only if using Docker**.
- MSYS2 UCRT64 GCC for `go test -race` on Windows.

```bash
cd /go-data-pipeline
go version
go mod tidy
go build ./...
```

## Developer Tool Installation
### GNU Make
On Windows, install GNU Make using MSYS2 or another compatible distribution.
Verify:
```bash
make --version
```

### golangci-lint
Install golangci-lint v2 using the official instructions:
https://golangci-lint.run/docs/welcome/install/

Verify:
```bash
golangci-lint version
```
### Enable Git Hooks

From the project root:

```bash
git config core.hooksPath .githooks
```
Verify:
```bash
git config --get core.hooksPath
```
Expected output:
```text
.githooks
```
Git Bash is required for the included hook scripts on Windows.

### Run Developer Checks
```bash
make help
make fmt
make check
```
The `make check` command requires golangci-lint to be installed.


## 2. Create and configure `.env`

Make a local copy of the example; never commit `.env`.

```bash
cp -n .env.example .env
```

The supplied `.env.example` contains:

```dotenv
APP_ENV=development
HOST=127.0.0.1
PORT=8080
API_KEY=replace-with-a-long-random-secret
INPUT_DIR=testdata/input
ALLOW_PRIVATE_SOURCES=true
RATE_LIMIT_PER_MINUTE=120
READ_HEADER_TIMEOUT=5s
READ_TIMEOUT=15s
WRITE_TIMEOUT=30s
IDLE_TIMEOUT=60s
SHUTDOWN_TIMEOUT=10s
```

Replace `API_KEY` in `.env` with a fresh random value. In Git Bash, you can generate one using:

```bash
openssl rand -hex 32
```

Keep the remaining values as shown for local testing. `INPUT_DIR` controls which local files can be referenced by **API job specifications**. `ALLOW_PRIVATE_SOURCES=true` is for trusted local development; do not enable it for publicly accessible deployments.

**Important:** `go run` does **not** load `.env` automatically. For the local Go server, export the file into the terminal session:

```bash
set -a
source .env
set +a
```

Check the non-secret settings without printing the API key:

```bash
printf 'Environment: %s | Host: %s | Port: %s\n' "$APP_ENV" "$HOST" "$PORT"
test -n "$API_KEY" && echo 'API key is configured'
```

Do **not** commit secrets or paste your real API key into issue reports.

## 3. Run locally (without Docker)

In Git Bash **Terminal 1**, from the project root:

```bash
go run ./cmd/server
```

In **Terminal 2**, load the `.env` again (terminals do not share shell exports):

```bash
cd /go-data-pipeline
set -a
source .env
set +a
curl -i http://localhost:8080/health
curl -i -H "X-API-Key: $API_KEY" http://localhost:8080/api/v1/pipelines
```

Expected: health **200 OK**, authenticated job list **200 OK**. When an `API_KEY` is configured, an unauthenticated call to `/api/v1/pipelines` returns **401 Unauthorized**. Stop the server with **Ctrl+C**; the HTTP server attempts graceful shutdown.

The service uses an in-memory job repository; jobs disappear when the process restarts.

## 4. Run with Docker Desktop

Docker is optional; use **either** the Docker server **or** `go run ./cmd/server` on port 8080, not both.

1. Launch Docker Desktop and wait until its engine is running.
2. Make sure `.env` contains a strong `API_KEY` (see section 2).
3. From the repository root, run:

```bash
docker --version
docker compose version
docker info
docker compose config -q
docker compose build
docker compose up -d
docker compose ps
docker compose logs --tail=50 api
```

Expected: `docker compose config -q` reports no errors; the `api` service is running and should become healthy after its health checks complete. The image build requires network access to its Go and runtime base images.

Verify the running container from Git Bash:

```bash
curl -i http://localhost:8080/health
set -a
source .env
set +a
curl -i http://localhost:8080/api/v1/pipelines
curl -i -H "X-API-Key: $API_KEY" http://localhost:8080/api/v1/pipelines
```

Expected: **200** for health, **401** without an API key, **200** with a valid API key.

### Docker configuration differences

`compose.yaml` **overrides** these settings regardless of the corresponding `.env` values:

| Setting | Container value | Reason |
| --- | --- | --- |
| `APP_ENV` | `production` | Production validations enabled |
| `HOST` | `0.0.0.0` | Listen inside the container |
| `PORT` | `8080` | Container port |
| `INPUT_DIR` | `/app/testdata/input` | Bundled sample inputs |
| `ALLOW_PRIVATE_SOURCES` | `false` | Deny private/reserved IP sources in job specifications |

The host port comes from `.env` `PORT` (default **8080**) and binds only to `127.0.0.1` on the host. The API key is mandatory in Compose. Compose also sets a read-only filesystem, runs without root privileges, and drops Linux capabilities. Docker build/runtime has to be verified on your own Docker Desktop installation.

The sample files are copied into the image under `/app/testdata/input`; an API job spec should use paths like `testdata/input/customers.csv` (relative to the container's working directory). Arbitrary host files are **not automatically mounted**.

### Docker lifecycle commands

```bash
docker compose ps
docker compose logs -f api
docker compose restart api
docker compose down
```

Running `docker compose down` removes the container; the in-memory job list is not preserved.

## 5. Swagger / OpenAPI

After starting the server, open:

- Swagger UI: <http://localhost:8080/swagger/>
- OpenAPI YAML: <http://localhost:8080/openapi.yaml>
- Health: <http://localhost:8080/health>

Swagger's **Authorize** dialog accepts the configured API key for `/api/v1/pipelines` requests. The Swagger page loads UI assets from jsDelivr, so browser access to that CDN is needed. The YAML endpoint does not require CDN access.

## 6. Run Go tests and formatting checks

```bash
go fmt ./...
go vet ./...
go test ./... -count=1
go build ./...
```

For the race detector on Git Bash with MSYS2 UCRT64 installed:

```bash
export PATH="/c/msys64/ucrt64/bin:$PATH"
export CGO_ENABLED=1
export CC=gcc
gcc --version
go test -race ./... -count=1
```

`go fmt` changes source formatting. If you want a non-editing check for CI-like verification, use `gofmt -l` on changed `.go` files or rely on the existing CI workflow.

## 7. Common issues

| Problem | Check / solution |
| --- | --- |
| `curl: (7) Failed to connect` | Start the server or container; check port 8080 and `docker compose ps` |
| `401 Unauthorized` | Load `.env` in **this** terminal and pass `X-API-Key: $API_KEY` |
| `API_KEY is required in production` | Set `API_KEY` in `.env`; Compose needs a nonempty value |
| `port is already allocated` | Stop the other server or change host `PORT` in `.env` |
| `go: -race requires cgo` | Install MSYS2 UCRT64 GCC, export `CGO_ENABLED=1`, and set Git Bash PATH |
| `gcc: command not found` | Add `/c/msys64/ucrt64/bin` to the **Git Bash** PATH |
| Docker image build cannot pull base image | Check Docker Desktop networking and whether the base image tags are available |
| Swagger opens but UI does not render | Check browser access to jsDelivr; `/openapi.yaml` is still available |
| API rejects local source file | File must exist inside `INPUT_DIR`; run from project root or use bundled Docker sample files |
| External API reader times out | Check network/proxy and use the demo CLI `-timeout 60s`; remote services are not guaranteed reachable |

For step-by-step API and pipeline verification, see [VERIFICATION.md](VERIFICATION.md). For the project's intended scope and limitations, see [../README.md](../README.md).
