# Golang Data Processing Pipeline

## Run the API

From the project root:

```bash
go version

# adding or removing library need to run this
go mod tidy

# prettier
go fmt ./...

# linting
go vet ./...

# test
go test ./... -v

export PATH="/c/msys64/ucrt64/bin:$PATH"
export CGO_ENABLED=1
export CC=gcc

# concurrecy issue test 
go test -race ./...


# compile project
go build ./...

# run project
go run ./cmd/server

# health test
curl http://localhost:8080/health

```

# data pipeline

```bash

# Creates a pipeline job and saves its ID in a shell variable
JOB_ID=$(curl -fsS -X POST http://localhost:8080/api/v1/pipelines -H "Content-Type: application/json" -d '{"sources":[{"name":"customers_csv","type":"csv","endpoint":"testdata/input/customers.csv"}]}' | sed -n 's/.*"job_id":"\([^"]*\)".*/\1/p')

# Checks that the job ID is not empty
test -n "$JOB_ID" && printf 'JOB_ID=%s\n' "$JOB_ID"

# Lists all pipeline jobs
curl -i http://localhost:8080/api/v1/pipelines

# Gets details of one job
curl -i "http://localhost:8080/api/v1/pipelines/$JOB_ID"

# Tests invalid job configuration
curl -i -X POST http://localhost:8080/api/v1/pipelines -H "Content-Type: application/json" -d '{}'

# Tests reading CSV records
go run ./cmd/ingest-demo -type csv -endpoint testdata/input/customers.csv

# Tests reading JSON records
go run ./cmd/ingest-demo -type json -endpoint testdata/input/customers.json

# Tests nested JSON parsing
go run ./cmd/ingest-demo -type json -endpoint testdata/input/nested.json -record-path data.records

# Fetches data from an external HTTP API
go run ./cmd/ingest-demo -type api -endpoint https://jsonplaceholder.typicode.com/posts > post.txt

# Deletes an existing job  - 204
curl -i -X DELETE "http://localhost:8080/api/v1/pipelines/$JOB_ID"

# Confirms the job was deleted - 404
curl -i "http://localhost:8080/api/v1/pipelines/$JOB_ID"

```
