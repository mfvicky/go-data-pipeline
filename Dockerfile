FROM golang:1.27-bookworm AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /server ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 app && useradd -u 65532 -g 65532 -M -s /usr/sbin/nologin app \
    && mkdir -p /app/output && chown -R 65532:65532 /app/output
WORKDIR /app
COPY --from=builder /server /app/server
COPY --from=builder /src/testdata/input /app/testdata/input
ENV APP_ENV=production HOST=0.0.0.0 PORT=8080 INPUT_DIR=/app/testdata/input STORAGE_DRIVER=postgres
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD ["/app/server", "healthcheck"]
USER 65532:65532
ENTRYPOINT ["/app/server"]
