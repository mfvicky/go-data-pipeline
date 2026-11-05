FROM golang:1.27-bookworm AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /server /app/server
COPY --from=builder /src/testdata/input /app/testdata/input
ENV APP_ENV=production HOST=0.0.0.0 PORT=8080 INPUT_DIR=/app/testdata/input
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD ["/app/server", "healthcheck"]
USER nonroot:nonroot
ENTRYPOINT ["/app/server"]
