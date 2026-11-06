package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"

	"github.com/mfvicky/go-data-pipeline/docs"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/middleware"
	"github.com/mfvicky/go-data-pipeline/internal/monitor"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
	"github.com/mfvicky/go-data-pipeline/internal/services"
)

type Options struct {
	APIKey             string
	RateLimitPerMinute int
	Logger             *slog.Logger
	SourcePolicy       *security.SourcePolicy
	JobContext         context.Context
	JobManager         *jobs.Manager
	MaxConcurrentJobs  int
	QueueCapacity      int
	Metrics            *monitor.Registry
	JobLogger          *slog.Logger
	CORSOrigins        []string
}

func NewRouter(repo repository.Store) http.Handler {
	return NewRouterWithOptions(repo, Options{})
}

func NewRouterWithOptions(repo repository.Store, opts Options) http.Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.RateLimitPerMinute <= 0 {
		opts.RateLimitPerMinute = 120
	}
	limiter := middleware.NewLimiter(opts.RateLimitPerMinute)
	manager := opts.JobManager
	if manager == nil {
		manager = jobs.NewManagerWithOptions(repo, opts.JobContext, jobs.Options{MaxConcurrent: opts.MaxConcurrentJobs, QueueCapacity: opts.QueueCapacity, SourcePolicy: opts.SourcePolicy, Metrics: opts.Metrics, Logger: opts.JobLogger})
	}
	h := &handler{service: services.JobService{Store: repo, Manager: manager, Policy: opts.SourcePolicy}}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if checker, ok := repo.(interface{ Ping() error }); ok {
			if err := checker.Ping(); err != nil {
				http.Error(w, "database unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	if opts.Metrics != nil {
		mux.Handle("GET /metrics", middleware.APIKey(opts.APIKey)(opts.Metrics))
	}
	mux.HandleFunc("GET "+HealthPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Pipeline API is running successfully!"))
	})
	mux.HandleFunc("GET "+OpenAPIPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(docs.OpenAPI)
	})
	mux.HandleFunc("GET "+SwaggerPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(docs.SwaggerHTML)
	})

	protected := func(next http.HandlerFunc) http.Handler {
		return limiter.Middleware(middleware.APIKey(opts.APIKey)(next))
	}
	mux.Handle("POST "+PipelinesPath, protected(h.create))
	mux.Handle("GET "+PipelinesPath, protected(h.list))
	mux.Handle("GET "+PipelinesPath+"/{id}", protected(h.get))
	mux.Handle("DELETE "+PipelinesPath+"/{id}", protected(h.delete))
	mux.Handle("GET "+PipelinesPath+"/{id}/results", protected(h.results))
	mux.Handle("GET "+PipelinesPath+"/{id}/errors", protected(h.errors))
	mux.Handle("GET "+PipelinesPath+"/{id}/progress", protected(h.progress))
	mux.Handle("PATCH "+PipelinesPath+"/{id}/cancel", protected(h.cancel))
	// Keep POST during migration so existing Postman collections remain valid.
	mux.Handle("POST "+PipelinesPath+"/{id}/cancel", protected(h.cancel))

	var routed http.Handler = mux
	if len(opts.CORSOrigins) > 0 {
		routed = middleware.CORS(opts.CORSOrigins)(routed)
	}
	wrapped := middleware.RequestID(
		middleware.Logging(logger)(
			middleware.Recovery(logger)(
				middleware.SecurityHeaders(routed),
			),
		),
	)
	if opts.Metrics != nil {
		return middleware.RecordMetrics(opts.Metrics)(wrapped)
	}
	return wrapped
}
