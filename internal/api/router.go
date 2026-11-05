package api

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/mfvicky/go-data-pipeline/docs"
	"github.com/mfvicky/go-data-pipeline/internal/middleware"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
)

type Options struct {
	APIKey             string
	RateLimitPerMinute int
	Logger             *slog.Logger
	SourcePolicy       *security.SourcePolicy
}

func NewRouter(repo *repository.JobRepository) http.Handler {
	return NewRouterWithOptions(repo, Options{})
}

func NewRouterWithOptions(repo *repository.JobRepository, opts Options) http.Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.RateLimitPerMinute <= 0 {
		opts.RateLimitPerMinute = 120
	}
	limiter := middleware.NewLimiter(opts.RateLimitPerMinute)
	h := &handler{repo: repo, sourcePolicy: opts.SourcePolicy}
	mux := http.NewServeMux()

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

	return middleware.RequestID(
		middleware.Logging(logger)(
			middleware.Recovery(logger)(
				middleware.SecurityHeaders(mux),
			),
		),
	)
}
