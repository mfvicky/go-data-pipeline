package api

import (
	"net/http"

	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

// NewRouter registers the API. Progress/results/errors/cancel will be
// added when the pipeline runner and observability are implemented.
func NewRouter(repo *repository.JobRepository) http.Handler {
	h := &handler{repo: repo}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Pipeline API is running successfully!"))
	})
	mux.HandleFunc("POST /api/v1/pipelines", h.create)
	mux.HandleFunc("GET /api/v1/pipelines", h.list)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", h.get)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", h.delete)
	return mux
}
