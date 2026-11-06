package middleware

import (
	"github.com/mfvicky/go-data-pipeline/internal/monitor"
	"net/http"
)

func RecordMetrics(metrics *monitor.Registry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			wrapped := &responseWriter{ResponseWriter: w}
			next.ServeHTTP(wrapped, r)
			status := wrapped.status
			if status == 0 {
				status = http.StatusOK
			}
			metrics.Request(r.Method, status)
		})
	}
}
