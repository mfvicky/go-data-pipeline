package monitor

import (
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrometheusMetrics(t *testing.T) {
	m := NewRegistry()
	m.JobStarted()
	m.Request("GET", 200)
	m.JobFinished(domain.StateCompleted, 4, 200*time.Millisecond)
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	for _, text := range []string{"pipeline_jobs_started_total 1", "pipeline_jobs_active 0", "pipeline_records_processed_total 4", "pipeline_http_requests_total{method=\"GET\",status_class=\"2xx\"} 1"} {
		if !strings.Contains(rec.Body.String(), text) {
			t.Fatalf("missing %s in %s", text, rec.Body.String())
		}
	}
}
