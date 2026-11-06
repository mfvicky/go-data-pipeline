package api

import (
	"encoding/json"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/monitor"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHardeningMetricsPaginationAndReadiness(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "source.csv")
	if err := os.WriteFile(file, []byte("id,city\n1,Delhi\n2,Mumbai\n3,Delhi\n"), 0600); err != nil {
		t.Fatal(err)
	}
	registry := monitor.NewRegistry()
	router := NewRouterWithOptions(repository.NewJobRepository(), Options{Metrics: registry, CORSOrigins: []string{"https://example.com"}})
	spec := domain.JobSpec{Sources: []domain.SourceConfig{{Type: "csv", Endpoint: file}}}
	id := postJob(t, router, spec)
	status := awaitJob(t, router, id)
	if status.State != domain.StateCompleted {
		t.Fatal(status.State)
	}
	for _, path := range []string{"/ready", "/metrics"} {
		res := fetch(t, router, http.MethodGet, path)
		if res.Code != http.StatusOK {
			t.Fatalf("%s -> %d", path, res.Code)
		}
	}
	metrics := fetch(t, router, http.MethodGet, "/metrics")
	if !strings.Contains(metrics.Body.String(), "pipeline_jobs_finished_total{state=\"completed\"} 1") {
		t.Fatal(metrics.Body.String())
	}
	listed := fetch(t, router, http.MethodGet, PipelinesPath+"?limit=1&offset=0")
	var page struct{ Count, Total int }
	if err := json.Unmarshal(listed.Body.Bytes(), &page); err != nil || page.Count != 1 || page.Total != 1 {
		t.Fatalf("page: %+v %v", page, err)
	}
	results := fetch(t, router, http.MethodGet, PipelinesPath+"/"+id+"/results?limit=1")
	var data struct {
		Records      []domain.Record `json:"records"`
		TotalRecords int             `json:"total_records"`
	}
	if err := json.Unmarshal(results.Body.Bytes(), &data); err != nil || len(data.Records) != 1 || data.TotalRecords != 3 {
		t.Fatalf("results: %+v %v", data, err)
	}
	bad := fetch(t, router, http.MethodGet, PipelinesPath+"?limit=2000")
	if bad.Code != http.StatusBadRequest {
		t.Fatal(bad.Code)
	}
	preflight := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, PipelinesPath, nil)
	req.Header.Set("Origin", "https://example.com")
	router.ServeHTTP(preflight, req)
	if preflight.Code != http.StatusNoContent || preflight.Header().Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Fatalf("preflight: %d %v", preflight.Code, preflight.Header())
	}
}
