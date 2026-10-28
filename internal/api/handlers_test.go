package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

const exampleSpec = `{
  "sources": [
    {"name":"local_csv", "type":"csv", "endpoint":"testdata/input/customers.csv"},
    {"name":"users", "type":"api", "endpoint":"https://jsonplaceholder.typicode.com/users"}
  ],
  "validation_rules": {"required": ["id"]},
  "transform_rules": {"normalize": ["city"]},
  "aggregations": [{"name":"per_city", "operation":"sum", "field":"amount", "group_by":["city"]}],
  "worker_pool_sizes": {"validation": 4, "transformation": 3},
  "timeout_seconds": 30,
  "exports": [{"type":"json", "path":"output/report.json"}]
}`

func testRequest(t *testing.T, server http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec
}

func TestJobAPI_CRUD(t *testing.T) {
	mux := NewRouter(repository.NewJobRepository())
	if resp := testRequest(t, mux, "GET", "/health", ""); resp.Code != http.StatusOK {
		t.Fatalf("health: %d", resp.Code)
	}
	empty := testRequest(t, mux, "GET", "/api/v1/pipelines", "")
	if empty.Code != http.StatusOK || !bytes.Contains(empty.Body.Bytes(), []byte(`"count":0`)) || !bytes.Contains(empty.Body.Bytes(), []byte(`"jobs":[]`)) {
		t.Fatalf("empty job listing: %d %s", empty.Code, empty.Body.String())
	}
	created := testRequest(t, mux, "POST", "/api/v1/pipelines", exampleSpec)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var response struct {
		JobID   string          `json:"job_id"`
		State   domain.JobState `json:"state"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.JobID) != 32 || response.State != domain.StatePending || response.Message == "" {
		t.Fatalf("unexpected create response: %+v", response)
	}
	path := "/api/v1/pipelines/" + response.JobID
	if location := created.Header().Get("Location"); location != path {
		t.Fatalf("Location: %q, want %q", location, path)
	}
	get := testRequest(t, mux, "GET", path, "")
	if get.Code != http.StatusOK {
		t.Fatalf("get: %d %s", get.Code, get.Body.String())
	}
	var details domain.JobDetails
	if err := json.Unmarshal(get.Body.Bytes(), &details); err != nil {
		t.Fatal(err)
	}
	if details.Spec.Sources[1].Type != "api" || details.Status.JobID != response.JobID || details.Status.CreatedAt.IsZero() || details.Status.State != domain.StatePending {
		t.Fatalf("unexpected details: %+v", details)
	}
	if details.Status.StartTime != nil || details.Status.RecordsProcessed != 0 {
		t.Fatal("Day 1 should not execute pipeline jobs")
	}
	listed := testRequest(t, mux, "GET", "/api/v1/pipelines", "")
	if listed.Code != http.StatusOK || !bytes.Contains(listed.Body.Bytes(), []byte(`"count":1`)) {
		t.Fatalf("list: %d %s", listed.Code, listed.Body.String())
	}
	if removed := testRequest(t, mux, "DELETE", path, ""); removed.Code != http.StatusNoContent || removed.Body.Len() != 0 {
		t.Fatalf("delete: %d %s", removed.Code, removed.Body.String())
	}
	if missing := testRequest(t, mux, "GET", path, ""); missing.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", missing.Code)
	}
	if missing := testRequest(t, mux, "DELETE", path, ""); missing.Code != http.StatusNotFound {
		t.Fatalf("expected 404 deleting nonexistent job, got %d", missing.Code)
	}
}

func TestJobAPI_Errors(t *testing.T) {
	mux := NewRouter(repository.NewJobRepository())
	cases := []struct {
		name, body string
		status     int
	}{
		{"bad json", `{`, http.StatusBadRequest},
		{"unknown field", `{"sources":[{"type":"csv","endpoint":"a.csv"}],"unknown":123}`, http.StatusBadRequest},
		{"missing sources", `{}`, http.StatusBadRequest},
		{"invalid source", `{"sources":[{"type":"api","endpoint":"wrong"}]}`, http.StatusBadRequest},
		{"trailing object", exampleSpec + `{}`, http.StatusBadRequest},
		{"oversized", strings.Repeat(" ", maxRequestBytes+1) + exampleSpec, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := testRequest(t, mux, "POST", "/api/v1/pipelines", tc.body)
			if got.Code != tc.status || !bytes.Contains(got.Body.Bytes(), []byte(`"error":`)) {
				t.Fatalf("status=%d body=%s", got.Code, got.Body.String())
			}
		})
	}
	req := httptest.NewRequest("POST", "/api/v1/pipelines", strings.NewReader(exampleSpec))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content-type: %d", rec.Code)
	}
	if got := testRequest(t, mux, "GET", "/api/v1/pipelines/does-not-exist", ""); got.Code != 404 {
		t.Fatalf("missing: %d", got.Code)
	}
	if got := testRequest(t, mux, "PATCH", "/api/v1/pipelines", ""); got.Code != 405 {
		t.Fatalf("method mismatch: %d", got.Code)
	}
}

func TestDeleteRejectsRunningJob(t *testing.T) {
	r := repository.NewJobRepository()
	id := "running-job"
	if err := r.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StateRunning}); err != nil {
		t.Fatal(err)
	}
	resp := testRequest(t, NewRouter(r), "DELETE", fmt.Sprintf("/api/v1/pipelines/%s", id), "")
	if resp.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d", resp.Code)
	}
	if _, err := r.GetStatus(id); err != nil {
		t.Fatalf("job should still exist: %v", err)
	}
}
