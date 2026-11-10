package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

func postJob(t *testing.T, router http.Handler, spec domain.JobSpec) string {
	t.Helper()
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, PipelinesPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create %d: %s", rec.Code, rec.Body.String())
	}
	var result struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || result.JobID == "" {
		t.Fatalf("create response: %+v %v", result, err)
	}
	return result.JobID
}

func fetch(t *testing.T, router http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRecorder()
	router.ServeHTTP(r, httptest.NewRequest(method, path, nil))
	return r
}

func awaitJob(t *testing.T, router http.Handler, id string) domain.JobStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response := fetch(t, router, http.MethodGet, PipelinesPath+"/"+id+"/progress")
		if response.Code != http.StatusOK {
			t.Fatalf("progress: %d %s", response.Code, response.Body.String())
		}
		var status domain.JobStatus
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		if status.State == domain.StateCompleted || status.State == domain.StateFailed || status.State == domain.StateCancelled {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not complete", id)
	return domain.JobStatus{}
}

func TestAPIEndToEndCSVJSONHTTPWithErrorsAndMetrics(t *testing.T) {
	dir := t.TempDir()
	csvFile := filepath.Join(dir, "data.csv")
	jsonFile := filepath.Join(dir, "data.json")
	if err := os.WriteFile(csvFile, []byte("id,city,amount\n1,Delhi,20\n2,Mumbai,invalid\n,Delhi,40\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jsonFile, []byte(`[{"id":"3","city":"Delhi","amount":30}]`), 0600); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"4","city":"Mumbai","amount":10}]`))
	}))
	defer source.Close()
	repo := repository.NewJobRepository()
	router := NewRouter(repo)
	id := postJob(t, router, domain.JobSpec{
		Sources:         []domain.SourceConfig{{Type: "csv", Endpoint: csvFile}, {Type: "json", Endpoint: jsonFile}, {Type: "api", Endpoint: source.URL}},
		ValidationRules: map[string]any{"required_fields": []string{"id"}},
		TransformRules:  map[string]any{"normalize": []string{"city"}, "type_conversions": map[string]any{"amount": "number"}},
		Aggregations:    []domain.AggregationConfig{{Name: "total", Operation: "sum", Field: "amount"}},
		Exports:         []domain.ExportConfig{{Type: "json", Path: filepath.Join(dir, "report.json")}, {Type: "csv", Path: filepath.Join(dir, "records.csv")}},
		WorkerPoolSizes: map[string]int{"validation": 3, "transformation": 2},
	})
	status := awaitJob(t, router, id)
	if status.State != domain.StateCompleted || status.RecordsRead != 5 || status.RecordsProcessed != 3 || status.RecordsFailed != 2 || status.ErrorCount != 2 || status.PercentComplete != 100 {
		t.Fatalf("unexpected progress: %+v", status)
	}
	if status.RatePerSecond <= 0 || len(status.StageLatencies) < 4 || status.StageLatencies["transformation"].Count != 4 {
		t.Fatalf("missing stage metrics %+v", status)
	}
	results := fetch(t, router, http.MethodGet, PipelinesPath+"/"+id+"/results")
	if results.Code != http.StatusOK {
		t.Fatalf("results: %d %s", results.Code, results.Body.String())
	}
	var output domain.JobOutput
	if err := json.Unmarshal(results.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Records) != 3 || len(output.Aggregations) != 1 || output.Aggregations[0].Value != 60 || len(output.FailedRecords) != 2 {
		t.Fatalf("unexpected output %+v", output)
	}
	failures := fetch(t, router, http.MethodGet, PipelinesPath+"/"+id+"/errors")
	if failures.Code != http.StatusOK {
		t.Fatalf("errors: %d %s", failures.Code, failures.Body.String())
	}
	var errorBody struct {
		Count         int                   `json:"count"`
		FailedRecords []domain.FailedRecord `json:"failed_records"`
	}
	if err := json.Unmarshal(failures.Body.Bytes(), &errorBody); err != nil {
		t.Fatal(err)
	}
	if errorBody.Count != 2 || len(errorBody.FailedRecords) != 2 {
		t.Fatalf("unexpected failures %+v", errorBody)
	}
	for _, name := range []string{"report.json", "records.csv"} {
		if info, err := os.Stat(filepath.Join(dir, name)); err != nil || info.Size() == 0 {
			t.Fatalf("export %s: %v", name, err)
		}
	}
	if response := fetch(t, router, http.MethodPost, PipelinesPath+"/"+id+"/cancel"); response.Code != http.StatusConflict {
		t.Fatalf("completed cancel: %d", response.Code)
	}
}

func TestAPICancelLongRunningJob(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-r.Context().Done()
	}))
	defer source.Close()
	router := NewRouter(repository.NewJobRepository())
	id := postJob(t, router, domain.JobSpec{Sources: []domain.SourceConfig{{Type: "api", Endpoint: source.URL}}})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("source was not contacted")
	}
	path := PipelinesPath + "/" + id
	if early := fetch(t, router, http.MethodGet, path+"/results"); early.Code != http.StatusConflict {
		t.Fatalf("early results: %d", early.Code)
	}
	if cancelled := fetch(t, router, http.MethodPost, path+"/cancel"); cancelled.Code != http.StatusAccepted {
		t.Fatalf("cancel: %d %s", cancelled.Code, cancelled.Body.String())
	}
	status := awaitJob(t, router, id)
	if status.State != domain.StateCancelled || status.EndTime == nil {
		t.Fatalf("expected CANCELLED, got %+v", status)
	}
	if response := fetch(t, router, http.MethodGet, path+"/results"); response.Code != http.StatusOK {
		t.Fatalf("cancelled results: %d", response.Code)
	}
	if response := fetch(t, router, http.MethodPost, path+"/cancel"); response.Code != http.StatusConflict {
		t.Fatalf("cancel twice: %d", response.Code)
	}
}

func TestAPITimeoutAndMissingJobs(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer source.Close()
	router := NewRouter(repository.NewJobRepository())
	id := postJob(t, router, domain.JobSpec{Sources: []domain.SourceConfig{{Type: "api", Endpoint: source.URL}}, TimeoutSeconds: 1})
	status := awaitJob(t, router, id)
	if status.State != domain.StateFailed || status.ErrorCount == 0 {
		t.Fatalf("expected failed timeout: %+v", status)
	}
	result := fetch(t, router, http.MethodGet, PipelinesPath+"/"+id+"/errors")
	if result.Code != 200 || !bytes.Contains(result.Body.Bytes(), []byte("deadline exceeded")) {
		t.Fatalf("timeout errors: %d %s", result.Code, result.Body.String())
	}
	for _, endpoint := range []string{"/results", "/errors", "/progress", "/cancel"} {
		method := http.MethodGet
		if endpoint == "/cancel" {
			method = http.MethodPost
		}
		got := fetch(t, router, method, PipelinesPath+"/missing"+endpoint)
		if got.Code != http.StatusNotFound {
			t.Fatalf("%s expected 404, got %d", endpoint, got.Code)
		}
	}
}

func TestAPIConcurrentJobs(t *testing.T) {
	input := filepath.Join(t.TempDir(), "batch.csv")
	if err := os.WriteFile(input, []byte("id,value\n1,10\n2,20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	router := NewRouter(repository.NewJobRepository())
	ids := make([]string, 8)
	for i := range ids {
		ids[i] = postJob(t, router, domain.JobSpec{Sources: []domain.SourceConfig{{Name: fmt.Sprintf("batch-%d", i), Type: "csv", Endpoint: input}}})
	}
	for _, id := range ids {
		status := awaitJob(t, router, id)
		if status.State != domain.StateCompleted || status.RecordsProcessed != 2 || status.RecordsRead != 2 {
			t.Fatalf("job %s %+v", id, status)
		}
	}
}

func TestAPICancelsWithParentContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var once sync.Once
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-r.Context().Done()
	}))
	defer source.Close()
	router := NewRouterWithOptions(repository.NewJobRepository(), Options{JobContext: ctx})
	id := postJob(t, router, domain.JobSpec{Sources: []domain.SourceConfig{{Type: "api", Endpoint: source.URL}}})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("did not start")
	}
	cancel()
	status := awaitJob(t, router, id)
	if status.State != domain.StateCancelled {
		t.Fatalf("expected cancelled after context, got %s", status.State)
	}
}
