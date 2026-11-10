package api

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

// These integration tests use a real local HTTP server and exercise the API,
// job manager, ingestion, workers, repository, aggregation and exports together.
func integrationServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(NewRouterWithOptions(repository.NewJobRepository(), Options{
		RateLimitPerMinute: 10000,
	}))
	t.Cleanup(server.Close)
	return server
}

func integrationRequest(client *http.Client, method, url string, body []byte, contentType string) (int, []byte, error) {
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(response.Body)
	return response.StatusCode, payload, err
}

func integrationCreate(client *http.Client, baseURL string, spec domain.JobSpec) (string, error) {
	payload, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	status, data, err := integrationRequest(client, http.MethodPost, baseURL+PipelinesPath, payload, "application/json")
	if err != nil {
		return "", err
	}
	if status != http.StatusCreated {
		return "", fmt.Errorf("create job: HTTP %d: %s", status, data)
	}
	var created struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(data, &created); err != nil {
		return "", err
	}
	if created.JobID == "" {
		return "", fmt.Errorf("create job returned empty job_id")
	}
	return created.JobID, nil
}

func integrationReadJSON(t *testing.T, client *http.Client, method, url string, out any) int {
	t.Helper()
	status, data, err := integrationRequest(client, method, url, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("%s %s: HTTP %d: invalid JSON %q: %v", method, url, status, data, err)
	}
	return status
}

func integrationWait(t *testing.T, client *http.Client, baseURL, id string) domain.JobStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var progress domain.JobStatus
		if code := integrationReadJSON(t, client, http.MethodGet, baseURL+PipelinesPath+"/"+id+"/progress", &progress); code != http.StatusOK {
			t.Fatalf("get progress: HTTP %d", code)
		}
		switch progress.State {
		case domain.StateCompleted, domain.StateCancelled, domain.StateFailed:
			return progress
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach a terminal state in 10 seconds", id)
	return domain.JobStatus{}
}

func integrationWriteFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestIntegrationRealHTTPEachSourceType(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Source") != "integration" {
			http.Error(w, "missing source header", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"payload":{"items":[{"id":"1","city":"Delhi","amount":10},{"id":"2","city":"Delhi","amount":20}]}}`))
	}))
	defer remote.Close()

	csvPath := integrationWriteFile(t, "source.csv", "id,city,amount\n1,Delhi,10\n2,Delhi,20\n")
	jsonPath := integrationWriteFile(t, "source.json", `{"payload":{"items":[{"id":"1","city":"Delhi","amount":10},{"id":"2","city":"Delhi","amount":20}]}}`)

	cases := []struct {
		name   string
		source domain.SourceConfig
	}{
		{"CSV file", domain.SourceConfig{Name: "csv-input", Type: "csv", Endpoint: csvPath}},
		{"JSON nested records", domain.SourceConfig{Name: "json-input", Type: "json", Endpoint: jsonPath, RecordPath: "payload.items"}},
		{"HTTP API", domain.SourceConfig{Name: "http-input", Type: "api", Endpoint: remote.URL, RecordPath: "payload.items", Headers: map[string]string{"X-Test-Source": "integration"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := integrationServer(t)
			client := server.Client()
			client.Timeout = 5 * time.Second
			dir := t.TempDir()
			jsonExport := filepath.Join(dir, "report.json")
			csvExport := filepath.Join(dir, "records.csv")
			spec := domain.JobSpec{
				Sources:         []domain.SourceConfig{tc.source},
				ValidationRules: map[string]any{"required_fields": []string{"id", "amount"}},
				TransformRules:  map[string]any{"normalize": []string{"city"}},
				Aggregations:    []domain.AggregationConfig{{Name: "count_by_city", Operation: "count", GroupBy: []string{"city"}}, {Name: "sum_amount", Operation: "sum", Field: "amount"}},
				Exports:         []domain.ExportConfig{{Type: "json", Path: jsonExport}, {Type: "csv", Path: csvExport}},
				WorkerPoolSizes: map[string]int{"validation": 3, "transformation": 2},
			}
			id, err := integrationCreate(client, server.URL, spec)
			if err != nil {
				t.Fatal(err)
			}
			progress := integrationWait(t, client, server.URL, id)
			if progress.State != domain.StateCompleted || progress.RecordsRead != 2 || progress.RecordsProcessed != 2 || progress.RecordsFailed != 0 || progress.ErrorCount != 0 {
				t.Fatalf("unexpected progress: %+v", progress)
			}
			if progress.PercentComplete != 100 || progress.RatePerSecond <= 0 || len(progress.StageLatencies) == 0 {
				t.Fatalf("incomplete progress metrics: %+v", progress)
			}
			base := server.URL + PipelinesPath + "/" + id
			var output domain.JobOutput
			if code := integrationReadJSON(t, client, http.MethodGet, base+"/results", &output); code != http.StatusOK {
				t.Fatalf("results: HTTP %d", code)
			}
			if len(output.Records) != 2 || len(output.Aggregations) != 2 || len(output.Errors) != 0 {
				t.Fatalf("unexpected result: %+v", output)
			}
			for _, a := range output.Aggregations {
				if a.Value != 2 && a.Value != 30 {
					t.Fatalf("unexpected aggregation %+v", a)
				}
			}
			var reported struct {
				Count int `json:"count"`
			}
			if code := integrationReadJSON(t, client, http.MethodGet, base+"/errors", &reported); code != http.StatusOK || reported.Count != 0 {
				t.Fatalf("errors: HTTP %d count %d", code, reported.Count)
			}
			var report domain.JobOutput
			data, err := os.ReadFile(jsonExport)
			if err != nil || json.Unmarshal(data, &report) != nil || len(report.Records) != 2 {
				t.Fatalf("JSON export invalid: %v: %s", err, data)
			}
			file, err := os.Open(csvExport)
			if err != nil {
				t.Fatal(err)
			}
			rows, readErr := csv.NewReader(file).ReadAll()
			closeErr := file.Close()
			if readErr != nil || closeErr != nil || len(rows) != 3 {
				t.Fatalf("CSV export invalid: rows=%v read=%v close=%v", rows, readErr, closeErr)
			}
		})
	}
}

func TestIntegrationMalformedJobRequests(t *testing.T) {
	server := integrationServer(t)
	client := server.Client()
	cases := []struct {
		name, body, contentType string
		want                    int
	}{
		{"malformed JSON", `{"sources": [`, "application/json", http.StatusBadRequest},
		{"unknown field", `{"sources":[{"type":"csv","endpoint":"file.csv"}],"unexpected":true}`, "application/json", http.StatusBadRequest},
		{"missing source", `{"sources":[]}`, "application/json", http.StatusBadRequest},
		{"invalid source type", `{"sources":[{"type":"unknown","endpoint":"data.txt"}]}`, "application/json", http.StatusBadRequest},
		{"trailing JSON", `{"sources":[{"type":"csv","endpoint":"file.csv"}]} {}`, "application/json", http.StatusBadRequest},
		{"wrong content type", `{"sources":[]}`, "text/plain", http.StatusUnsupportedMediaType},
		{"payload too large", strings.Repeat(" ", maxRequestBytes+1) + `{"sources":[]}`, "application/json", http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body, err := integrationRequest(client, http.MethodPost, server.URL+PipelinesPath, []byte(tc.body), tc.contentType)
			if err != nil {
				t.Fatal(err)
			}
			if status != tc.want || !bytes.Contains(body, []byte(`"error"`)) {
				t.Fatalf("want HTTP %d, got HTTP %d, body=%s", tc.want, status, body)
			}
		})
	}
	var jobs struct {
		Count int `json:"count"`
	}
	if status := integrationReadJSON(t, client, http.MethodGet, server.URL+PipelinesPath, &jobs); status != http.StatusOK || jobs.Count != 0 {
		t.Fatalf("invalid requests created jobs: HTTP %d count=%d", status, jobs.Count)
	}
}

func TestIntegrationMalformedSourceData(t *testing.T) {
	brokenCSV := integrationWriteFile(t, "broken.csv", "id,amount\n1,10\n2\n")
	brokenJSON := integrationWriteFile(t, "broken.json", `{"data":[`)
	badHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	defer badHTTP.Close()
	cases := []struct {
		name   string
		source domain.SourceConfig
	}{
		{"invalid CSV row", domain.SourceConfig{Type: "csv", Endpoint: brokenCSV}},
		{"malformed JSON file", domain.SourceConfig{Type: "json", Endpoint: brokenJSON}},
		{"HTTP upstream error", domain.SourceConfig{Type: "api", Endpoint: badHTTP.URL}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := integrationServer(t)
			client := server.Client()
			id, err := integrationCreate(client, server.URL, domain.JobSpec{Sources: []domain.SourceConfig{tc.source}})
			if err != nil {
				t.Fatal(err)
			}
			status := integrationWait(t, client, server.URL, id)
			if status.ErrorCount == 0 || (tc.name != "invalid CSV row" && status.RecordsProcessed != 0) || (tc.name == "invalid CSV row" && status.RecordsProcessed != 1) {
				t.Fatalf("expected ingestion failure: %+v", status)
			}
			var errorsBody struct {
				Count  int                    `json:"count"`
				Errors []domain.PipelineError `json:"errors"`
			}
			base := server.URL + PipelinesPath + "/" + id
			if code := integrationReadJSON(t, client, http.MethodGet, base+"/errors", &errorsBody); code != http.StatusOK || errorsBody.Count == 0 || errorsBody.Errors[0].Stage != "ingestion" {
				t.Fatalf("unexpected ingestion errors: HTTP %d %+v", code, errorsBody)
			}
		})
	}
}

func TestIntegrationCancellationAndTimeoutOverHTTP(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel bool
	}{{"explicit cancellation", true}, {"timeout", false}} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			var once sync.Once
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				once.Do(func() { close(started) })
				<-r.Context().Done()
			}))
			defer upstream.Close()
			server := integrationServer(t)
			client := server.Client()
			client.Timeout = 5 * time.Second
			spec := domain.JobSpec{Sources: []domain.SourceConfig{{Type: "api", Endpoint: upstream.URL}}}
			if !tc.cancel {
				spec.TimeoutSeconds = 1
			}
			id, err := integrationCreate(client, server.URL, spec)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream never received the request")
			}
			base := server.URL + PipelinesPath + "/" + id
			if tc.cancel {
				var pending map[string]any
				if code := integrationReadJSON(t, client, http.MethodGet, base+"/results", &pending); code != http.StatusConflict {
					t.Fatalf("expected results 409 before completion, got HTTP %d", code)
				}
				code, _, err := integrationRequest(client, http.MethodPost, base+"/cancel", nil, "")
				if err != nil || code != http.StatusAccepted {
					t.Fatalf("cancel: HTTP %d %v", code, err)
				}
			}
			status := integrationWait(t, client, server.URL, id)
			if tc.cancel {
				if status.State != domain.StateCancelled {
					t.Fatalf("expected CANCELLED: %+v", status)
				}
			} else {
				if status.State != domain.StateFailed || status.ErrorCount == 0 {
					t.Fatalf("expected timeout FAILED: %+v", status)
				}
			}
			if status.EndTime == nil {
				t.Fatalf("missing end time: %+v", status)
			}
			if code, _, err := integrationRequest(client, http.MethodPost, base+"/cancel", nil, ""); err != nil || code != http.StatusConflict {
				t.Fatalf("second cancel should be 409: code=%d err=%v", code, err)
			}
		})
	}
}

func TestIntegrationParallelHTTPJobsKeepResultsIsolated(t *testing.T) {
	server := integrationServer(t)
	client := server.Client()
	client.Timeout = 5 * time.Second
	const jobsCount = 12
	type submitted struct {
		id  string
		err error
	}
	jobs := make([]submitted, jobsCount)
	filePaths := make([]string, jobsCount)
	for i := range filePaths {
		filePaths[i] = integrationWriteFile(t, fmt.Sprintf("data-%02d.json", i), fmt.Sprintf(`[{"id":"job-%d", "value": %d}]`, i, i+1))
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range jobs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			jobs[i].id, jobs[i].err = integrationCreate(client, server.URL, domain.JobSpec{
				Sources:      []domain.SourceConfig{{Name: fmt.Sprintf("input-%d", i), Type: "json", Endpoint: filePaths[i]}},
				Aggregations: []domain.AggregationConfig{{Name: "total", Operation: "sum", Field: "value"}},
			})
		}(i)
	}
	close(start)
	wg.Wait()
	seen := make(map[string]struct{}, jobsCount)
	for i, job := range jobs {
		if job.err != nil {
			t.Fatalf("submit %d: %v", i, job.err)
		}
		if _, exists := seen[job.id]; exists {
			t.Fatalf("duplicate job ID %s", job.id)
		}
		seen[job.id] = struct{}{}
	}
	for i, job := range jobs {
		status := integrationWait(t, client, server.URL, job.id)
		if status.State != domain.StateCompleted || status.RecordsProcessed != 1 || status.ErrorCount != 0 {
			t.Fatalf("job %d progress: %+v", i, status)
		}
		var output domain.JobOutput
		if code := integrationReadJSON(t, client, http.MethodGet, server.URL+PipelinesPath+"/"+job.id+"/results", &output); code != http.StatusOK {
			t.Fatalf("job %d results HTTP %d", i, code)
		}
		if output.JobID != job.id || len(output.Records) != 1 || len(output.Aggregations) != 1 || output.Aggregations[0].Value != float64(i+1) || output.Records[0].Data["id"] != fmt.Sprintf("job-%d", i) {
			t.Fatalf("job %d result leaked/mismatched: %+v", i, output)
		}
	}

	// Concurrent GET requests exercise thread-safe status/result snapshots.
	readErrors := make(chan error, jobsCount*2)
	for i, job := range jobs {
		wg.Add(1)
		go func(i int, jobID string) {
			defer wg.Done()
			base := server.URL + PipelinesPath + "/" + jobID
			for _, endpoint := range []string{"/progress", "/results"} {
				code, data, err := integrationRequest(client, http.MethodGet, base+endpoint, nil, "")
				if err != nil || code != http.StatusOK {
					readErrors <- fmt.Errorf("job %d %s: HTTP %d: %v: %s", i, endpoint, code, err, data)
					continue
				}
				var envelope struct {
					JobID string `json:"job_id"`
				}
				if err := json.Unmarshal(data, &envelope); err != nil || envelope.JobID != jobID {
					readErrors <- fmt.Errorf("job %d %s: incorrect job ID %q: %v", i, endpoint, envelope.JobID, err)
				}
			}
		}(i, job.id)
	}
	wg.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Error(err)
	}
}
