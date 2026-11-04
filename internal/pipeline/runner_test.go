package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestRunCSVAndJSON(t *testing.T) {
	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{
			{Name: "customers_csv", Type: "csv", Endpoint: "../../testdata/input/customers.csv"},
			{Name: "customers_json", Type: "json", Endpoint: "../../testdata/input/customers.json"},
		},
		ValidationRules: map[string]any{"required_fields": []string{"id", "city"}},
		TransformRules: map[string]any{
			"trim_strings":     true,
			"normalize":        []string{"city"},
			"type_conversions": map[string]any{"amount": "number"},
		},
		WorkerPoolSizes: map[string]int{"validation": 3, "transformation": 2},
	}

	result, err := Run(context.Background(), "test-job", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 5 || len(result.Errors) != 0 {
		t.Fatalf("expected 5 records and 0 errors, got %d records, %d errors", len(result.Records), len(result.Errors))
	}

	cities := map[string]int{}
	for _, record := range result.Records {
		if _, ok := record.Data["amount"].(float64); !ok {
			t.Fatalf("amount was not converted: %+v", record.Data)
		}
		city, ok := record.Data["city"].(string)
		if !ok {
			t.Fatalf("invalid city: %+v", record.Data)
		}
		cities[city]++
	}
	if cities["delhi"] != 3 || cities["mumbai"] != 1 || cities["bengaluru"] != 1 {
		t.Fatalf("unexpected normalized cities: %v", cities)
	}
	t.Logf("processed=%d, errors=%d, cities=%v", len(result.Records), len(result.Errors), cities)
}

func TestRunFiltersInvalidRecords(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "customers.csv")
	data := "id,city,amount\n1, Delhi ,125.50\n,Mumbai,50\n3,Pune,invalid\n4, Bengaluru ,40\n"
	if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	spec := domain.JobSpec{
		Sources:         []domain.SourceConfig{{Name: "csv", Type: "csv", Endpoint: filename}},
		ValidationRules: map[string]any{"required_fields": []string{"id", "city"}},
		TransformRules: map[string]any{
			"trim_strings":     true,
			"normalize":        []string{"city"},
			"type_conversions": map[string]any{"amount": "number"},
		},
		WorkerPoolSizes: map[string]int{"validation": 2, "transformation": 2},
	}

	result, err := Run(context.Background(), "test-job", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 || len(result.Errors) != 2 {
		t.Fatalf("expected 2 records and 2 errors, got %d records and %d errors", len(result.Records), len(result.Errors))
	}
	stages := make(map[string]int)
	for _, failure := range result.Errors {
		stages[failure.Stage]++
	}
	if stages["validation"] != 1 || stages["transformation"] != 1 {
		t.Fatalf("unexpected errors: %+v", result.Errors)
	}
	t.Logf("processed=%d, rejected=%d, stages=%v", len(result.Records), len(result.Errors), stages)
}

func TestRunSourceErrorDoesNotBlockOthers(t *testing.T) {
	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{
			{Name: "missing", Type: "csv", Endpoint: filepath.Join(t.TempDir(), "missing.csv")},
			{Name: "customers_json", Type: "json", Endpoint: "../../testdata/input/customers.json"},
		},
	}
	result, err := Run(context.Background(), "test-job", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 || len(result.Errors) != 1 || result.Errors[0].Stage != "ingestion" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRunCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{{Name: "csv", Type: "csv", Endpoint: "../../testdata/input/customers.csv"}},
	}
	_, err := Run(ctx, "test-job", spec)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRunCancellationStopsHTTPReader(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(finished)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan error, 1)
	go func() {
		_, err := Run(ctx, "cancel-test", domain.JobSpec{
			Sources:         []domain.SourceConfig{{Name: "slow", Type: "api", Endpoint: server.URL}},
			WorkerPoolSizes: map[string]int{"validation": 2, "transformation": 2},
		})
		resultCh <- err
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP reader did not start")
	}
	cancel()

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline did not stop after cancellation")
	}

	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP reader did not stop after cancellation")
	}
}

func TestRunTimeoutStopsHTTPReader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	_, err := Run(context.Background(), "timeout-test", domain.JobSpec{
		Sources:        []domain.SourceConfig{{Name: "slow", Type: "api", Endpoint: server.URL}},
		TimeoutSeconds: 1,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}
