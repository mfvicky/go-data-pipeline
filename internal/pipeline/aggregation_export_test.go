package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/export"
)

func TestRunAggregationAndExports(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	csvPath := filepath.Join(dir, "records.csv")
	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{
			{Name: "csv", Type: "csv", Endpoint: "../../testdata/input/customers.csv"},
			{Name: "json", Type: "json", Endpoint: "../../testdata/input/customers.json"},
		},
		ValidationRules: map[string]any{"required_fields": []string{"id", "city"}},
		TransformRules:  map[string]any{"normalize": []string{"city"}},
		Aggregations: []domain.AggregationConfig{
			{Name: "by_city", Operation: "count", GroupBy: []string{"city"}},
			{Name: "total", Operation: "sum", Field: "amount", GroupBy: []string{"city"}},
			{Name: "average", Operation: "avg", Field: "amount"},
		},
		Exports: []domain.ExportConfig{
			{Type: "json", Path: jsonPath},
			{Type: "csv", Path: csvPath},
		},
		WorkerPoolSizes: map[string]int{"validation": 3, "transformation": 3},
	}
	result, err := Run(context.Background(), "agg-export-test", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 5 || len(result.Errors) != 0 || len(result.Aggregations) != 7 {
		t.Fatalf("unexpected result %+v", result)
	}
	var found bool
	for _, agg := range result.Aggregations {
		if agg.Name == "total" && agg.Group["city"] == "delhi" {
			found = true
			if math.Abs(agg.Value-515.75) > 1e-9 || agg.Count != 3 {
				t.Fatalf("unexpected Delhi total %+v", agg)
			}
		}
	}
	if !found {
		t.Fatal("Delhi sum missing")
	}
	content, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	var report export.Report
	if err := json.Unmarshal(content, &report); err != nil {
		t.Fatal(err)
	}
	if report.JobID != "agg-export-test" || len(report.Records) != 5 || len(report.Aggregations) != 7 {
		t.Fatalf("unexpected exported report %+v", report)
	}
	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil || len(rows) != 6 {
		t.Fatalf("expected CSV header and 5 rows, got %d, %v", len(rows), err)
	}
}

func TestRunRecordsInvalidForAggregationAreReported(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "items.csv")
	if err := os.WriteFile(input, []byte("id,amount\n1,10\n2,nope\n3,5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	spec := domain.JobSpec{
		Sources:      []domain.SourceConfig{{Type: "csv", Endpoint: input}},
		Aggregations: []domain.AggregationConfig{{Name: "total", Operation: "sum", Field: "amount"}},
	}
	result, err := Run(context.Background(), "invalid-agg", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 3 || len(result.Aggregations) != 1 || result.Aggregations[0].Value != 15 {
		t.Fatalf("unexpected aggregation %+v", result)
	}
	if len(result.Errors) != 1 || result.Errors[0].Stage != "aggregation" || result.Errors[0].RecordID == "" {
		t.Fatalf("expected one per-record aggregation error, got %+v", result.Errors)
	}
}

func TestRunReturnsExportError(t *testing.T) {
	path := t.TempDir() // A directory cannot be replaced by a report file.
	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{{Type: "csv", Endpoint: "../../testdata/input/customers.csv"}},
		Exports: []domain.ExportConfig{{Type: "json", Path: path}},
	}
	result, err := Run(context.Background(), "export-error", spec)
	if err == nil || len(result.Records) != 3 {
		t.Fatalf("expected export error with processed records, got result=%+v err=%v", result, err)
	}
}
