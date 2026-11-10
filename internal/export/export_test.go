package export

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestWriteJSONReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "report.json")
	report := Report{
		JobID:        "job-1",
		Records:      []domain.Record{{ID: "1", Data: map[string]any{"amount": json.Number("5")}}},
		Aggregations: []domain.AggregationResult{{Name: "total", Operation: "count", Value: 1, Count: 1}},
		Errors:       []domain.PipelineError{{JobID: "job-1", Stage: "validation", Message: "bad record"}},
	}
	if err := Write(context.Background(), domain.ExportConfig{Type: "json", Path: path}, report); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got Report
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.JobID != "job-1" || len(got.Records) != 1 || len(got.Aggregations) != 1 || len(got.Errors) != 1 {
		t.Fatalf("unexpected JSON report: %+v", got)
	}
}

func TestWriteCSVRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.csv")
	stamp := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	report := Report{Records: []domain.Record{
		{ID: "r1", Source: "csv", Timestamp: stamp, Data: map[string]any{"name": "Alice, A", "amount": "10"}},
		{ID: "r2", Source: "json", Timestamp: stamp, Data: map[string]any{"city": "Delhi", "amount": 20}},
	}}
	if err := Write(context.Background(), domain.ExportConfig{Type: "csv", Path: path}, report); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	expectedHeader := []string{"record_id", "source", "timestamp", "amount", "city", "name"}
	if len(rows) != 3 || !reflect.DeepEqual(rows[0], expectedHeader) {
		t.Fatalf("unexpected CSV: %q", rows)
	}
	if rows[1][0] != "r1" || rows[1][3] != "10" || rows[1][4] != "" || rows[1][5] != "Alice, A" {
		t.Fatalf("bad first row: %q", rows[1])
	}
	if rows[2][0] != "r2" || rows[2][3] != "20" || rows[2][4] != "Delhi" || rows[2][5] != "" {
		t.Fatalf("bad second row: %q", rows[2])
	}
}

func TestWriteCancellationDoesNotCreateExport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "output.csv")
	err := Write(ctx, domain.ExportConfig{Type: "csv", Path: path}, Report{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled, got %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no output, got %v", err)
	}
}

func TestWriteInvalidJSONDoesNotReplaceExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	report := Report{Records: []domain.Record{{Data: map[string]any{"bad": make(chan int)}}}}
	if err := Write(context.Background(), domain.ExportConfig{Type: "json", Path: path}, report); err == nil {
		t.Fatal("expected JSON encoding error")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "old" {
		t.Fatalf("existing report changed: %q %v", got, err)
	}
}
