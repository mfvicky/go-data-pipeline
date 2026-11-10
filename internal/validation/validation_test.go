package validation

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/ingestion"
)

func TestCheck(t *testing.T) {
	v, err := New(map[string]any{
		"required_fields": []any{"id", "city"},
		"field_types":     map[string]any{"id": "string", "active": "bool", "quantity": "int"},
		"min_values":      map[string]any{"amount": 0},
		"max_values":      map[string]any{"amount": 200},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		data    map[string]any
		wantErr string
	}{
		{"valid", map[string]any{"id": "1", "city": "Delhi", "amount": "125.50", "active": true, "quantity": json.Number("2")}, ""},
		{"missing required", map[string]any{"id": "1"}, "missing"},
		{"blank required", map[string]any{"id": "1", "city": " "}, "blank"},
		{"wrong boolean", map[string]any{"id": "1", "city": "Delhi", "active": "true"}, "bool"},
		{"wrong integer", map[string]any{"id": "1", "city": "Delhi", "quantity": json.Number("1.5")}, "int"},
		{"below minimum", map[string]any{"id": "1", "city": "Delhi", "amount": "-5"}, ">= 0"},
		{"above maximum", map[string]any{"id": "1", "city": "Delhi", "amount": "300"}, "<= 200"},
		{"invalid number", map[string]any{"id": "1", "city": "Delhi", "amount": "bad"}, ">= 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Check(domain.Record{Data: tt.data})
			if tt.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("got error %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	v, err := New(map[string]any{
		"required_fields": []any{"id", "city"},
		"min_values":      map[string]any{"amount": 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	records := []domain.Record{
		{ID: "csv:1", Data: map[string]any{"id": "1", "city": "Delhi", "amount": "125.50"}},
		{ID: "csv:2", Data: map[string]any{"id": "2", "city": "Mumbai", "amount": "-5"}},
		{ID: "csv:3", Data: map[string]any{"id": "3", "amount": "50"}},
		{ID: "csv:4", Data: map[string]any{"id": "4", "city": "Pune", "amount": "30"}},
	}

	valid, errors := v.Filter("job-123", records)
	if len(valid) != 2 || len(errors) != 2 {
		t.Fatalf("got %d valid and %d invalid, want 2 each", len(valid), len(errors))
	}
	if valid[0].ID != "csv:1" || valid[1].ID != "csv:4" {
		t.Fatalf("unexpected valid records: %+v", valid)
	}
	if errors[0].JobID != "job-123" || errors[0].Stage != "validation" || errors[0].RecordID != "csv:2" || errors[0].Timestamp.IsZero() {
		t.Fatalf("unexpected first error: %+v", errors[0])
	}
	if errors[1].RecordID != "csv:3" {
		t.Fatalf("unexpected second error: %+v", errors[1])
	}
}

func TestExistingCSV(t *testing.T) {
	source := domain.SourceConfig{
		Name:     "customers_csv",
		Type:     "csv",
		Endpoint: filepath.Join("..", "..", "testdata", "input", "customers.csv"),
	}
	records, err := ingestion.Read(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	v, err := New(map[string]any{
		"required_fields": []any{"id", "city"},
		"min_values":      map[string]any{"amount": 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	valid, invalid := v.Filter("job-1", records)
	if len(valid) != 3 || len(invalid) != 0 {
		t.Fatalf("got %d valid and %d invalid records", len(valid), len(invalid))
	}
}

func TestInvalidConfiguration(t *testing.T) {
	tests := []map[string]any{
		{"required_fields": "id"},
		{"required_fields": []any{1}},
		{"field_types": map[string]any{"amount": "decimal"}},
		{"min_values": map[string]any{"amount": "abc"}},
		{"min_values": map[string]any{"amount": 10}, "max_values": map[string]any{"amount": 5}},
	}
	for _, rules := range tests {
		if _, err := New(rules); err == nil {
			t.Fatalf("expected error for invalid rules: %v", rules)
		}
	}
}
