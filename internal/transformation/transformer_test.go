package transformation

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/ingestion"
)

func TestApply(t *testing.T) {
	transformer, err := New(map[string]any{
		"trim_strings":     true,
		"normalize":        []any{"city"},
		"uppercase_fields": []any{"code"},
		"type_conversions": map[string]any{
			"amount": "number", "quantity": "int", "active": "bool", "id": "string",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	created := time.Now().UTC()
	record := domain.Record{
		ID: "customers:1", Source: "customers", Timestamp: created,
		Data: map[string]any{
			"id": json.Number("7"), "name": " Alice ", "city": " DeLhI ",
			"code": " ab ", "amount": " 125.50 ", "quantity": "3", "active": "true",
		},
	}

	result, err := transformer.Apply(record)
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != record.ID || result.Source != record.Source || !result.Timestamp.Equal(created) {
		t.Fatal("record metadata should remain unchanged")
	}
	for field, expected := range map[string]any{
		"id": "7", "name": "Alice", "city": "delhi", "code": "AB",
		"amount": 125.5, "quantity": int64(3), "active": true,
	} {
		if result.Data[field] != expected {
			t.Errorf("%s: got %v, want %v", field, result.Data[field], expected)
		}
	}
	if record.Data["name"] != " Alice " || record.Data["amount"] != " 125.50 " {
		t.Fatal("original record should not be modified")
	}
}

func TestInvalidConversion(t *testing.T) {
	transformer, err := New(map[string]any{
		"type_conversions": map[string]any{"amount": "number"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transformer.Apply(domain.Record{Data: map[string]any{"amount": "invalid"}}); err == nil {
		t.Fatal("expected an error for invalid numeric input")
	}
}

func TestInvalidConfiguration(t *testing.T) {
	cases := []map[string]any{
		{"trim_strings": "yes"},
		{"normalize": "city"},
		{"lowercase_fields": []any{42}},
		{"normalize": []any{"city"}, "uppercase_fields": []any{"city"}},
		{"type_conversions": map[string]any{"amount": "currency"}},
	}
	for _, rules := range cases {
		if _, err := New(rules); err == nil {
			t.Errorf("expected invalid config error: %v", rules)
		}
	}
}

func TestExistingCSV(t *testing.T) {
	file := filepath.Join("..", "..", "testdata", "input", "customers.csv")
	records, err := ingestion.Read(context.Background(), domain.SourceConfig{
		Name: "customers", Type: "csv", Endpoint: file,
	})
	if err != nil {
		t.Fatal(err)
	}
	transformer, err := New(map[string]any{
		"normalize":        []any{"city"},
		"type_conversions": map[string]any{"amount": "number"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	for _, record := range records {
		result, err := transformer.Apply(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := result.Data["amount"].(float64); !ok {
			t.Fatalf("amount should be a number, got %T", result.Data["amount"])
		}
	}
}
