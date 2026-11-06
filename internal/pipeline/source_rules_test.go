package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestSourceSpecificEnrichmentAndConversionsEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.json")
	if err := os.WriteFile(path, []byte(`[{"gross":"15.5","city":"DELHI"},{"gross":"NaN","city":"Mumbai"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	spec := domain.JobSpec{
		Sources: []domain.SourceConfig{{Type: "json", Endpoint: path, Name: "customers",
			FieldMap:       map[string]string{"amount": "gross"},
			RequiredFields: []string{"amount", "city"},
			EnrichFields:   map[string]any{"region": "India"},
			TransformRules: map[string]any{"type_conversions": map[string]any{"amount": "number"}, "lowercase_fields": []any{"city"}},
		}},
		Aggregations: []domain.AggregationConfig{{Name: "total", Operation: "sum", Field: "amount"}},
	}
	result, err := Run(context.Background(), "rules-job", spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || len(result.Errors) != 1 || len(result.Aggregations) != 1 {
		t.Fatalf("got %+v", result)
	}
	rec := result.Records[0].Data
	if rec["amount"] != 15.5 || rec["city"] != "delhi" || rec["region"] != "India" {
		t.Fatalf("source transforms missing: %+v", rec)
	}
	if result.Aggregations[0].Value != 15.5 {
		t.Fatalf("incorrect aggregation: %+v", result.Aggregations)
	}
}
