package aggregation

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestComputeGroupedCountSumAvg(t *testing.T) {
	records := []domain.Record{
		{ID: "1", Data: map[string]any{"city": "delhi", "region": "north", "amount": "10.5"}},
		{ID: "2", Data: map[string]any{"city": "delhi", "region": "north", "amount": json.Number("20.5")}},
		{ID: "3", Data: map[string]any{"city": "mumbai", "region": "west", "amount": 4}},
	}
	configs := []domain.AggregationConfig{
		{Name: "count", Operation: "count", GroupBy: []string{"city", "region"}},
		{Name: "sum", Operation: "sum", Field: "amount", GroupBy: []string{"city"}},
		{Name: "avg", Operation: "avg", Field: "amount", GroupBy: []string{"city"}},
		{Name: "total", Operation: "count"},
	}
	results, failures, err := Compute(context.Background(), "job", records, configs)
	if err != nil || len(failures) != 0 {
		t.Fatalf("compute: results=%+v failures=%+v err=%v", results, failures, err)
	}
	if len(results) != 7 {
		t.Fatalf("expected 7 results, got %+v", results)
	}
	want := map[string]map[string]float64{
		"count": {"delhi": 2, "mumbai": 1},
		"sum":   {"delhi": 31, "mumbai": 4},
		"avg":   {"delhi": 15.5, "mumbai": 4},
	}
	seen := 0
	for _, result := range results {
		if result.Name == "total" {
			if result.Value != 3 || result.Count != 3 || len(result.Group) != 0 {
				t.Fatalf("unexpected ungrouped count %+v", result)
			}
			continue
		}
		city := result.Group["city"].(string)
		if math.Abs(result.Value-want[result.Name][city]) > 1e-9 {
			t.Fatalf("%s/%s: got %v, want %v", result.Name, city, result.Value, want[result.Name][city])
		}
		if result.Name == "count" && result.Group["region"] == nil {
			t.Fatalf("missing second group field: %+v", result)
		}
		seen++
	}
	if seen != 6 {
		t.Fatalf("expected 6 grouped results, got %d", seen)
	}
}

func TestComputeSkipsInvalidNumericRecords(t *testing.T) {
	records := []domain.Record{
		{ID: "ok", Data: map[string]any{"city": "delhi", "amount": "12"}},
		{ID: "invalid", Data: map[string]any{"city": "delhi", "amount": "not-a-number"}},
		{ID: "missing", Data: map[string]any{"city": "delhi"}},
		{ID: "no-city", Data: map[string]any{"amount": 7}},
	}
	configs := []domain.AggregationConfig{{Name: "sum", Operation: "sum", Field: "amount", GroupBy: []string{"city"}}}
	results, failures, err := Compute(context.Background(), "job-5", records, configs)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Value != 12 || results[0].Count != 1 {
		t.Fatalf("unexpected results %+v", results)
	}
	if len(failures) != 3 {
		t.Fatalf("expected 3 errors, got %+v", failures)
	}
	for _, failure := range failures {
		if failure.JobID != "job-5" || failure.Stage != "aggregation" || failure.RecordID == "" || failure.Message == "" {
			t.Fatalf("incomplete failure %+v", failure)
		}
	}
}

func TestComputeEmptyAndCancelled(t *testing.T) {
	configs := []domain.AggregationConfig{{Name: "empty", Operation: "avg", Field: "value"}}
	results, failures, err := Compute(context.Background(), "job", nil, configs)
	if err != nil || len(failures) != 0 || len(results) != 1 || results[0].Count != 0 || results[0].Value != 0 {
		t.Fatalf("unexpected empty aggregation: %+v %+v %v", results, failures, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = Compute(ctx, "job", []domain.Record{{ID: "1", Data: map[string]any{"value": 7}}}, configs)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestComputeRejectsNonFiniteAndComplexValues(t *testing.T) {
	records := []domain.Record{
		{ID: "nan", Data: map[string]any{"city": "x", "amount": math.NaN()}},
		{ID: "complex", Data: map[string]any{"city": []string{"x"}, "amount": 10}},
	}
	configs := []domain.AggregationConfig{{Name: "sum", Operation: "sum", Field: "amount", GroupBy: []string{"city"}}}
	results, failures, err := Compute(context.Background(), "job", records, configs)
	if err != nil || len(results) != 0 || len(failures) != 2 {
		t.Fatalf("unexpected results=%+v failures=%+v err=%v", results, failures, err)
	}
}
