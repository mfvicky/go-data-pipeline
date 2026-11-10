package aggregation

import (
	"context"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"math"
	"testing"
)

func TestMinMaxAggregationsDoNotOverflow(t *testing.T) {
	records := []domain.Record{{ID: "r1", Data: map[string]any{"amount": math.MaxFloat64}}, {ID: "r2", Data: map[string]any{"amount": math.MaxFloat64}}}
	configs := []domain.AggregationConfig{{Name: "minimum", Operation: "min", Field: "amount"}, {Name: "maximum", Operation: "max", Field: "amount"}}
	result, failures, err := Compute(context.Background(), "job", records, configs)
	if err != nil || len(failures) > 0 || len(result) != 2 {
		t.Fatalf("compute: %+v failures=%+v err=%v", result, failures, err)
	}
	for _, r := range result {
		if r.Value != math.MaxFloat64 || r.Count != 2 {
			t.Fatalf("incorrect extreme: %+v", r)
		}
	}
}
