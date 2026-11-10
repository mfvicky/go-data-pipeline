package aggregation

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type bucket struct {
	group map[string]any
	sum   float64
	count int64
	min   float64
	max   float64
}

// Compute aggregates transformed, valid records. Invalid aggregate inputs are
// reported per record and do not prevent other records from being aggregated.
func Compute(ctx context.Context, jobID string, records []domain.Record, configs []domain.AggregationConfig) ([]domain.AggregationResult, []domain.PipelineError, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("context is required")
	}
	a := NewAccumulator(jobID, configs)
	for _, record := range records {
		if err := a.Add(ctx, record); err != nil {
			return nil, a.failures, err
		}
	}
	return a.Finish(ctx)
}

func groupFor(data map[string]any, fields []string) (map[string]any, string, error) {
	if len(fields) == 0 {
		return nil, "[]", nil
	}
	group := make(map[string]any, len(fields))
	values := make([]any, len(fields))
	for i, field := range fields {
		value, ok := data[field]
		if !ok || value == nil {
			return nil, "", fmt.Errorf("group_by field %q is missing", field)
		}
		switch value.(type) {
		case string, bool, int, int64, float64, json.Number:
		default:
			return nil, "", fmt.Errorf("group_by field %q must be a scalar value", field)
		}
		group[field] = value
		values[i] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, "", fmt.Errorf("encode group: %w", err)
	}
	return group, string(encoded), nil
}

func number(data map[string]any, field string) (float64, error) {
	value, ok := data[field]
	if !ok || value == nil {
		return 0, fmt.Errorf("field %q is missing", field)
	}
	var n float64
	var err error
	switch v := value.(type) {
	case float64:
		n = v
	case float32:
		n = float64(v)
	case int:
		n = float64(v)
	case int64:
		n = float64(v)
	case json.Number:
		n, err = v.Float64()
	case string:
		n, err = strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, fmt.Errorf("field %q must be numeric", field)
	}
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("field %q must contain a finite number", field)
	}
	return n, nil
}
