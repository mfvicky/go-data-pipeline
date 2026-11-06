package aggregation

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

type bucket struct {
	group map[string]any
	sum   float64
	count int64
}

// Compute aggregates transformed, valid records. Invalid aggregate inputs are
// reported per record and do not prevent other records from being aggregated.
func Compute(ctx context.Context, jobID string, records []domain.Record, configs []domain.AggregationConfig) ([]domain.AggregationResult, []domain.PipelineError, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("context is required")
	}
	var results []domain.AggregationResult
	var failures []domain.PipelineError

	for _, config := range configs {
		buckets := make(map[string]*bucket)
		if len(config.GroupBy) == 0 {
			buckets["[]"] = &bucket{}
		}

		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return results, failures, err
			}
			group, key, err := groupFor(record.Data, config.GroupBy)
			if err == nil && config.Operation != "count" {
				var n float64
				n, err = number(record.Data, config.Field)
				if err == nil {
					current := 0.0
					if prior := buckets[key]; prior != nil {
						current = prior.sum
					}
					if math.IsInf(current+n, 0) || math.IsNaN(current+n) {
						err = fmt.Errorf("field %q: sum exceeds numeric range", config.Field)
					} else {
						b := buckets[key]
						if b == nil {
							b = &bucket{group: group}
							buckets[key] = b
						}
						b.sum += n
						b.count++
					}
				}
			} else if err == nil {
				b := buckets[key]
				if b == nil {
					b = &bucket{group: group}
					buckets[key] = b
				}
				b.count++
			}
			if err != nil {
				failures = append(failures, domain.PipelineError{
					JobID: jobID, Stage: "aggregation", RecordID: record.ID,
					Message:   fmt.Sprintf("aggregation %q: %v", config.Name, err),
					Timestamp: time.Now().UTC(),
				})
			}
		}

		keys := make([]string, 0, len(buckets))
		for key := range buckets {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			b := buckets[key]
			value := float64(b.count)
			switch config.Operation {
			case "sum":
				value = b.sum
			case "avg":
				value = 0
				if b.count > 0 {
					value = b.sum / float64(b.count)
				}
			}
			results = append(results, domain.AggregationResult{
				Name: config.Name, Operation: config.Operation, Field: config.Field,
				Group: b.group, Value: value, Count: b.count,
			})
		}
	}
	if err := ctx.Err(); err != nil {
		return results, failures, err
	}
	return results, failures, nil
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
