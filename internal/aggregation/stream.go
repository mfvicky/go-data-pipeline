package aggregation

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// Accumulator processes records as they arrive on the fan-in channel.
// Memory is O(configured aggregations * unique groups), not O(input rows).
// The job repository still retains individual processed records for /results.
type Accumulator struct {
	jobID    string
	configs  []domain.AggregationConfig
	buckets  []map[string]*bucket
	failures []domain.PipelineError
}

func NewAccumulator(jobID string, configs []domain.AggregationConfig) *Accumulator {
	a := &Accumulator{jobID: jobID, configs: configs, buckets: make([]map[string]*bucket, len(configs))}
	for i, config := range configs {
		a.buckets[i] = map[string]*bucket{}
		if len(config.GroupBy) == 0 {
			a.buckets[i]["[]"] = &bucket{}
		}
	}
	return a
}

func (a *Accumulator) Add(ctx context.Context, record domain.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, config := range a.configs {
		if config.SourceKind != "" && record.Data["kind"] != config.SourceKind {
			continue
		}
		groups := a.buckets[i]
		group, key, err := groupFor(record.Data, config.GroupBy)
		if err == nil && config.Operation != "count" {
			var n float64
			n, err = number(record.Data, config.Field)
			if err == nil {
				current := 0.0
				if existing := groups[key]; existing != nil {
					current = existing.sum
				}
				if (config.Operation == "sum" || config.Operation == "avg") && (math.IsInf(current+n, 0) || math.IsNaN(current+n)) {
					err = fmt.Errorf("field %q: sum exceeds numeric range", config.Field)
				} else {
					b := groups[key]
					if b == nil {
						b = &bucket{group: group}
						groups[key] = b
					}
					if b.count == 0 || n < b.min {
						b.min = n
					}
					if b.count == 0 || n > b.max {
						b.max = n
					}
					if config.Operation == "sum" || config.Operation == "avg" {
						b.sum += n
					}
					b.count++
				}
			}
		} else if err == nil {
			b := groups[key]
			if b == nil {
				b = &bucket{group: group}
				groups[key] = b
			}
			b.count++
		}
		if err != nil {
			a.failures = append(a.failures, domain.PipelineError{
				JobID: a.jobID, Stage: "aggregation", RecordID: record.ID,
				Message: fmt.Sprintf("aggregation %q: %v", config.Name, err), Timestamp: time.Now().UTC(),
			})
		}
	}
	return nil
}

func (a *Accumulator) Finish(ctx context.Context) ([]domain.AggregationResult, []domain.PipelineError, error) {
	if err := ctx.Err(); err != nil {
		return nil, a.failures, err
	}
	var results []domain.AggregationResult
	for i, config := range a.configs {
		groups := a.buckets[i]
		keys := make([]string, 0, len(groups))
		for k := range groups {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			b := groups[key]
			value := float64(b.count)
			switch config.Operation {
			case "sum":
				value = b.sum
			case "min":
				value = b.min
			case "max":
				value = b.max
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
	return results, a.failures, nil
}
