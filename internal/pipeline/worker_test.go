package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/validation"
)

func TestStartWorkerPools(t *testing.T) {
	input := make(chan domain.Record, 4)
	input <- domain.Record{ID: "1", Data: map[string]any{"id": "1", "city": " Delhi ", "amount": "125.50"}}
	input <- domain.Record{ID: "2", Data: map[string]any{"id": "2", "city": "Mumbai", "amount": "-5"}}
	input <- domain.Record{ID: "3", Data: map[string]any{"id": "3", "city": " Pune ", "amount": "bad"}}
	input <- domain.Record{ID: "4", Data: map[string]any{"id": "4", "city": "CHENNAI", "amount": "40"}}
	close(input)

	spec := domain.JobSpec{
		ValidationRules: map[string]any{
			"required_fields": []string{"id", "city"},
			"min_values":      map[string]any{"amount": 0},
		},
		TransformRules: map[string]any{
			"trim_strings":     true,
			"normalize":        []string{"city"},
			"type_conversions": map[string]any{"amount": "number"},
		},
		WorkerPoolSizes: map[string]int{"validation": 3, "transformation": 2},
	}

	failures := make(chan domain.PipelineError, 4)
	output, err := StartWorkerPools(context.Background(), "test-job", spec, input, failures)
	if err != nil {
		t.Fatal(err)
	}

	got := make(map[string]domain.Record)
	for record := range output {
		got[record.ID] = record
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 valid records, got %d", len(got))
	}
	if got["1"].Data["city"] != "delhi" || got["1"].Data["amount"] != 125.5 {
		t.Fatalf("record 1 was not transformed correctly: %+v", got["1"].Data)
	}
	if got["4"].Data["city"] != "chennai" || got["4"].Data["amount"] != float64(40) {
		t.Fatalf("record 4 was not transformed correctly: %+v", got["4"].Data)
	}

	close(failures)
	if len(failures) != 2 {
		t.Fatalf("expected 2 failed records, got %d", len(failures))
	}
	for failure := range failures {
		if failure.Stage != "validation" || failure.JobID != "test-job" {
			t.Fatalf("unexpected failure: %+v", failure)
		}
	}
}

func TestTransformationWorkerError(t *testing.T) {
	input := make(chan domain.Record, 2)
	input <- domain.Record{ID: "1", Data: map[string]any{"amount": "invalid"}}
	input <- domain.Record{ID: "2", Data: map[string]any{"amount": "25"}}
	close(input)

	failures := make(chan domain.PipelineError, 2)
	spec := domain.JobSpec{TransformRules: map[string]any{
		"type_conversions": map[string]any{"amount": "number"},
	}}
	output, err := StartWorkerPools(context.Background(), "job", spec, input, failures)
	if err != nil {
		t.Fatal(err)
	}
	var records []domain.Record
	for record := range output {
		records = append(records, record)
	}
	if len(records) != 1 || records[0].ID != "2" {
		t.Fatalf("unexpected processed records: %+v", records)
	}
	close(failures)
	if len(failures) != 1 || (<-failures).Stage != "transformation" {
		t.Fatal("expected one transformation failure")
	}
}

func TestWorkersProcessConcurrently(t *testing.T) {
	const count = 4
	input := make(chan domain.Record, count)
	for i := 0; i < count; i++ {
		input <- domain.Record{ID: fmt.Sprint(i)}
	}
	close(input)

	started := make(chan struct{}, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	failures := make(chan domain.PipelineError, count)
	output, err := startWorkers(context.Background(), "job", "test", count, input, failures, func(record domain.Record) (domain.Record, error) {
		started <- struct{}{}
		<-release
		return record, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not run concurrently")
		}
	}
	releaseOnce.Do(func() { close(release) })

	processed := 0
	for range output {
		processed++
	}
	if processed != count {
		t.Fatalf("expected %d processed records, got %d", count, processed)
	}
}

func TestWorkersStopOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	input := make(chan domain.Record)
	failures := make(chan domain.PipelineError)

	validator, err := validation.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	output, err := StartValidationWorkers(ctx, "job", 2, validator, input, failures)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-output:
		if ok {
			t.Fatal("expected output channel to close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("workers did not stop after cancellation")
	}
}

func TestWorkerConfiguration(t *testing.T) {
	input := make(chan domain.Record)
	failures := make(chan domain.PipelineError)
	tests := []struct {
		name string
		spec domain.JobSpec
	}{
		{"zero workers", domain.JobSpec{WorkerPoolSizes: map[string]int{"validation": 0}}},
		{"too many workers", domain.JobSpec{WorkerPoolSizes: map[string]int{"transformation": 65}}},
		{"invalid validation rules", domain.JobSpec{ValidationRules: map[string]any{"field_types": map[string]any{"id": "unsupported"}}}},
		{"invalid transform rules", domain.JobSpec{TransformRules: map[string]any{"trim_strings": "yes"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := StartWorkerPools(context.Background(), "job", tt.spec, input, failures)
			if err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}

	_, err := StartWorkerPools(context.Background(), "job", domain.JobSpec{}, nil, failures)
	if err == nil || !strings.Contains(err.Error(), "input") {
		t.Fatalf("expected missing input channel error, got %v", err)
	}
	_, err = StartWorkerPools(context.Background(), "job", domain.JobSpec{}, input, nil)
	if err == nil || !strings.Contains(err.Error(), "error channel") {
		t.Fatalf("expected missing error channel error, got %v", err)
	}
}
