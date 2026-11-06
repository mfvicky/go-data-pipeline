package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestWorkerPoolsConcurrentRecordsAndErrors(t *testing.T) {
	const total = 240
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	input := make(chan domain.Record)
	errorsCh := make(chan domain.PipelineError)

	spec := domain.JobSpec{
		ValidationRules: map[string]any{"required_fields": []string{"id"}},
		TransformRules: map[string]any{
			"normalize":        []string{"city"},
			"type_conversions": map[string]any{"amount": "number"},
		},
		WorkerPoolSizes: map[string]int{"validation": 8, "transformation": 6},
	}

	output, err := StartWorkerPools(ctx, "bulk-job", spec, input, errorsCh)
	if err != nil {
		t.Fatal(err)
	}

	failuresDone := make(chan []domain.PipelineError, 1)
	go func() {
		var failures []domain.PipelineError
		for failure := range errorsCh {
			failures = append(failures, failure)
		}
		failuresDone <- failures
	}()

	go func() {
		defer close(input)
		for i := 0; i < total; i++ {
			id := fmt.Sprintf("id-%03d", i)
			if i%7 == 0 {
				id = ""
			}
			amount := "12.50"
			if i%11 == 0 {
				amount = "invalid"
			}
			record := domain.Record{
				ID: fmt.Sprintf("record-%03d", i),
				Data: map[string]any{
					"id": id, "city": " DELHI ", "amount": amount,
				},
			}
			select {
			case <-ctx.Done():
				return
			case input <- record:
			}
		}
	}()

	seen := make(map[string]bool, total)
	for record := range output {
		if seen[record.ID] {
			t.Errorf("record %q processed more than once", record.ID)
		}
		seen[record.ID] = true
		if record.Data["city"] != "delhi" || record.Data["amount"] != float64(12.5) {
			t.Errorf("record %q not transformed: %v", record.ID, record.Data)
		}
	}

	// The caller owns errorsCh. All workers are finished when output closes.
	close(errorsCh)
	failures := <-failuresDone
	if err := ctx.Err(); err != nil {
		t.Fatalf("worker pool stalled: %v", err)
	}

	var wantValid, wantValidationErrors, wantTransformationErrors int
	for i := 0; i < total; i++ {
		switch {
		case i%7 == 0:
			wantValidationErrors++
		case i%11 == 0:
			wantTransformationErrors++
		default:
			wantValid++
		}
	}
	if len(seen) != wantValid {
		t.Errorf("processed %d distinct records, want %d", len(seen), wantValid)
	}
	if len(failures) != wantValidationErrors+wantTransformationErrors {
		t.Fatalf("collected %d errors, want %d", len(failures), wantValidationErrors+wantTransformationErrors)
	}

	stages := make(map[string]int)
	failedRecords := make(map[string]bool)
	for _, failure := range failures {
		if failure.JobID != "bulk-job" || failure.RecordID == "" {
			t.Errorf("error missing job or record ID: %+v", failure)
		}
		if failedRecords[failure.RecordID] {
			t.Errorf("duplicate failure for record %q", failure.RecordID)
		}
		failedRecords[failure.RecordID] = true
		stages[failure.Stage]++
	}
	if stages[StageValidation] != wantValidationErrors || stages[StageTransformation] != wantTransformationErrors {
		t.Errorf("error stages = %v, want validation=%d transformation=%d", stages, wantValidationErrors, wantTransformationErrors)
	}
}

func TestWorkersCloseOutputWithEmptyInput(t *testing.T) {
	input := make(chan domain.Record)
	close(input)

	output, err := StartWorkerPools(context.Background(), "empty-job", domain.JobSpec{
		WorkerPoolSizes: map[string]int{"validation": 4, "transformation": 4},
	}, input, make(chan domain.PipelineError))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-output:
		if open {
			t.Fatal("expected the output channel to close without records")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("output channel remained open after all workers exited")
	}
}

func TestWorkersCancelWithUndrainedErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan domain.Record, 1)
	input <- domain.Record{ID: "bad"}
	processed := make(chan struct{})
	output, err := startWorkers(ctx, "cancel-job", StageValidation, 1, input,
		make(chan domain.PipelineError), func(record domain.Record) (domain.Record, error) {
			close(processed)
			return domain.Record{}, errors.New("invalid record")
		})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not process the invalid record")
	}

	// Nobody receives errors, and the input remains open. Cancellation must
	// still stop the worker and close its output channel.
	cancel()
	select {
	case _, open := <-output:
		if open {
			t.Fatal("invalid record unexpectedly reached output")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop when error collection was blocked")
	}
}

func TestWorkersCancelWithBlockedOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan domain.Record, 1)
	input <- domain.Record{ID: "one"}
	processed := make(chan struct{})
	output, err := startWorkers(ctx, "cancel-job", StageTransformation, 1, input,
		make(chan domain.PipelineError), func(record domain.Record) (domain.Record, error) {
			close(processed)
			return record, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not process its record")
	}

	// No consumer has received output yet, and the input remains open.
	cancel()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, open := <-output:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("output channel remained open after cancellation")
		}
	}
}

func TestConcurrentRunsKeepJobResultsIsolated(t *testing.T) {
	const (
		jobs = 8
		rows = 60
	)
	csvData := "id,city,amount\n"
	var wantValid, wantErrors int
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("customer-%02d", i)
		if i%10 == 0 {
			id = ""
		}
		amount := "25"
		if i%13 == 0 {
			amount = "invalid"
		}
		csvData += fmt.Sprintf("%s, Delhi ,%s\n", id, amount)
		if id == "" || amount == "invalid" {
			wantErrors++
		} else {
			wantValid++
		}
	}
	filename := filepath.Join(t.TempDir(), "customers.csv")
	if err := os.WriteFile(filename, []byte(csvData), 0600); err != nil {
		t.Fatal(err)
	}

	spec := domain.JobSpec{
		Sources:         []domain.SourceConfig{{Name: "customers", Type: "csv", Endpoint: filename}},
		ValidationRules: map[string]any{"required_fields": []string{"id"}},
		TransformRules: map[string]any{
			"normalize":        []string{"city"},
			"type_conversions": map[string]any{"amount": "number"},
		},
		WorkerPoolSizes: map[string]int{"validation": 4, "transformation": 3},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan error, jobs)
	for i := 0; i < jobs; i++ {
		go func(n int) {
			jobID := fmt.Sprintf("job-%02d", n)
			result, err := Run(ctx, jobID, spec)
			if err != nil {
				results <- fmt.Errorf("%s: %w", jobID, err)
				return
			}
			if len(result.Records) != wantValid || len(result.Errors) != wantErrors {
				results <- fmt.Errorf("%s: got %d records and %d errors, want %d and %d", jobID,
					len(result.Records), len(result.Errors), wantValid, wantErrors)
				return
			}
			seen := make(map[string]bool)
			for _, record := range result.Records {
				if seen[record.ID] {
					results <- fmt.Errorf("%s: duplicate record %q", jobID, record.ID)
					return
				}
				seen[record.ID] = true
			}
			for _, failure := range result.Errors {
				if failure.JobID != jobID {
					results <- fmt.Errorf("%s: received an error belonging to %s", jobID, failure.JobID)
					return
				}
			}
			results <- nil
		}(i)
	}

	for i := 0; i < jobs; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatalf("concurrent pipelines did not finish: %v", ctx.Err())
		}
	}
}
