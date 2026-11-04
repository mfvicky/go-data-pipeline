package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/transformation"
	"github.com/mfvicky/go-data-pipeline/internal/validation"
)

const maxtWorkers = 64

func StartValidationWorkers(
	ctx context.Context,
	jobID string,
	workerCount int,
	validator *validation.Validator,
	input <-chan domain.Record,
	errors chan<- domain.PipelineError,
) (<-chan domain.Record, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator is required")
	}
	return startWorkers(ctx, jobID, "validation", workerCount, input, errors, func(record domain.Record) (domain.Record, error) {
		return record, validator.Check(record)
	})
}

func StartTransformationWorkers(
	ctx context.Context,
	jobID string,
	workerCount int,
	transformer *transformation.Transformer,
	input <-chan domain.Record,
	errors chan<- domain.PipelineError,
) (<-chan domain.Record, error) {
	if transformer == nil {
		return nil, fmt.Errorf("transformer is required")
	}
	return startWorkers(ctx, jobID, "transformation", workerCount, input, errors, transformer.Apply)
}

func StartWorkerPools(
	ctx context.Context,
	jobID string,
	spec domain.JobSpec,
	input <-chan domain.Record,
	errors chan<- domain.PipelineError,
) (<-chan domain.Record, error) {
	validationCount := workerCount(spec.WorkerPoolSizes, "validation")
	transformationCount := workerCount(spec.WorkerPoolSizes, "transformation")
	if err := checkWorkerInputs(ctx, validationCount, input, errors); err != nil {
		return nil, err
	}
	if err := checkWorkerInputs(ctx, transformationCount, input, errors); err != nil {
		return nil, err
	}
	validator, err := validation.New(spec.ValidationRules)
	if err != nil {
		return nil, fmt.Errorf("validation rules: %w", err)
	}
	transformer, err := transformation.New(spec.TransformRules)
	if err != nil {
		return nil, fmt.Errorf("transformation rules: %w", err)
	}

	validated, err := StartValidationWorkers(ctx, jobID, validationCount, validator, input, errors)
	if err != nil {
		return nil, err
	}
	return StartTransformationWorkers(ctx, jobID, transformationCount, transformer, validated, errors)
}

func workerCount(sizes map[string]int, stage string) int {
	if count, ok := sizes[stage]; ok {
		return count
	}
	return 1
}

func checkWorkerInputs(ctx context.Context, count int, input <-chan domain.Record, errors chan<- domain.PipelineError) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}

	if count < 1 || count > maxtWorkers {
		return fmt.Errorf("worker count must be between 1 and %d", maxtWorkers)
	}
	if input == nil {
		return fmt.Errorf("input chanel is required")
	}
	if errors == nil {
		return fmt.Errorf("error channel is required")
	}
	if errors == nil {
		return fmt.Errorf("error channel is required")
	}
	return nil
}

func startWorkers(
	ctx context.Context,
	jobID string,
	stage string,
	count int,
	input <-chan domain.Record,
	errors chan<- domain.PipelineError,
	process func(domain.Record) (domain.Record, error),
) (<-chan domain.Record, error) {
	if err := checkWorkerInputs(ctx, count, input, errors); err != nil {
		return nil, err
	}
	output := make(chan domain.Record)
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case record, ok := <-input:
					if !ok {
						return
					}
					processed, err := process(record)
					if ctx.Err() != nil {
						return
					}
					if err != nil {
						failure := domain.PipelineError{
							JobID:     jobID,
							Stage:     stage,
							RecordID:  record.ID,
							Message:   err.Error(),
							Timestamp: time.Now().UTC(),
						}
						select {
						case <-ctx.Done():
							return
						case errors <- failure:
						}
						continue
					}
					select {
					case <-ctx.Done():
						return
					case output <- processed:
					}

				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output, nil
}
