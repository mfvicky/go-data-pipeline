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
	return startWorkers(ctx, jobID, StageValidation, workerCount, input, errors, func(record domain.Record) (domain.Record, error) {
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
	return startWorkers(ctx, jobID, StageTransformation, workerCount, input, errors, transformer.Apply)
}

func StartWorkerPools(
	ctx context.Context,
	jobID string,
	spec domain.JobSpec,
	input <-chan domain.Record,
	errors chan<- domain.PipelineError,
) (<-chan domain.Record, error) {
	return startWorkerPoolsObserved(ctx, jobID, spec, input, errors, nil)
}

func startWorkerPoolsObserved(ctx context.Context, jobID string, spec domain.JobSpec, input <-chan domain.Record, errors chan<- domain.PipelineError, monitor *metricsTracker) (<-chan domain.Record, error) {
	validationCount := workerCount(spec.WorkerPoolSizes, StageValidation)
	transformationCount := workerCount(spec.WorkerPoolSizes, StageTransformation)
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

	validated, err := startWorkersObserved(ctx, jobID, StageValidation, validationCount, input, errors, monitor, func(record domain.Record) (domain.Record, error) {
		return record, validator.Check(record)
	})
	if err != nil {
		return nil, err
	}
	return startWorkersObserved(ctx, jobID, StageTransformation, transformationCount, validated, errors, monitor, transformer.Apply)
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

	if count < 1 || count > maxWorkers {
		return fmt.Errorf("worker count must be between 1 and %d", maxWorkers)
	}
	if input == nil {
		return fmt.Errorf("input channel is required")
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
	return startWorkersObserved(ctx, jobID, stage, count, input, errors, nil, process)
}

func startWorkersObserved(
	ctx context.Context,
	jobID string,
	stage string,
	count int,
	input <-chan domain.Record,
	errors chan<- domain.PipelineError,
	monitor *metricsTracker,
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
					start := time.Now()
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
							Record:    &record,
							Timestamp: time.Now().UTC(),
						}
						select {
						case <-ctx.Done():
							return
						case errors <- failure:
							if monitor != nil {
								monitor.reportError(failure)
								monitor.stageComplete(stage, time.Since(start), 1)
							}
						}
						continue
					}
					select {
					case <-ctx.Done():
						return
					case output <- processed:
						if monitor != nil {
							monitor.stageComplete(stage, time.Since(start), 0)
						}
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
