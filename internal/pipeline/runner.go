package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/aggregation"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/export"
	"github.com/mfvicky/go-data-pipeline/internal/ingestion"
)

type RunResult struct {
	Records       []domain.Record
	Aggregations  []domain.AggregationResult
	Errors        []domain.PipelineError
	FailedRecords []domain.FailedRecord
	Progress      domain.ProgressSnapshot
}

func Run(ctx context.Context, jobID string, spec domain.JobSpec) (RunResult, error) {
	return RunWithObserver(ctx, jobID, spec, Observer{})
}

// RunWithObserver runs the pipeline and publishes progress/errors as stages finish.
func RunWithObserver(ctx context.Context, jobID string, spec domain.JobSpec, observer Observer) (result RunResult, runErr error) {
	if ctx == nil {
		return result, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := domain.ValidateJobSpec(spec); err != nil {
		return result, err
	}

	monitor := newMetricsTracker(observer)
	defer func() {
		result.Progress = monitor.current()
		if runErr == nil {
			result.Progress.PercentComplete = 100
		}
		result.FailedRecords = collectFailedRecords(result.Errors)
	}()

	var cancel context.CancelFunc
	if spec.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(spec.TimeoutSeconds)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	channels := NewChannels(DefaultBufferSize)
	output, err := startWorkerPoolsObserved(ctx, jobID, spec, channels.Records, channels.Errors, monitor)
	if err != nil {
		return result, err
	}

	errorsDone := make(chan []domain.PipelineError, 1)
	go func() {
		var collected []domain.PipelineError
		for failure := range channels.Errors {
			collected = append(collected, failure)
		}
		errorsDone <- collected
	}()

	var sourceWG sync.WaitGroup
	for _, source := range spec.Sources {
		sourceWG.Add(1)
		go func(source domain.SourceConfig) {
			defer sourceWG.Done()
			started := time.Now()
			records, readErr := ingestion.Read(ctx, source)
			if readErr == nil {
				for _, record := range records {
					select {
					case <-ctx.Done():
						readErr = ctx.Err()
					case channels.Records <- record:
						monitor.recordRead()
					}
					if readErr != nil {
						break
					}
				}
			}
			if readErr != nil && ctx.Err() == nil {
				failure := domain.PipelineError{JobID: jobID, Stage: StageIngestion,
					Message: fmt.Sprintf("source %q: %v", source.Name, readErr), Timestamp: time.Now().UTC()}
				select {
				case <-ctx.Done():
				case channels.Errors <- failure:
					monitor.reportError(failure)
				}
				monitor.stageComplete(StageIngestion, time.Since(started), 1)
			} else {
				monitor.stageComplete(StageIngestion, time.Since(started), 0)
			}
		}(source)
	}
	go func() {
		sourceWG.Wait()
		close(channels.Records)
	}()

	for record := range output {
		result.Records = append(result.Records, record)
	}
	sourceWG.Wait()
	monitor.finishIngestion()
	close(channels.Errors)
	result.Errors = <-errorsDone

	if err := ctx.Err(); err != nil {
		return result, err
	}

	started := time.Now()
	aggregated, failures, err := aggregation.Compute(ctx, jobID, result.Records, spec.Aggregations)
	result.Aggregations = aggregated
	recordByID := make(map[string]domain.Record, len(result.Records))
	for _, record := range result.Records {
		recordByID[record.ID] = record
	}
	for i := range failures {
		if record, ok := recordByID[failures[i].RecordID]; ok {
			copied := record
			failures[i].Record = &copied
		}
	}
	result.Errors = append(result.Errors, failures...)
	for _, failure := range failures {
		monitor.reportError(failure)
	}
	failureCount := int64(len(failures))
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		failureCount++
		failure := domain.PipelineError{JobID: jobID, Stage: "aggregation", Message: err.Error(), Timestamp: time.Now().UTC()}
		result.Errors = append(result.Errors, failure)
		monitor.reportError(failure)
	}
	if len(spec.Aggregations) > 0 {
		monitor.stageComplete("aggregation", time.Since(started), failureCount)
	}
	if err != nil {
		return result, err
	}

	report := export.Report{JobID: jobID, Records: result.Records,
		Aggregations: result.Aggregations, Errors: result.Errors,
		FailedRecords: collectFailedRecords(result.Errors)}
	for _, target := range spec.Exports {
		started := time.Now()
		if err := export.Write(ctx, target, report); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				failure := domain.PipelineError{JobID: jobID, Stage: "export", Message: err.Error(), Timestamp: time.Now().UTC()}
				result.Errors = append(result.Errors, failure)
				monitor.reportError(failure)
				monitor.stageComplete("export", time.Since(started), 1)
			}
			return result, fmt.Errorf("export %q: %w", target.Path, err)
		}
		monitor.stageComplete("export", time.Since(started), 0)
	}
	return result, nil
}

func collectFailedRecords(failures []domain.PipelineError) []domain.FailedRecord {
	records := make([]domain.FailedRecord, 0)
	for _, failure := range failures {
		if failure.Record != nil {
			records = append(records, domain.FailedRecord{Record: *failure.Record, Error: failure})
		}
	}
	return records
}
