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
	"github.com/mfvicky/go-data-pipeline/internal/report"
	"github.com/mfvicky/go-data-pipeline/internal/transformation"
)

type RunResult struct {
	Records       []domain.Record
	Aggregations  []domain.AggregationResult
	DailyReport   []domain.DailyReportRow
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

	// Fixed source-worker pool: the ingestion setting now controls simultaneous sources.
	sourceCount := workerCount(spec.WorkerPoolSizes, StageIngestion)
	if sourceCount > len(spec.Sources) {
		sourceCount = len(spec.Sources)
	}
	sourceQueue := make(chan domain.SourceConfig)
	var sourceWG sync.WaitGroup
	for i := 0; i < sourceCount; i++ {
		sourceWG.Add(1)
		go func() {
			defer sourceWG.Done()
			for source := range sourceQueue {
				started := time.Now()
				rejectRecord := func(record domain.Record, problem error) {
					failure := domain.PipelineError{JobID: jobID, Stage: StageIngestion, RecordID: record.ID, Message: problem.Error(), Timestamp: time.Now().UTC(), Record: &record}
					select {
					case <-ctx.Done():
					case channels.Errors <- failure:
						monitor.recordRejected()
						monitor.reportError(failure)
					}
				}
				var sourceTransformer *transformation.Transformer
				var setupErr error
				if len(source.TransformRules) > 0 {
					sourceTransformer, setupErr = transformation.New(source.TransformRules)
				}
				if setupErr != nil {
					failure := domain.PipelineError{JobID: jobID, Stage: StageIngestion, Message: fmt.Sprintf("source %q: transformation rules: %v", source.Name, setupErr), Timestamp: time.Now().UTC()}
					select {
					case <-ctx.Done():
					case channels.Errors <- failure:
						monitor.reportError(failure)
					}
					monitor.stageComplete(StageIngestion, time.Since(started), 1)
					continue
				}
				_, readErr := ingestion.StreamWithErrors(ctx, source, func(record domain.Record) error {
					if sourceTransformer != nil {
						converted, err := sourceTransformer.Apply(record)
						if err != nil {
							rejectRecord(record, err)
							return nil
						}
						record = converted
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case channels.Records <- record:
						monitor.recordRead()
						return nil
					}
				}, rejectRecord)
				if readErr != nil && ctx.Err() == nil {
					failure := domain.PipelineError{JobID: jobID, Stage: StageIngestion, Message: fmt.Sprintf("source %q: %v", source.Name, readErr), Timestamp: time.Now().UTC()}
					select {
					case <-ctx.Done():
					case channels.Errors <- failure:
						monitor.reportError(failure)
					}
					monitor.stageComplete(StageIngestion, time.Since(started), 1)
				} else {
					monitor.stageComplete(StageIngestion, time.Since(started), 0)
				}
			}
		}()
	}
	go func() {
		defer close(sourceQueue)
		for _, source := range spec.Sources {
			select {
			case <-ctx.Done():
				return
			case sourceQueue <- source:
			}
		}
	}()
	go func() { sourceWG.Wait(); close(channels.Records) }()

	// Fan-in aggregation consumes a bounded record channel concurrently with
	// ingestion/validation/transformation. Only grouped state lives here; the
	// job output still retains records for the public results endpoint.
	type aggOutcome struct {
		values   []domain.AggregationResult
		daily    []domain.DailyReportRow
		failures []domain.PipelineError
		err      error
	}
	aggregatedInput := make(chan domain.Record, DefaultBufferSize)
	aggDone := make(chan aggOutcome, 1)
	started := time.Now()
	go func() {
		acc := aggregation.NewAccumulator(jobID, spec.Aggregations)
		var dailyBuilder *report.Builder
		if spec.Report != nil {
			dailyBuilder = report.NewBuilder()
		}
		var firstErr error
		for record := range aggregatedInput {
			if firstErr != nil {
				continue
			} // always drain so the producer can exit
			if err := acc.Add(ctx, record); err != nil {
				firstErr = err
				continue
			}
			if dailyBuilder != nil {
				if err := dailyBuilder.Add(record); err != nil {
					firstErr = err
				}
			}
		}
		if firstErr != nil {
			aggDone <- aggOutcome{err: firstErr}
			return
		}
		values, failures, err := acc.Finish(ctx)
		var daily []domain.DailyReportRow
		if err == nil && dailyBuilder != nil {
			daily, err = dailyBuilder.Finish(ctx)
		}
		aggDone <- aggOutcome{values: values, failures: failures, daily: daily, err: err}
	}()

	for record := range output {
		result.Records = append(result.Records, record)
		select {
		case <-ctx.Done(): // continue draining upstream on cancellation
		case aggregatedInput <- record:
		}
	}
	close(aggregatedInput)
	sourceWG.Wait()
	monitor.finishIngestion()
	close(channels.Errors)
	result.Errors = <-errorsDone
	// The channel has one buffered outcome; after upstream shutdown this
	// goroutine always exits even if the pipeline has been cancelled.
	outcome := <-aggDone
	if err := ctx.Err(); err != nil {
		return result, err
	}

	aggregated, failures, err := outcome.values, outcome.failures, outcome.err
	result.Aggregations = aggregated
	result.DailyReport = outcome.daily
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
	if len(spec.Aggregations) > 0 || spec.Report != nil {
		monitor.stageComplete("aggregation", time.Since(started), failureCount)
	}
	if err != nil {
		return result, err
	}

	exportReport := export.Report{JobID: jobID, Records: result.Records,
		Aggregations: result.Aggregations, DailyReport: result.DailyReport, Errors: result.Errors,
		FailedRecords: collectFailedRecords(result.Errors)}
	// A dedicated exporter goroutine handles all targets sequentially to avoid
	// overwriting a shared file while preserving context cancellation.
	type exportOutcome struct {
		index   int
		err     error
		elapsed time.Duration
	}
	exportDone := make(chan exportOutcome, 1)
	go func() {
		for i, target := range spec.Exports {
			started := time.Now()
			if err := export.Write(ctx, target, exportReport); err != nil {
				exportDone <- exportOutcome{index: i, err: err, elapsed: time.Since(started)}
				return
			}
			monitor.stageComplete("export", time.Since(started), 0)
		}
		exportDone <- exportOutcome{}
	}()
	var written exportOutcome
	select {
	case written = <-exportDone:
	case <-ctx.Done():
		return result, ctx.Err()
	}
	if written.err != nil {
		if !errors.Is(written.err, context.Canceled) && !errors.Is(written.err, context.DeadlineExceeded) {
			failure := domain.PipelineError{JobID: jobID, Stage: "export", Message: written.err.Error(), Timestamp: time.Now().UTC()}
			result.Errors = append(result.Errors, failure)
			monitor.reportError(failure)
			monitor.stageComplete("export", written.elapsed, 1)
		}
		return result, fmt.Errorf("export %q: %w", spec.Exports[written.index].Type, written.err)
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
