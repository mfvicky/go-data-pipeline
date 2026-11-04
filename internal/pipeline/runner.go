package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/ingestion"
)

type RunResult struct {
	Records []domain.Record
	Errors  []domain.PipelineError
}

func Run(ctx context.Context, jobID string, spec domain.JobSpec) (RunResult, error) {
	var result RunResult
	if ctx == nil {
		return result, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := domain.ValidateJobSpec(spec); err != nil {
		return result, err
	}

	var cancel context.CancelFunc
	if spec.TimeoutSeconds > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(spec.TimeoutSeconds)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	channels := NewChannels(16)
	output, err := StartWorkerPools(ctx, jobID, spec, channels.Records, channels.Errors)
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

			_, readErr := ingestion.SendToChannel(ctx, source, channels.Records)
			if readErr != nil && ctx.Err() == nil {
				failure := domain.PipelineError{
					JobID:     jobID,
					Stage:     "ingestion",
					Message:   fmt.Sprintf("source %q: %v", source.Name, readErr),
					Timestamp: time.Now().UTC(),
				}
				select {
				case <-ctx.Done():
				case channels.Errors <- failure:
				}
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
	close(channels.Errors)
	result.Errors = <-errorsDone

	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}
