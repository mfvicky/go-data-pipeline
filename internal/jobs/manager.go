package jobs

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/pipeline"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

var ErrNotCancellable = errors.New("job is not running")

// Manager executes submitted jobs using cancelable contexts. Results and
// progress are stored in the existing thread-safe in-memory repository.
type Manager struct {
	repo   *repository.JobRepository
	parent context.Context
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

func NewManager(repo *repository.JobRepository, parent context.Context) *Manager {
	if parent == nil {
		parent = context.Background()
	}
	return &Manager{repo: repo, parent: parent, active: make(map[string]context.CancelFunc)}
}

// Start registers cancellation before launching the background job.
func (m *Manager) Start(id string, spec domain.JobSpec) {
	ctx, cancel := context.WithCancel(m.parent)
	m.mu.Lock()
	m.active[id] = cancel
	m.mu.Unlock()
	go m.run(ctx, id, spec)
}

func (m *Manager) run(ctx context.Context, id string, spec domain.JobSpec) {
	defer func() {
		m.mu.Lock()
		if cancel, found := m.active[id]; found {
			cancel()
			delete(m.active, id)
		}
		m.mu.Unlock()
	}()

	started := time.Now().UTC()
	_ = m.repo.UpdateStatus(id, func(status *domain.JobStatus) error {
		status.StartTime = &started
		status.State = domain.StateRunning
		return nil
	})

	result, err := pipeline.RunWithObserver(ctx, id, spec, pipeline.Observer{
		OnProgress: func(progress domain.ProgressSnapshot) {
			_ = m.repo.UpdateStatus(id, func(status *domain.JobStatus) error {
				status.RecordsRead = progress.RecordsRead
				status.RecordsProcessed = progress.RecordsProcessed
				status.RecordsFailed = progress.RecordsFailed
				status.RecordsPending = progress.RecordsPending
				status.ErrorCount = progress.ErrorCount
				status.PercentComplete = progress.PercentComplete
				status.RatePerSecond = progress.RatePerSecond
				status.StageLatencies = progress.StageLatencies
				return nil
			})
		},
		OnError: func(failure domain.PipelineError) {
			_ = m.repo.AppendError(id, failure)
		},
	})

	state := domain.StateCompleted
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		state = domain.StateCancelled
	} else if err != nil {
		state = domain.StateFailed
	}
	if err != nil && state == domain.StateFailed {
		failure := domain.PipelineError{JobID: id, Stage: "pipeline", Message: err.Error(), Timestamp: time.Now().UTC()}
		result.Errors = append(result.Errors, failure)
		_ = m.repo.AppendError(id, failure)
	}
	output := domain.JobOutput{JobID: id, Records: result.Records,
		Aggregations: result.Aggregations, Errors: result.Errors, FailedRecords: result.FailedRecords}
	_ = m.repo.SaveResult(id, output)

	ended := time.Now().UTC()
	_ = m.repo.UpdateStatus(id, func(status *domain.JobStatus) error {
		status.State = state
		status.EndTime = &ended
		status.ErrorCount = int64(len(result.Errors))
		status.RecordsPending = result.Progress.RecordsPending
		status.RecordsRead = result.Progress.RecordsRead
		status.RecordsProcessed = result.Progress.RecordsProcessed
		status.RecordsFailed = result.Progress.RecordsFailed
		status.RatePerSecond = result.Progress.RatePerSecond
		status.StageLatencies = result.Progress.StageLatencies
		if state == domain.StateCompleted {
			status.PercentComplete = 100
		} else {
			status.PercentComplete = result.Progress.PercentComplete
		}
		return nil
	})
}

func (m *Manager) Cancel(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	status, err := m.repo.GetStatus(id)
	if err != nil {
		return err
	}
	if status.State != domain.StatePending && status.State != domain.StateRunning {
		return ErrNotCancellable
	}
	cancel, exists := m.active[id]
	if !exists {
		return ErrNotCancellable
	}
	cancel()
	return nil
}
