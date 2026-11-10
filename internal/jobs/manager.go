package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/monitor"
	"github.com/mfvicky/go-data-pipeline/internal/pipeline"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
)

var (
	ErrNotCancellable = errors.New("job is not running")
	ErrQueueFull      = errors.New("job queue is full")
	ErrShuttingDown   = errors.New("job manager is shutting down")
)

type request struct {
	id        string
	spec      domain.JobSpec
	ctx       context.Context
	requestID string
}

// Publisher enqueues job identifiers in an external durable message broker.
// It must confirm publication before returning successfully.
type Publisher interface {
	Publish(context.Context, string, string) error
}

type Options struct {
	Publisher     Publisher
	MaxConcurrent int
	QueueCapacity int
	SourcePolicy  *security.SourcePolicy
	Metrics       *monitor.Registry
	Logger        *slog.Logger
}

// Manager limits concurrent jobs and queues bounded pending work.
type Manager struct {
	repo      repository.Store
	publisher Publisher
	parent    context.Context
	policy    *security.SourcePolicy
	metrics   *monitor.Registry
	logger    *slog.Logger
	mu        sync.Mutex
	active    map[string]context.CancelFunc
	queue     chan request
	closed    bool
	workers   sync.WaitGroup
}

func NewManager(repo repository.Store, parent context.Context) *Manager {
	return NewManagerWithOptions(repo, parent, Options{})
}
func NewManagerWithOptions(repo repository.Store, parent context.Context, opts Options) *Manager {
	if parent == nil {
		parent = context.Background()
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 5
	}
	if opts.QueueCapacity <= 0 {
		opts.QueueCapacity = 100
	}
	m := &Manager{repo: repo, publisher: opts.Publisher, parent: parent, policy: opts.SourcePolicy, metrics: opts.Metrics, logger: opts.Logger, active: make(map[string]context.CancelFunc), queue: make(chan request, opts.QueueCapacity)}
	for i := 0; opts.Publisher == nil && i < opts.MaxConcurrent; i++ {
		m.workers.Add(1)
		go func() {
			defer m.workers.Done()
			for req := range m.queue {
				if err := m.run(req.ctx, req.id, req.spec, req.requestID); err != nil && m.logger != nil {
					m.logger.Error("job worker failed", "job_id", req.id, "error", err)
				}
			}
		}()
	}
	return m
}

func (m *Manager) Start(id string, spec domain.JobSpec) error {
	return m.StartWithRequestID(id, spec, "")
}

func (m *Manager) StartWithRequestID(id string, spec domain.JobSpec, requestID string) error {
	m.mu.Lock()
	if m.closed || m.parent.Err() != nil {
		m.mu.Unlock()
		return ErrShuttingDown
	}
	publisher := m.publisher
	if publisher != nil {
		// No lock is held across a network publish: cancellation and shutdown
		// remain responsive while waiting for the broker's durable confirmation.
		m.mu.Unlock()
		ctx, cancel := context.WithTimeout(m.parent, 10*time.Second)
		defer cancel()
		return publisher.Publish(ctx, id, requestID)
	}
	ctx, cancel := context.WithCancel(m.parent)
	m.active[id] = cancel
	select {
	case m.queue <- request{id: id, spec: spec, ctx: ctx, requestID: requestID}:
		m.mu.Unlock()
		return nil
	default:
		delete(m.active, id)
		cancel()
		m.mu.Unlock()
		return ErrQueueFull
	}
}

// UsesExternalQueue distinguishes a durable dispatch failure from an
// in-memory capacity rejection; a durable job must not be deleted on an
// ambiguous publisher confirm timeout.
func (m *Manager) UsesExternalQueue() bool { return m.publisher != nil }

func (m *Manager) run(ctx context.Context, id string, spec domain.JobSpec, requestID string) error {
	defer func() {
		m.mu.Lock()
		if cancel, ok := m.active[id]; ok {
			cancel()
			delete(m.active, id)
		}
		m.mu.Unlock()
	}()
	started := time.Now().UTC()
	if m.logger != nil {
		m.logger.Info("pipeline job started", "job_id", id, "request_id", requestID)
	}
	if ctx.Err() != nil {
		if m.publisher != nil && m.parent.Err() != nil {
			return ErrShuttingDown
		}
		return m.finishCancelled(id)
	}
	if m.policy != nil {
		ctx = security.WithPolicy(ctx, m.policy)
	}
	if err := m.repo.UpdateStatus(id, func(status *domain.JobStatus) error {
		status.StartTime = &started
		status.State = domain.StateRunning
		return nil
	}); err != nil {
		return err
	}
	m.metrics.JobStarted()
	result, err := pipeline.RunWithObserver(ctx, id, spec, pipeline.Observer{
		OnProgress: func(progress domain.ProgressSnapshot) {
			persistErr := m.repo.UpdateStatus(id, func(s *domain.JobStatus) error {
				s.RecordsRead = progress.RecordsRead
				s.RecordsProcessed = progress.RecordsProcessed
				s.RecordsFailed = progress.RecordsFailed
				s.RecordsPending = progress.RecordsPending
				s.ErrorCount = progress.ErrorCount
				s.PercentComplete = progress.PercentComplete
				s.RatePerSecond = progress.RatePerSecond
				s.StageLatencies = progress.StageLatencies
				return nil
			})
			if persistErr != nil && m.logger != nil {
				m.logger.Error("persist progress failed", "job_id", id, "error", persistErr)
			}
		},
		OnError: func(failure domain.PipelineError) {
			if persistErr := m.repo.AppendError(id, failure); persistErr != nil && m.logger != nil {
				m.logger.Error("persist pipeline error failed", "job_id", id, "error", persistErr)
			}
		},
	})
	// On process shutdown, leave the broker delivery unacknowledged so it can
	// be retried after restart. User-triggered cancellation still becomes CANCELLED.
	if m.publisher != nil && m.parent.Err() != nil {
		return ErrShuttingDown
	}
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
	output := domain.JobOutput{JobID: id, Records: result.Records, Aggregations: result.Aggregations, DailyReport: result.DailyReport, Errors: result.Errors, FailedRecords: result.FailedRecords}
	if persistErr := m.repo.SaveResult(id, output); persistErr != nil {
		if m.logger != nil {
			m.logger.Error("persist result failed", "job_id", id, "request_id", requestID, "error", persistErr)
		}
		return persistErr
	}
	ended := time.Now().UTC()
	finalErr := m.repo.UpdateStatus(id, func(s *domain.JobStatus) error {
		s.State = state
		s.EndTime = &ended
		s.ErrorCount = int64(len(result.Errors))
		s.RecordsPending = result.Progress.RecordsPending
		s.RecordsRead = result.Progress.RecordsRead
		s.RecordsProcessed = result.Progress.RecordsProcessed
		s.RecordsFailed = result.Progress.RecordsFailed
		s.RatePerSecond = result.Progress.RatePerSecond
		s.StageLatencies = result.Progress.StageLatencies
		if state == domain.StateCompleted {
			s.PercentComplete = 100
		} else {
			s.PercentComplete = result.Progress.PercentComplete
		}
		return nil
	})
	if finalErr != nil && m.logger != nil {
		m.logger.Error("persist final state failed", "job_id", id, "error", finalErr)
	}
	if finalErr != nil {
		return finalErr
	}
	m.metrics.JobFinished(state, result.Progress.RecordsProcessed, ended.Sub(started))
	if m.logger != nil {
		m.logger.Info("pipeline job finished", "job_id", id, "request_id", requestID, "state", state, "duration", ended.Sub(started).String(), "records", result.Progress.RecordsProcessed, "errors", len(result.Errors))
		for stage, latency := range result.Progress.StageLatencies {
			m.logger.Info("pipeline stage completed", "job_id", id, "request_id", requestID, "stage", stage, "items", latency.Count, "duration_ms", latency.TotalMilliseconds)
		}
	}
	return nil
}
func (m *Manager) finishCancelled(id string) error {
	now := time.Now().UTC()
	if err := m.repo.SaveResult(id, domain.JobOutput{JobID: id}); err != nil {
		return err
	}
	return m.repo.UpdateStatus(id, func(s *domain.JobStatus) error { s.State = domain.StateCancelled; s.EndTime = &now; return nil })
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
	cancel, ok := m.active[id]
	if !ok && m.publisher != nil && status.State == domain.StatePending {
		now := time.Now().UTC()
		if err := m.repo.SaveResult(id, domain.JobOutput{JobID: id}); err != nil {
			return err
		}
		return m.repo.UpdateStatus(id, func(s *domain.JobStatus) error { s.State = domain.StateCancelled; s.EndTime = &now; return nil })
	}
	if !ok {
		return ErrNotCancellable
	}
	cancel()
	return nil
}

// Shutdown cancels accepted work, closes the queue, and waits for worker exit.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		for _, cancel := range m.active {
			cancel()
		}
		close(m.queue)
	}
	m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.workers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("job manager shutdown: %w", ctx.Err())
	}
}

// ProcessExternal executes one RabbitMQ delivery. Terminal jobs are skipped,
// making duplicate deliveries harmless. The caller ACKs only after nil error.
func (m *Manager) ProcessExternal(_ context.Context, id, requestID string) error {
	m.mu.Lock()
	if m.closed || m.parent.Err() != nil {
		m.mu.Unlock()
		return ErrShuttingDown
	}
	if _, busy := m.active[id]; busy {
		m.mu.Unlock()
		return fmt.Errorf("job %s already executing", id)
	}
	status, err := m.repo.GetStatus(id)
	if errors.Is(err, repository.ErrNotFound) {
		m.mu.Unlock()
		return nil
	} // deleted after failed publish
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if status.State != domain.StatePending && status.State != domain.StateRunning {
		m.mu.Unlock()
		return nil
	}
	spec, err := m.repo.GetSpec(id)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	ctx, cancel := context.WithCancel(m.parent)
	m.active[id] = cancel
	m.workers.Add(1)
	m.mu.Unlock()
	defer m.workers.Done()
	err = m.run(ctx, id, *spec, requestID)
	if err != nil {
		// Safe to retry after a transient database persistence error.
		_ = m.repo.UpdateStatus(id, func(s *domain.JobStatus) error {
			if s.State == domain.StateRunning {
				s.State = domain.StatePending
				s.StartTime = nil
			}
			return nil
		})
	}
	return err
}
