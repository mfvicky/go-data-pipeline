package repository

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

var (
	ErrNotFound       = errors.New("job not found")
	ErrExists         = errors.New("job already exists")
	ErrRunning        = errors.New("cannot delete pending or running job")
	ErrResultNotReady = errors.New("job results are not ready")
)

// JobRepository stores pending job specifications and execution status in memory.
// Nothing survives a server restart. All returned objects are defensive copies.
type JobRepository struct {
	mu          sync.RWMutex
	jobs        map[string]*domain.JobStatus
	specs       map[string]*domain.JobSpec
	results     map[string]*domain.JobOutput
	errors      map[string][]domain.PipelineError
	store       persistentStore
	lastPersist map[string]time.Time
}

func NewJobRepository() *JobRepository {
	return &JobRepository{
		jobs:        make(map[string]*domain.JobStatus),
		specs:       make(map[string]*domain.JobSpec),
		results:     make(map[string]*domain.JobOutput),
		errors:      make(map[string][]domain.PipelineError),
		lastPersist: make(map[string]time.Time),
	}
}

// Create inserts a job atomically, rejecting a duplicate ID.
func (r *JobRepository) Create(jobID string, spec domain.JobSpec, status domain.JobStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.jobs[jobID]; exists {
		return ErrExists
	}
	r.saveLocked(jobID, &spec, &status)
	if err := r.persistLocked(jobID); err != nil {
		delete(r.jobs, jobID)
		delete(r.specs, jobID)
		return err
	}
	r.lastPersist[jobID] = time.Now()
	return nil
}

// Save preserves the original scaffold method for callers that need upsert.
func (r *JobRepository) Save(jobID string, spec *domain.JobSpec, status *domain.JobStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveLocked(jobID, spec, status)
	_ = r.persistLocked(jobID)
}

func (r *JobRepository) saveLocked(jobID string, spec *domain.JobSpec, status *domain.JobStatus) {
	copySpec := cloneSpec(*spec)
	copyStatus := cloneStatus(*status)
	r.specs[jobID] = &copySpec
	r.jobs[jobID] = &copyStatus
}

func (r *JobRepository) GetStatus(jobID string) (*domain.JobStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, exists := r.jobs[jobID]
	if !exists {
		return nil, ErrNotFound
	}
	copyStatus := cloneStatus(*status)
	return &copyStatus, nil
}

func (r *JobRepository) GetSpec(jobID string) (*domain.JobSpec, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, exists := r.specs[jobID]
	if !exists {
		return nil, ErrNotFound
	}
	copySpec := cloneSpec(*spec)
	return &copySpec, nil
}

func (r *JobRepository) GetJob(jobID string) (*domain.JobDetails, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, exists := r.jobs[jobID]
	if !exists {
		return nil, ErrNotFound
	}
	return &domain.JobDetails{JobID: jobID, Spec: cloneSpec(*r.specs[jobID]), Status: cloneStatus(*status)}, nil
}

// ListJobs returns sorted status snapshots, not pointers to repository state.
func (r *JobRepository) ListJobs() []*domain.JobStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]*domain.JobStatus, 0, len(r.jobs))
	for _, status := range r.jobs {
		copied := cloneStatus(*status)
		list = append(list, &copied)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].JobID < list[j].JobID })
	return list
}

// UpdateStatus runs a mutator under the write lock and commits only on success.
// The callback must not call another repository method.
func (r *JobRepository) UpdateStatus(jobID string, mutate func(*domain.JobStatus) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, exists := r.jobs[jobID]
	if !exists {
		return ErrNotFound
	}
	copyStatus := cloneStatus(*status)
	if err := mutate(&copyStatus); err != nil {
		return err
	}
	r.jobs[jobID] = &copyStatus
	// Coalesce high-frequency progress notifications to avoid one database write
	// per record. State transitions are always durable before returning.
	last := r.lastPersist[jobID]
	shouldPersist := r.store != nil && (copyStatus.State != status.State || copyStatus.EndTime != nil || last.IsZero() || time.Since(last) >= time.Second)
	if shouldPersist {
		if err := r.persistLocked(jobID); err != nil {
			r.jobs[jobID] = status
			return err
		}
		r.lastPersist[jobID] = time.Now()
	}
	return nil
}

func (r *JobRepository) DeleteJob(jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, exists := r.jobs[jobID]
	if !exists {
		return ErrNotFound
	}
	if status.State == domain.StateRunning || status.State == domain.StatePending {
		return ErrRunning
	}
	if r.store != nil {
		if err := r.store.remove(jobID); err != nil {
			return err
		}
	}
	delete(r.jobs, jobID)
	delete(r.specs, jobID)
	delete(r.results, jobID)
	delete(r.errors, jobID)
	delete(r.lastPersist, jobID)
	return nil
}

// AppendError stores a failure while the job is still running.
func (r *JobRepository) AppendError(jobID string, failure domain.PipelineError) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.jobs[jobID]; !exists {
		return ErrNotFound
	}
	r.errors[jobID] = append(r.errors[jobID], cloneError(failure))
	return r.persistLocked(jobID)
}

func (r *JobRepository) GetErrors(jobID string) ([]domain.PipelineError, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, exists := r.jobs[jobID]; !exists {
		return nil, ErrNotFound
	}
	failures := make([]domain.PipelineError, len(r.errors[jobID]))
	for i, failure := range r.errors[jobID] {
		failures[i] = cloneError(failure)
	}
	return failures, nil
}

// SaveResult stores a complete snapshot, even when a job failed or was cancelled.
func (r *JobRepository) SaveResult(jobID string, result domain.JobOutput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.jobs[jobID]; !exists {
		return ErrNotFound
	}
	copyResult := cloneOutput(result)
	r.results[jobID] = &copyResult
	r.errors[jobID] = make([]domain.PipelineError, len(copyResult.Errors))
	for i, failure := range copyResult.Errors {
		r.errors[jobID][i] = cloneError(failure)
	}
	return r.persistLocked(jobID)
}

func (r *JobRepository) GetResult(jobID string) (*domain.JobOutput, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, exists := r.jobs[jobID]; !exists {
		return nil, ErrNotFound
	}
	result, exists := r.results[jobID]
	if !exists {
		return nil, ErrResultNotReady
	}
	copyResult := cloneOutput(*result)
	return &copyResult, nil
}

func cloneRecord(record domain.Record) domain.Record {
	record.Data = cloneAnyMap(record.Data)
	return record
}

func cloneError(failure domain.PipelineError) domain.PipelineError {
	if failure.Record != nil {
		record := cloneRecord(*failure.Record)
		failure.Record = &record
	}
	return failure
}

func cloneOutput(result domain.JobOutput) domain.JobOutput {
	out := domain.JobOutput{JobID: result.JobID}
	out.Records = make([]domain.Record, len(result.Records))
	for i, record := range result.Records {
		out.Records[i] = cloneRecord(record)
	}
	out.DailyReport = make([]domain.DailyReportRow, len(result.DailyReport))
	for i, row := range result.DailyReport {
		out.DailyReport[i] = row
		if row.Cases != nil {
			n := *row.Cases
			out.DailyReport[i].Cases = &n
		}
		if row.Deaths != nil {
			n := *row.Deaths
			out.DailyReport[i].Deaths = &n
		}
		if row.TemperatureC != nil {
			n := *row.TemperatureC
			out.DailyReport[i].TemperatureC = &n
		}
		if row.CryptoPrices != nil {
			out.DailyReport[i].CryptoPrices = make(map[string]float64, len(row.CryptoPrices))
			for k, v := range row.CryptoPrices {
				out.DailyReport[i].CryptoPrices[k] = v
			}
		}
		out.DailyReport[i].Sources = append([]string(nil), row.Sources...)
	}
	out.Aggregations = make([]domain.AggregationResult, len(result.Aggregations))
	for i, aggregation := range result.Aggregations {
		out.Aggregations[i] = aggregation
		out.Aggregations[i].Group = cloneAnyMap(aggregation.Group)
	}
	out.Errors = make([]domain.PipelineError, len(result.Errors))
	for i, failure := range result.Errors {
		out.Errors[i] = cloneError(failure)
	}
	out.FailedRecords = make([]domain.FailedRecord, len(result.FailedRecords))
	for i, failure := range result.FailedRecords {
		out.FailedRecords[i] = domain.FailedRecord{Record: cloneRecord(failure.Record), Error: cloneError(failure.Error)}
	}
	return out
}

func cloneStatus(s domain.JobStatus) domain.JobStatus {
	if s.StageLatencies != nil {
		copyLatencies := make(map[string]domain.StageLatency, len(s.StageLatencies))
		for name, latency := range s.StageLatencies {
			copyLatencies[name] = latency
		}
		s.StageLatencies = copyLatencies
	}
	if s.StartTime != nil {
		start := *s.StartTime
		s.StartTime = &start
	}
	if s.EndTime != nil {
		end := *s.EndTime
		s.EndTime = &end
	}
	return s
}

func cloneSpec(s domain.JobSpec) domain.JobSpec {
	if s.Sources != nil {
		sources := make([]domain.SourceConfig, len(s.Sources))
		copy(sources, s.Sources)
		for i := range sources {
			sources[i].RequiredFields = append([]string(nil), sources[i].RequiredFields...)
			sources[i].TransformRules = cloneAnyMap(sources[i].TransformRules)
			sources[i].EnrichFields = cloneAnyMap(sources[i].EnrichFields)
			if sources[i].FieldMap != nil {
				fields := make(map[string]string, len(sources[i].FieldMap))
				for k, v := range sources[i].FieldMap {
					fields[k] = v
				}
				sources[i].FieldMap = fields
			}
			if sources[i].Headers != nil {
				headers := make(map[string]string, len(sources[i].Headers))
				for k, v := range sources[i].Headers {
					headers[k] = v
				}
				sources[i].Headers = headers
			}
		}
		s.Sources = sources
	}
	s.ValidationRules = cloneAnyMap(s.ValidationRules)
	s.TransformRules = cloneAnyMap(s.TransformRules)
	if s.WorkerPoolSizes != nil {
		pools := make(map[string]int, len(s.WorkerPoolSizes))
		for k, v := range s.WorkerPoolSizes {
			pools[k] = v
		}
		s.WorkerPoolSizes = pools
	}
	if s.Aggregations != nil {
		aggs := make([]domain.AggregationConfig, len(s.Aggregations))
		copy(aggs, s.Aggregations)
		for i := range aggs {
			aggs[i].GroupBy = append([]string(nil), aggs[i].GroupBy...)
		}
		s.Aggregations = aggs
	}
	s.ExportTargets = append([]string(nil), s.ExportTargets...)
	s.Exports = append([]domain.ExportConfig(nil), s.Exports...)
	if s.Report != nil {
		r := *s.Report
		s.Report = &r
	}
	return s
}

func cloneAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = cloneValue(v)
	}
	return result
}

// JSON-decoded config consists of maps and slices with scalar leaves.
func cloneValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneAnyMap(x)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = cloneValue(item)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	default:
		return x
	}
}

// UpdateSpec persists validated, job-specific export destinations before execution.
func (r *JobRepository) UpdateSpec(id string, spec domain.JobSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	old, ok := r.specs[id]
	if !ok {
		return ErrNotFound
	}
	copied := cloneSpec(spec)
	r.specs[id] = &copied
	if err := r.persistLocked(id); err != nil {
		r.specs[id] = old
		return err
	}
	return nil
}
