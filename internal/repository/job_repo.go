package repository

import (
	"errors"
	"sort"
	"sync"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

var (
	ErrNotFound = errors.New("job not found")
	ErrExists   = errors.New("job already exists")
	ErrRunning  = errors.New("cannot delete running job")
)

// JobRepository stores pending job specifications and execution status in memory.
// Nothing survives a server restart. All returned objects are defensive copies.
type JobRepository struct {
	mu    sync.RWMutex
	jobs  map[string]*domain.JobStatus
	specs map[string]*domain.JobSpec
}

func NewJobRepository() *JobRepository {
	return &JobRepository{
		jobs:  make(map[string]*domain.JobStatus),
		specs: make(map[string]*domain.JobSpec),
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
	return nil
}

// Save preserves the original scaffold method for callers that need upsert.
func (r *JobRepository) Save(jobID string, spec *domain.JobSpec, status *domain.JobStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveLocked(jobID, spec, status)
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
	return nil
}

func (r *JobRepository) DeleteJob(jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, exists := r.jobs[jobID]
	if !exists {
		return ErrNotFound
	}
	if status.State == domain.StateRunning {
		return ErrRunning
	}
	delete(r.jobs, jobID)
	delete(r.specs, jobID)
	return nil
}

func cloneStatus(s domain.JobStatus) domain.JobStatus {
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
