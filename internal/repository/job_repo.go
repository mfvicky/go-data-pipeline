package repository

import (
	"errors"
	"sync"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// JobRepository handles storage and retrieval of job specs and statuses
type JobRepository struct {
	mu   sync.RWMutex
	jobs map[string]*domain.JobStatus
	specs map[string]*domain.JobSpec
}

func NewJobRepository() *JobRepository {
	return &JobRepository{
		jobs:  make(map[string]*domain.JobStatus),
		specs: make(map[string]*domain.JobSpec),
	}
}

func (r *JobRepository) Save(jobID string, spec *domain.JobSpec, status *domain.JobStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[jobID] = status
	r.specs[jobID] = spec
}

func (r *JobRepository) GetStatus(jobID string) (*domain.JobStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, exists := r.jobs[jobID]
	if !exists {
		return nil, errors.New("job not found")
	}
	return status, nil
}

func (r *JobRepository) ListJobs() []*domain.JobStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]*domain.JobStatus, 0, len(r.jobs))
	for _, status := range r.jobs {
		list = append(list, status)
	}
	return list
}