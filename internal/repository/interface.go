package repository

import (
	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

// Store is the persistence contract consumed by the API and job manager.
// Both NewJobRepository (memory) and OpenPostgres (durable PostgreSQL) satisfy it.
type Store interface {
	Create(string, domain.JobSpec, domain.JobStatus) error
	GetStatus(string) (*domain.JobStatus, error)
	GetSpec(string) (*domain.JobSpec, error)
	GetJob(string) (*domain.JobDetails, error)
	ListJobs() []*domain.JobStatus
	UpdateStatus(string, func(*domain.JobStatus) error) error
	UpdateSpec(string, domain.JobSpec) error
	DeleteJob(string) error
	AppendError(string, domain.PipelineError) error
	GetErrors(string) ([]domain.PipelineError, error)
	SaveResult(string, domain.JobOutput) error
	GetResult(string) (*domain.JobOutput, error)
}

var _ Store = (*JobRepository)(nil)
