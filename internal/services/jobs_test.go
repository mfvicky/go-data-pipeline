package services

import (
	"context"
	"errors"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

func TestInvalidSpecRejectedBeforeJobCreation(t *testing.T) {
	repo := repository.NewJobRepository()
	manager := jobs.NewManager(repo, context.Background())
	defer func() { _ = manager.Shutdown(context.Background()) }()
	svc := JobService{Store: repo, Manager: manager}
	_, err := svc.Create(context.Background(), domain.JobSpec{}, "req-1")
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected invalid spec, got %v", err)
	}
	if n := len(repo.ListJobs()); n != 0 {
		t.Fatalf("unexpected jobs: %d", n)
	}
}
