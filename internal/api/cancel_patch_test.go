package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

func TestPatchCancellationRoute(t *testing.T) {
	repo := repository.NewJobRepository()
	id := "0123456789abcdef0123456789abcdef"
	if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	router := NewRouter(repo)
	path := PipelinesPath + "/" + id + "/cancel"
	// Completed jobs are not cancellable; 409 proves the PATCH handler is wired.
	patch := testRequest(t, router, http.MethodPatch, path, "")
	if patch.Code != http.StatusConflict {
		t.Fatalf("PATCH route: %d %s", patch.Code, patch.Body.String())
	}
	// Preserve the preexisting POST route for installed Postman collections.
	legacy := testRequest(t, router, http.MethodPost, path, "")
	if legacy.Code != http.StatusConflict {
		t.Fatalf("POST compatibility: %d %s", legacy.Code, legacy.Body.String())
	}
}

// testPublisher switches the manager to the external-queue cancellation path:
// a durable pending job can be marked CANCELLED even before a worker claims it.
type testPublisher struct{}

func (testPublisher) Publish(context.Context, string, string) error { return nil }

func TestPatchCancelPendingJob(t *testing.T) {
	repo := repository.NewJobRepository()
	id := "fedcba9876543210fedcba9876543210"
	if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StatePending}); err != nil {
		t.Fatal(err)
	}
	manager := jobs.NewManagerWithOptions(repo, context.Background(), jobs.Options{Publisher: testPublisher{}})
	defer func() { _ = manager.Shutdown(context.Background()) }()
	router := NewRouterWithOptions(repo, Options{JobManager: manager})
	response := testRequest(t, router, http.MethodPatch, PipelinesPath+"/"+id+"/cancel", "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("PATCH cancel status: %d %s", response.Code, response.Body.String())
	}
	updated, err := repo.GetStatus(id)
	if err != nil || updated.State != domain.StateCancelled {
		t.Fatalf("expected CANCELLED, status=%v err=%v", updated, err)
	}
}
