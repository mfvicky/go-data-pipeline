package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/jobs"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

type unavailablePublisher struct{}

func (unavailablePublisher) Publish(context.Context, string, string) error {
	return errors.New("simulated broker outage")
}

// A broker failure is ambiguous: the DB row MUST remain PENDING for replay.
func TestCreateRetainsJobForDurableBrokerRetry(t *testing.T) {
	repo := repository.NewJobRepository()
	manager := jobs.NewManagerWithOptions(repo, context.Background(), jobs.Options{Publisher: unavailablePublisher{}})
	defer func() { _ = manager.Shutdown(context.Background()) }()
	svc := JobService{Store: repo, Manager: manager}
	spec := domain.JobSpec{Sources: []domain.SourceConfig{{Type: "csv", Endpoint: "testdata/input/customers.csv"}}}
	id, err := svc.Create(context.Background(), spec, "req-test")
	if !errors.Is(err, ErrDispatchPending) {
		t.Fatalf("expected retryable dispatch error, got id %q err %v", id, err)
	}
	if id == "" {
		t.Fatal("must return persisted ID to caller")
	}
	status, err := svc.Progress(id)
	if err != nil || status.State != domain.StatePending {
		t.Fatalf("pending job lost: %+v, %v", status, err)
	}
}

func TestServiceResultsBeforeAndAfterCompletion(t *testing.T) {
	repo := repository.NewJobRepository()
	manager := jobs.NewManager(repo, context.Background())
	defer func() { _ = manager.Shutdown(context.Background()) }()
	svc := JobService{Store: repo, Manager: manager}
	id := "service-results"
	err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StatePending, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Result(id); !errors.Is(err, repository.ErrResultNotReady) {
		t.Fatalf("expected result not ready: %v", err)
	}
	if err := repo.SaveResult(id, domain.JobOutput{JobID: id}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(id, func(s *domain.JobStatus) error { s.State = domain.StateCompleted; return nil }); err != nil {
		t.Fatal(err)
	}
	if got, err := svc.Result(id); err != nil || got.JobID != id {
		t.Fatalf("result %+v err %v", got, err)
	}
}
