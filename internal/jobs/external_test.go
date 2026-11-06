package jobs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
)

type fakePublisher struct {
	mu  sync.Mutex
	ids []string
}

func (p *fakePublisher) Publish(_ context.Context, id, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = append(p.ids, id)
	return nil
}

func TestExternalJobSkipsCancelledDelivery(t *testing.T) {
	repo := repository.NewJobRepository()
	p := &fakePublisher{}
	m := NewManagerWithOptions(repo, context.Background(), Options{Publisher: p})
	defer func() { _ = m.Shutdown(context.Background()) }()
	id := "cancelled-before-delivery"
	now := time.Now().UTC()
	if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StatePending, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := m.StartWithRequestID(id, domain.JobSpec{}, "req-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(id); err != nil {
		t.Fatal(err)
	}
	if err := m.ProcessExternal(context.Background(), id, "req-1"); err != nil {
		t.Fatal(err)
	}
	status, err := repo.GetStatus(id)
	if err != nil || status.State != domain.StateCancelled {
		t.Fatalf("status %+v: %v", status, err)
	}
	if len(p.ids) != 1 {
		t.Fatalf("publisher got %v", p.ids)
	}
}
