package repository

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func TestJobRepositoryLifecycleAndCopies(t *testing.T) {
	r := NewJobRepository()
	start := time.Now().UTC()
	spec := domain.JobSpec{
		Sources:         []domain.SourceConfig{{Name: "A", Type: "api", Endpoint: "https://example.org", Headers: map[string]string{"X-Key": "secret"}}},
		ValidationRules: map[string]any{"allowed": []any{map[string]any{"key": "original"}}},
		Aggregations:    []domain.AggregationConfig{{Name: "count", Operation: "count", GroupBy: []string{"city"}}},
		WorkerPoolSizes: map[string]int{"validation": 3},
		ExportTargets:   []string{"json"},
	}
	status := domain.JobStatus{JobID: "id", State: domain.StatePending, StartTime: &start}
	if err := r.Create("id", spec, status); err != nil {
		t.Fatal(err)
	}
	if err := r.Create("id", spec, status); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	// Mutating input or returned values must never alter in-memory state.
	spec.Sources[0].Name = "changed"
	spec.Sources[0].Headers["X-Key"] = "changed"
	spec.WorkerPoolSizes["validation"] = 99
	spec.ValidationRules["allowed"].([]any)[0].(map[string]any)["key"] = "changed"
	spec.Aggregations[0].GroupBy[0] = "changed"
	start = start.Add(1 * time.Hour)
	got, err := r.GetJob("id")
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Sources[0].Name != "A" || got.Spec.Sources[0].Headers["X-Key"] != "secret" || got.Spec.WorkerPoolSizes["validation"] != 3 || got.Spec.ValidationRules["allowed"].([]any)[0].(map[string]any)["key"] != "original" || got.Spec.Aggregations[0].GroupBy[0] != "city" {
		t.Fatalf("repository leaked caller mutation: %+v", got.Spec)
	}
	got.Spec.Sources[0].Headers["X-Key"] = "new"
	got.Status.StartTime = &start
	statusCopy, err := r.GetStatus("id")
	if err != nil || statusCopy.State != domain.StatePending {
		t.Fatalf("GetStatus: %+v, %v", statusCopy, err)
	}
	statusCopy.State = domain.StateCancelled
	if now, _ := r.GetStatus("id"); now.State != domain.StatePending {
		t.Fatalf("status leaked mutation: %+v", now)
	}
	if err := r.UpdateStatus("id", func(s *domain.JobStatus) error { s.State = domain.StateRunning; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateStatus("id", func(s *domain.JobStatus) error { s.State = domain.StateFailed; return errors.New("abort") }); err == nil {
		t.Fatal("expected mutator error")
	}
	if now, _ := r.GetStatus("id"); now.State != domain.StateRunning {
		t.Fatalf("failed mutation should not commit: %+v", now)
	}
	list := r.ListJobs()
	if len(list) != 1 {
		t.Fatalf("got %d jobs, want 1", len(list))
	}
	list[0].State = domain.StateFailed
	if err := r.DeleteJob("id"); !errors.Is(err, ErrRunning) {
		t.Fatalf("expected running delete to be rejected, got %v", err)
	}
	if err := r.UpdateStatus("id", func(s *domain.JobStatus) error { s.State = domain.StateCompleted; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteJob("id"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetJob("id"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted job not found, got %v", err)
	}
	if _, err := r.GetSpec("id"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected deleted spec not found, got %v", err)
	}
	if err := r.DeleteJob("id"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing delete, got %v", err)
	}
}

func TestRepositoryConcurrentCreationAndListing(t *testing.T) {
	r := NewJobRepository()
	var wg sync.WaitGroup
	for n := 0; n < 50; n++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("id-%03d", i)
			if err := r.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id}); err != nil {
				t.Errorf("create: %v", err)
			}
			_ = r.ListJobs()
			_, _ = r.GetStatus(id)
		}(n)
	}
	wg.Wait()
	list := r.ListJobs()
	if len(list) != 50 || list[0].JobID != "id-000" || list[49].JobID != "id-049" {
		t.Fatalf("expected 50 sorted entries, got %+v", list)
	}
}
