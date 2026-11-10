package jobs

import (
	"context"
	"errors"
	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBoundedQueueAndShutdown(t *testing.T) {
	repo := repository.NewJobRepository()
	manager := NewManagerWithOptions(repo, context.Background(), Options{MaxConcurrent: 1, QueueCapacity: 1})
	file := filepath.Join(t.TempDir(), "one.json")
	if err := os.WriteFile(file, []byte(`[ {"id":"1"} ]`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StatePending, CreatedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	// One source is enough to run a completed job and exercise worker cleanup.
	spec := domain.JobSpec{Sources: []domain.SourceConfig{{Type: "json", Endpoint: file}}}
	if err := manager.Start("first", spec); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start("second", spec); err != nil && !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start("later", spec); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}
