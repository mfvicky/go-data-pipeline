package services

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"github.com/mfvicky/go-data-pipeline/internal/security"
)

func TestDeleteRemovesOnlyOwnArtifacts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "output")
	jobID := strings.Repeat("a", 32)
	otherID := strings.Repeat("b", 32)
	jobPath := filepath.Join(root, jobID, "report.json")
	otherPath := filepath.Join(root, otherID, "report.json")
	for _, p := range []string{jobPath, otherPath} {
		if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := repository.NewJobRepository()
	status := domain.JobStatus{JobID: jobID, State: domain.StateCompleted, CreatedAt: time.Now()}
	if err := repo.Create(jobID, domain.JobSpec{Sources: []domain.SourceConfig{{Type: "csv", Endpoint: "file.csv"}}}, status); err != nil {
		t.Fatal(err)
	}
	svc := JobService{Store: repo, Policy: &security.SourcePolicy{OutputDir: root}}
	if err := svc.Delete(jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(jobPath); !os.IsNotExist(err) {
		t.Fatalf("artifact not removed: %v", err)
	}
	if _, err := os.Stat(otherPath); err != nil {
		t.Fatalf("unrelated artifact removed: %v", err)
	}
	if _, err := repo.GetJob(jobID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("job still in repository: %v", err)
	}
}

func TestDeleteRejectsSymlinkDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "output")
	foreign := t.TempDir()
	jobID := strings.Repeat("c", 32)
	if err := os.MkdirAll(root, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(root, jobID)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	file := filepath.Join(foreign, "private")
	if err := os.WriteFile(file, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewJobRepository()
	if err := repo.Create(jobID, domain.JobSpec{}, domain.JobStatus{JobID: jobID, State: domain.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	svc := JobService{Store: repo, Policy: &security.SourcePolicy{OutputDir: root}}
	if err := svc.Delete(jobID); err == nil {
		t.Fatal("expected symlink to be rejected")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("symlink target deleted: %v", err)
	}
	if _, err := repo.GetJob(jobID); err != nil {
		t.Fatalf("DB row should remain for retry: %v", err)
	}
}

func TestDeleteRunningJobRetainsArtifacts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "output")
	jobID := strings.Repeat("d", 32)
	p := filepath.Join(root, jobID, "report.json")
	if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewJobRepository()
	if err := repo.Create(jobID, domain.JobSpec{}, domain.JobStatus{JobID: jobID, State: domain.StateRunning}); err != nil {
		t.Fatal(err)
	}
	svc := JobService{Store: repo, Policy: &security.SourcePolicy{OutputDir: root}}
	if err := svc.Delete(jobID); !errors.Is(err, repository.ErrRunning) {
		t.Fatalf("want ErrRunning, got %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("running export was removed: %v", err)
	}
}
