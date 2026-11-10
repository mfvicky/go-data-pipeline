package repository

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
)

func testPostgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run tests against a real PostgreSQL instance")
	}
	return dsn
}

func TestPostgresPersistsJobAndResultAcrossRestart(t *testing.T) {
	dsn := testPostgresDSN(t)
	id := fmt.Sprintf("postgres-persist-%d", time.Now().UnixNano())
	repo, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupRepo, err := OpenPostgres(dsn)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = cleanupRepo.Close() }()
		if err := cleanupRepo.DeleteJob(id); err != nil && err != ErrNotFound {
			t.Error(err)
		}
	})
	spec := domain.JobSpec{Sources: []domain.SourceConfig{{Type: "csv", Endpoint: "testdata/input/customers.csv"}}}
	status := domain.JobStatus{JobID: id, State: domain.StatePending, CreatedAt: time.Now().UTC()}
	if err := repo.Create(id, spec, status); err != nil {
		t.Fatal(err)
	}
	result := domain.JobOutput{JobID: id, Records: []domain.Record{{ID: "r1", Data: map[string]any{"amount": 7.5}}}}
	if err := repo.SaveResult(id, result); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(id, func(s *domain.JobStatus) error {
		now := time.Now().UTC()
		s.State = domain.StateCompleted
		s.EndTime = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.GetStatus(id)
	if err != nil || loaded.State != domain.StateCompleted {
		t.Fatalf("status: %+v %v", loaded, err)
	}
	saved, err := reopened.GetResult(id)
	if err != nil || len(saved.Records) != 1 {
		t.Fatalf("result: %+v %v", saved, err)
	}
	if err := reopened.DeleteJob(id); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err = OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.GetStatus(id); err != ErrNotFound {
		t.Fatalf("delete did not persist: %v", err)
	}
}

func TestPostgresRestartMarksRunningJobFailed(t *testing.T) {
	dsn := testPostgresDSN(t)
	id := fmt.Sprintf("postgres-recover-%d", time.Now().UnixNano())
	repo, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupRepo, err := OpenPostgres(dsn)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = cleanupRepo.Close() }()
		if err := cleanupRepo.DeleteJob(id); err != nil && err != ErrNotFound {
			t.Error(err)
		}
	})
	if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StateRunning, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	status, err := reopened.GetStatus(id)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != domain.StateFailed || status.EndTime == nil || status.ErrorCount == 0 {
		t.Fatalf("recovered status: %+v", status)
	}
	result, err := reopened.GetResult(id)
	if err != nil || len(result.Errors) == 0 {
		t.Fatalf("recovered errors: %+v %v", result, err)
	}
}

func TestPostgresParameterizedJobIDs(t *testing.T) {
	dsn := testPostgresDSN(t)
	id := fmt.Sprintf("literal' OR '1'='1-%d", time.Now().UnixNano())
	repo, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.Close() }()
	if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StateCompleted, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetStatus(id); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteJob(id); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresMigrationIncludesIndexes(t *testing.T) {
	dsn := testPostgresDSN(t)
	repo, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.Close() }()
	pg, ok := repo.store.(*postgresStore)
	if !ok {
		t.Fatal("expected GORM PostgreSQL store")
	}
	var count int64
	if err := pg.db.Table("schema_migrations").Where("version = ?", 2).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("database migration version 2 missing")
	}
}

func TestPostgresRecoverPendingWhenRabbitMode(t *testing.T) {
	dsn := testPostgresDSN(t)
	id := fmt.Sprintf("postgres-requeue-%d", time.Now().UnixNano())
	repo, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(id, domain.JobSpec{}, domain.JobStatus{JobID: id, State: domain.StateRunning, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgresWithRecovery(dsn, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	status, err := reopened.GetStatus(id)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != domain.StatePending || status.StartTime != nil {
		t.Fatalf("expected replay pending, got %+v", status)
	}
	ids := reopened.PendingIDs()
	found := false
	for _, candidate := range ids {
		if candidate == id {
			found = true
		}
	}
	if !found {
		t.Fatal("recovery did not include pending job")
	}
	if err := reopened.UpdateStatus(id, func(s *domain.JobStatus) error {
		now := time.Now().UTC()
		s.State = domain.StateCancelled
		s.EndTime = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DeleteJob(id); err != nil {
		t.Fatal(err)
	}
}
