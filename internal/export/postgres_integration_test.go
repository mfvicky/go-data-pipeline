package export

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mfvicky/go-data-pipeline/internal/domain"
	"github.com/mfvicky/go-data-pipeline/internal/repository"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Run against a real PostgreSQL database with TEST_POSTGRES_DSN; never
// requires Docker for the default unit test suite.
func TestPostgresReportExportIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN for real PostgreSQL integration")
	}
	repo, err := repository.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.Close() }()
	id := fmt.Sprintf("report-integration-%d", time.Now().UnixNano())
	spec := domain.JobSpec{Sources: []domain.SourceConfig{{Type: "csv", Endpoint: "placeholder.csv"}}}
	if err := repo.Create(id, spec, domain.JobStatus{JobID: id, State: domain.StatePending, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = repo.DeleteJob(id) }()
	t.Setenv("DATABASE_URL", dsn)
	price := 50.0
	cases := 10.0
	report := Report{JobID: id, DailyReport: []domain.DailyReportRow{{Date: "2026-11-03", Cases: &cases, CryptoPrices: map[string]float64{"bitcoin": price}, Sources: []string{"covid", "coin"}}}}
	if err := Write(context.Background(), domain.ExportConfig{Type: "postgres"}, report); err != nil {
		t.Fatal(err)
	}
	price = 55
	report.DailyReport[0].CryptoPrices["bitcoin"] = price
	if err := Write(context.Background(), domain.ExportConfig{Type: "postgres"}, report); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sqlDB.Close() }()
	var count int
	if err := sqlDB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM pipeline_daily_reports WHERE job_id=$1", id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one upserted report row, got %d", count)
	}
}
