package export

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// writePostgres persists the date-joined report into a dedicated PostgreSQL
// table. DATABASE_URL is taken from server configuration, not user input.
// Re-running a job replaces only its own rows in a single transaction.
func writePostgres(ctx context.Context, report Report) error {
	if report.JobID == "" {
		return fmt.Errorf("job id required for postgres export")
	}
	if len(report.DailyReport) == 0 {
		return fmt.Errorf("postgres report export requires nonempty global daily report")
	}
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" && strings.TrimSpace(os.Getenv("PGHOST")) == "" {
		return fmt.Errorf("DATABASE_URL or PGHOST required for postgres export")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("connect report database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer func() { _ = sqlDB.Close() }()
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DELETE FROM pipeline_daily_reports WHERE job_id = ?", report.JobID).Error; err != nil {
			return err
		}
		for _, row := range report.DailyReport {
			raw, err := json.Marshal(row)
			if err != nil {
				return err
			}
			// Table created by numbered migration 003 when the API initializes the ORM.
			if err := tx.Exec("INSERT INTO pipeline_daily_reports (job_id, report_date, data) VALUES (?, ?, CAST(? AS JSONB))", report.JobID, row.Date, string(raw)).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
