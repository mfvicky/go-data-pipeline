CREATE TABLE IF NOT EXISTS pipeline_daily_reports (job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE, report_date DATE NOT NULL, data JSONB NOT NULL, PRIMARY KEY (job_id, report_date));
CREATE INDEX IF NOT EXISTS idx_daily_reports_report_date ON pipeline_daily_reports (report_date);
