CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs ((data->'status'->>'state'));
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs ((data->'status'->>'created_at'));
