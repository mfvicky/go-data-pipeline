package domain

import "time"

// JobState represents the lifecycle of a pipeline job.
type JobState string

const (
	StatePending   JobState = "PENDING"
	StateRunning   JobState = "RUNNING"
	StateCompleted JobState = "COMPLETED"
	StateFailed    JobState = "FAILED"
	StateCancelled JobState = "CANCELLED"
)

// SourceConfig specifies a CSV file/URL, JSON file/URL, or HTTP JSON API.
// RecordPath selects a nested JSON array, e.g. "results" or "data.items".
type SourceConfig struct {
	Name       string            `json:"name,omitempty"`
	Type       string            `json:"type"`
	Endpoint   string            `json:"endpoint"`
	RecordPath string            `json:"record_path,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// AggregationConfig is consumed by the aggregation stage (scheduled for Day 3).
// Operations are count, sum, or avg; GroupBy optionally partitions the result.
type AggregationConfig struct {
	Name      string   `json:"name"`
	Operation string   `json:"operation"`
	Field     string   `json:"field,omitempty"`
	GroupBy   []string `json:"group_by,omitempty"`
}

// ExportConfig describes a file output (implemented in Day 3).
type ExportConfig struct {
	Type string `json:"type"` // csv, json
	Path string `json:"path"`
}

// JobSpec is the configuration accepted by POST /api/v1/pipelines.
// Legacy ExportTargets remains for compatibility with the original scaffold.
type JobSpec struct {
	Sources         []SourceConfig      `json:"sources"`
	ValidationRules map[string]any      `json:"validation_rules,omitempty"`
	TransformRules  map[string]any      `json:"transform_rules,omitempty"`
	Aggregations    []AggregationConfig `json:"aggregations,omitempty"`
	WorkerPoolSizes map[string]int      `json:"worker_pool_sizes,omitempty"`
	TimeoutSeconds  int                 `json:"timeout_seconds,omitempty"`
	ExportTargets   []string            `json:"export_targets,omitempty"`
	Exports         []ExportConfig      `json:"exports,omitempty"`
}

// JobStatus is a snapshot of a job's execution state.
// StartTime stays nil until a runner actually starts the job (Day 2).
type JobStatus struct {
	JobID            string     `json:"job_id"`
	State            JobState   `json:"state"`
	RecordsProcessed int64      `json:"records_processed"`
	RecordsPending   int64      `json:"records_pending"`
	ErrorCount       int64      `json:"error_count"`
	PercentComplete  float64    `json:"percent_complete"`
	CreatedAt        time.Time  `json:"created_at"`
	StartTime        *time.Time `json:"start_time,omitempty"`
	EndTime          *time.Time `json:"end_time,omitempty"`
}

// JobDetails joins a saved specification and the corresponding status.
type JobDetails struct {
	JobID  string    `json:"job_id"`
	Spec   JobSpec   `json:"spec"`
	Status JobStatus `json:"status"`
}

// Record is a typed item passed between future pipeline stages.
type Record struct {
	ID        string         `json:"id"`
	Source    string         `json:"source"`
	Data      map[string]any `json:"data"`
	Timestamp time.Time      `json:"timestamp"`
}

// PipelineError describes a record or stage error.
type PipelineError struct {
	JobID     string    `json:"job_id"`
	Stage     string    `json:"stage"`
	RecordID  string    `json:"record_id,omitempty"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}
