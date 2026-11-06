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

// AggregationConfig is consumed by the aggregation stage.
// Operations are count, sum, or avg; GroupBy optionally partitions the result.
type AggregationConfig struct {
	Name      string   `json:"name"`
	Operation string   `json:"operation"`
	Field     string   `json:"field,omitempty"`
	GroupBy   []string `json:"group_by,omitempty"`
}

// ExportConfig describes a file output.
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

// AggregationResult is the output of one aggregation for one group.
// Count is the number of records contributing to Value.
type AggregationResult struct {
	Name      string         `json:"name"`
	Operation string         `json:"operation"`
	Field     string         `json:"field,omitempty"`
	Group     map[string]any `json:"group,omitempty"`
	Value     float64        `json:"value"`
	Count     int64          `json:"count"`
}

// JobStatus is a snapshot of a job's execution state.
// StartTime stays nil until a runner actually starts the job.
type JobStatus struct {
	JobID            string                  `json:"job_id"`
	State            JobState                `json:"state"`
	RecordsProcessed int64                   `json:"records_processed"`
	RecordsRead      int64                   `json:"records_read"`
	RecordsFailed    int64                   `json:"records_failed"`
	RatePerSecond    float64                 `json:"rate_per_second"`
	StageLatencies   map[string]StageLatency `json:"stage_latencies"`
	RecordsPending   int64                   `json:"records_pending"`
	ErrorCount       int64                   `json:"error_count"`
	PercentComplete  float64                 `json:"percent_complete"`
	CreatedAt        time.Time               `json:"created_at"`
	StartTime        *time.Time              `json:"start_time,omitempty"`
	EndTime          *time.Time              `json:"end_time,omitempty"`
}

// StageLatency summarizes processing duration for a stage.
type StageLatency struct {
	Count               int64   `json:"count"`
	TotalMilliseconds   float64 `json:"total_milliseconds"`
	AverageMilliseconds float64 `json:"average_milliseconds"`
}

// ProgressSnapshot contains concurrent pipeline progress metrics.
type ProgressSnapshot struct {
	RecordsRead      int64                   `json:"records_read"`
	RecordsProcessed int64                   `json:"records_processed"`
	RecordsFailed    int64                   `json:"records_failed"`
	ErrorCount       int64                   `json:"error_count"`
	RecordsPending   int64                   `json:"records_pending"`
	PercentComplete  float64                 `json:"percent_complete"`
	RatePerSecond    float64                 `json:"rate_per_second"`
	StageLatencies   map[string]StageLatency `json:"stage_latencies"`
}

// JobOutput is stored after a job finishes (successfully or otherwise).
type JobOutput struct {
	JobID         string              `json:"job_id"`
	Records       []Record            `json:"records"`
	Aggregations  []AggregationResult `json:"aggregations"`
	Errors        []PipelineError     `json:"errors"`
	FailedRecords []FailedRecord      `json:"failed_records"`
}

// FailedRecord keeps the input record and why it was rejected.
type FailedRecord struct {
	Record Record        `json:"record"`
	Error  PipelineError `json:"error"`
}

// JobDetails joins a saved specification and the corresponding status.
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
	Record    *Record   `json:"record,omitempty"`
}
