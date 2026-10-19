package domain

import "time"

// JobState represents the current lifecycle state of a pipeline job
type JobState string

const (
	StatePending   JobState = "PENDING"
	StateRunning   JobState = "RUNNING"
	StateCompleted JobState = "COMPLETED"
	StateFailed    JobState = "FAILED"
	StateCancelled JobState = "CANCELLED"
)

// SourceConfig defines a single input source (CSV, JSON, or API)
type SourceConfig struct {
	Type     string `json:"type"`     // "csv", "json", "api"
	Endpoint string `json:"endpoint"` // URL or file path
}

// JobSpec defines the entire configuration payload sent via POST /api/v1/pipelines
type JobSpec struct {
	Sources           []SourceConfig `json:"sources"`
	ValidationRules   map[string]any `json:"validation_rules"`
	TransformRules    map[string]any `json:"transform_rules"`
	WorkerPoolSizes   map[string]int `json:"worker_pool_sizes"` // e.g., {"validation": 4, "transformation": 4}
	ExportTargets     []string       `json:"export_targets"`    // e.g., ["sqlite", "csv"]
}

// JobStatus tracks the real-time execution progress of a pipeline job
type JobStatus struct {
	JobID           string    `json:"job_id"`
	State           JobState  `json:"state"`
	RecordsProcessed int64    `json:"records_processed"`
	RecordsPending   int64    `json:"records_pending"`
	ErrorCount      int64     `json:"error_count"`
	PercentComplete float64   `json:"percent_complete"`
	StartTime       time.Time `json:"start_time"`
	EndTime         *time.Time `json:"end_time,omitempty"`
}

// Record represents a single item flowing through the pipeline stages
type Record struct {
	ID        string         `json:"id"`
	Source    string         `json:"source"`
	Data      map[string]any `json:"data"`
	Timestamp time.Time      `json:"timestamp"`
}

// PipelineError represents a structured error captured during pipeline execution
type PipelineError struct {
	JobID     string    `json:"job_id"`
	Stage     string    `json:"stage"`
	RecordID  string    `json:"record_id,omitempty"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}