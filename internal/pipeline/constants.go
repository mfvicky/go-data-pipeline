package pipeline

const (
	StageIngestion      = "ingestion"
	StageValidation     = "validation"
	StageTransformation = "transformation"
	DefaultBufferSize   = 16
	maxWorkers          = 64
)
